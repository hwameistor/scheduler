package scheduler

import (
	"fmt"
	"os"
	"time"

	"github.com/hwameistor/scheduler/pkg/scheduler/disk"
	"github.com/hwameistor/scheduler/pkg/scheduler/interfaces"
	"github.com/hwameistor/scheduler/pkg/scheduler/lvm"

	localstoragev1alpha1 "github.com/hwameistor/local-storage/pkg/apis/localstorage/v1alpha1"
	"github.com/hwameistor/local-storage/pkg/member/controller/scheduler"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	corelister "k8s.io/client-go/listers/core/v1"
	storagelister "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

// Scheduler is to scheduler hwameistor volume
type Scheduler struct {
	lvmScheduler  interfaces.Scheduler
	diskScheduler interfaces.Scheduler

	pvLister  corelister.PersistentVolumeLister
	pvcLister corelister.PersistentVolumeClaimLister
	scLister  storagelister.StorageClassLister
}

// NewDataCache creates a cache instance
func NewScheduler(f framework.Handle) *Scheduler {

	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.WithError(err).Fatal("Failed to construct the cluster config")
	}

	options := manager.Options{
		MetricsBindAddress: "0", // disable metrics
	}

	mgr, err := manager.New(cfg, options)
	if err != nil {
		klog.V(1).Info(err)
		os.Exit(1)
	}

	// Setup Scheme for all resources of Local Storage
	if err := localstoragev1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		log.WithError(err).Fatal("Failed to setup scheme for all resources")
	}

	cache := mgr.GetCache()
	ctx := signals.SetupSignalHandler()
	go func() {
		cache.Start(ctx)
	}()
	replicaScheduler := scheduler.New(mgr.GetClient(), cache, 1000)
	// wait for cache synced
	for {
		if cache.WaitForCacheSync(ctx) {
			break
		}
		time.Sleep(time.Second * 1)
	}
	replicaScheduler.Init()

	return &Scheduler{
		lvmScheduler:  lvm.NewScheduler(f, replicaScheduler),
		diskScheduler: disk.NewScheduler(f, replicaScheduler),
		pvLister:      f.SharedInformerFactory().Core().V1().PersistentVolumes().Lister(),
		pvcLister:     f.SharedInformerFactory().Core().V1().PersistentVolumeClaims().Lister(),
		scLister:      f.SharedInformerFactory().Storage().V1().StorageClasses().Lister(),
	}
}

func (s *Scheduler) Filter(pod *v1.Pod, node *v1.Node) (bool, error) {
	lvmBoundPVCs, lvmUnboundPVCs, diskBoundPVCs, diskUnboundPVCs, err := s.getHwameiStorPVCs(pod)
	if err != nil {
		return false, err
	}
	// figure out the existing local volume associated to the PVC, and send it to the scheduler's filter
	existingLocalVolumes := []string{}
	for _, pvc := range lvmBoundPVCs {
		pv, err := s.pvLister.Get(pvc.Spec.VolumeName)
		if err != nil {
			log.WithFields(log.Fields{"pvc": pvc.Name, "namespace": pvc.Namespace, "pv": pvc.Spec.VolumeName}).WithError(err).Error("Failed to get a Bound PVC's PV")
			return false, err
		}
		if pv.Spec.CSI == nil || len(pv.Spec.CSI.VolumeHandle) == 0 {
			log.WithFields(log.Fields{"pvc": pvc.Name, "namespace": pvc.Namespace, "pv": pvc.Spec.VolumeName}).Error("Wrong PV status of a Bound PVC")
			return false, fmt.Errorf("wrong pv")
		}
		existingLocalVolumes = append(existingLocalVolumes, pv.Spec.CSI.VolumeHandle)
	}
	canSchedule, err := s.lvmScheduler.Filter(existingLocalVolumes, lvmUnboundPVCs, node)
	if err != nil {
		return false, err
	}
	if !canSchedule {
		return false, fmt.Errorf("can't schedule the LVM volume to node %s", node.Name)
	}

	// figure out the existing local volume associated to the PVC, and send it to the scheduler's filter
	existingLocalVolumes = []string{}
	for _, pvc := range diskBoundPVCs {
		pv, err := s.pvLister.Get(pvc.Spec.VolumeName)
		if err != nil {
			log.WithFields(log.Fields{"pvc": pvc.Name, "namespace": pvc.Namespace, "pv": pvc.Spec.VolumeName}).WithError(err).Error("Failed to get a Bound PVC's PV")
			return false, err
		}
		if pv.Spec.CSI == nil || len(pv.Spec.CSI.VolumeHandle) == 0 {
			log.WithFields(log.Fields{"pvc": pvc.Name, "namespace": pvc.Namespace, "pv": pvc.Spec.VolumeName}).Error("Wrong PV status of a Bound PVC")
			return false, fmt.Errorf("wrong pv")
		}
		existingLocalVolumes = append(existingLocalVolumes, pv.Spec.CSI.VolumeHandle)
	}

	return s.diskScheduler.Filter(existingLocalVolumes, diskUnboundPVCs, node)
}

// return: lvmBoundClaims, lvmPendingClaims, diskBoundClaims, diskPendingClaims, error
func (s *Scheduler) getHwameiStorPVCs(pod *v1.Pod) ([]*v1.PersistentVolumeClaim, []*v1.PersistentVolumeClaim, []*v1.PersistentVolumeClaim, []*v1.PersistentVolumeClaim, error) {
	lvmBoundClaims := []*v1.PersistentVolumeClaim{}
	lvmPendingClaims := []*v1.PersistentVolumeClaim{}
	diskBoundClaims := []*v1.PersistentVolumeClaim{}
	diskPendingClaims := []*v1.PersistentVolumeClaim{}

	lvmCSIDriverName := s.lvmScheduler.CSIDriverName()
	diskCSIDriverName := s.diskScheduler.CSIDriverName()

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}
		pvc, err := s.pvcLister.PersistentVolumeClaims(pod.Namespace).Get(vol.PersistentVolumeClaim.ClaimName)
		if err != nil {
			// if pvc can't be found in the cluster, the pod should not be able to be scheduled
			return lvmBoundClaims, lvmPendingClaims, diskBoundClaims, diskPendingClaims, err
		}
		if pvc.Spec.StorageClassName == nil {
			// should not be the CSI pvc, ignore
			continue
		}
		sc, err := s.scLister.Get(*pvc.Spec.StorageClassName)
		if err != nil {
			// can't found storageclass in the cluster, the pod should not be able to be scheduled
			return lvmBoundClaims, lvmPendingClaims, diskBoundClaims, diskPendingClaims, err
		}
		if sc.Provisioner == lvmCSIDriverName {
			if pvc.Status.Phase == v1.ClaimBound {
				lvmBoundClaims = append(lvmBoundClaims, pvc)
			} else if pvc.Status.Phase == v1.ClaimPending {
				lvmPendingClaims = append(lvmPendingClaims, pvc)
			} else {
				return lvmBoundClaims, lvmPendingClaims, diskBoundClaims, diskPendingClaims, fmt.Errorf("unhealthy HwameiStor LVM pvc")
			}
		}
		if sc.Provisioner == diskCSIDriverName {
			if pvc.Status.Phase == v1.ClaimBound {
				diskBoundClaims = append(diskBoundClaims, pvc)
			} else if pvc.Status.Phase == v1.ClaimPending {
				diskPendingClaims = append(diskPendingClaims, pvc)
			} else {
				return lvmBoundClaims, lvmPendingClaims, diskBoundClaims, diskPendingClaims, fmt.Errorf("unhealthy HwameiStor Disk pvc")
			}
		}
	}

	return lvmBoundClaims, lvmPendingClaims, diskBoundClaims, diskPendingClaims, nil
}
