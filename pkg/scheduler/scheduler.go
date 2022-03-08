package scheduler

import (
	"fmt"

	"github.com/hwameistor/scheduler/pkg/scheduler/disk"
	"github.com/hwameistor/scheduler/pkg/scheduler/interfaces"
	"github.com/hwameistor/scheduler/pkg/scheduler/lvm"
	v1 "k8s.io/api/core/v1"
	corelister "k8s.io/client-go/listers/core/v1"
	storagelister "k8s.io/client-go/listers/storage/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
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

	return &Scheduler{
		lvmScheduler:  lvm.NewScheduler(f),
		diskScheduler: disk.NewScheduler(f),
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
	canSchedule, err := s.lvmScheduler.Filter(lvmBoundPVCs, lvmUnboundPVCs, node)
	if err != nil {
		return false, err
	}
	if !canSchedule {
		return false, fmt.Errorf("can't schedule the LVM volume to node %s", node.Name)
	}

	return s.diskScheduler.Filter(diskBoundPVCs, diskUnboundPVCs, node)
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
