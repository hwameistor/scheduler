package scheduler

import (
	"context"
	"fmt"
	"strconv"

	localstorageapis "github.com/hwameistor/local-storage/pkg/apis"
	localstoragev1alpha1 "github.com/hwameistor/local-storage/pkg/apis/hwameistor/v1alpha1"
	lvmscheduler "github.com/hwameistor/local-storage/pkg/member/controller/scheduler"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	storagev1lister "k8s.io/client-go/listers/storage/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

type LVMVolumeScheduler struct {
	fHandle framework.FrameworkHandle

	csiDriverName    string
	topoNodeLabelKey string

	replicaScheduler lvmscheduler.Scheduler
	hwameiStorCache  cache.Cache

	scLister storagev1lister.StorageClassLister
}

func NewLVMVolumeScheduler(f framework.FrameworkHandle, scheduler lvmscheduler.Scheduler, hwameiStorCache cache.Cache) VolumeScheduler {
	sche := &LVMVolumeScheduler{
		fHandle:          f,
		topoNodeLabelKey: localstorageapis.TopologyNodeKey,
		csiDriverName:    localstoragev1alpha1.CSIDriverName,
		replicaScheduler: scheduler,
		hwameiStorCache:  hwameiStorCache,
		scLister:         f.SharedInformerFactory().Storage().V1().StorageClasses().Lister(),
	}

	return sche
}

func (s *LVMVolumeScheduler) CSIDriverName() string {
	return s.csiDriverName
}

func (s *LVMVolumeScheduler) Filter(lvs []string, pendingPVCs []*corev1.PersistentVolumeClaim, node *corev1.Node) (bool, error) {
	canSchedule, err := s.filterForExistingLocalVolumes(lvs, node)
	if err != nil {
		return false, err
	}
	if !canSchedule {
		return false, fmt.Errorf("filtered out the node %s", node.Name)
	}

	return s.filterForNewPVCs(pendingPVCs, node)
}

func (s *LVMVolumeScheduler) filterForExistingLocalVolumes(lvs []string, node *corev1.Node) (bool, error) {

	if len(lvs) == 0 {
		return true, nil
	}

	// Bound PVC already has volume created in the cluster. Just check if this node has the expected volume
	for _, lvName := range lvs {
		lv := &localstoragev1alpha1.LocalVolume{}
		if err := s.hwameiStorCache.Get(context.Background(), types.NamespacedName{Name: lvName}, lv); err != nil {
			log.WithFields(log.Fields{"localvolume": lvName}).WithError(err).Error("Failed to fetch LocalVolume")
			return false, err
		}
		if lv.Spec.Config == nil {
			log.WithFields(log.Fields{"localvolume": lvName}).Error("Not found replicas info in the LocalVolume")
			return false, fmt.Errorf("pending localvolume")
		}
		isLocalNode := false
		for _, rep := range lv.Spec.Config.Replicas {
			if rep.Hostname == node.Name {
				isLocalNode = true
				break
			}
		}
		if !isLocalNode {
			log.WithFields(log.Fields{"localvolume": lvName, "node": node.Name}).Debug("LocalVolume doesn't locate at this node")
			return false, fmt.Errorf("not right node")
		}
	}

	log.WithFields(log.Fields{"localvolumes": lvs, "node": node.Name}).Debug("Filtered in this node for all existing LVM volumes")
	return true, nil
}

func (s *LVMVolumeScheduler) filterForNewPVCs(pvcs []*corev1.PersistentVolumeClaim, node *corev1.Node) (bool, error) {

	fmt.Printf("debug filterForNewPVCs pvcs = %+v", pvcs)

	if len(pvcs) == 0 {
		return true, nil
	}

	lvGroup := []*localstoragev1alpha1.LocalVolume{}
	for i := range pvcs {
		lv, err := s.constructLocalVolumeForPVC(pvcs[i])
		fmt.Printf("debug constructLocalVolumeForPVC lv = %+v, pvcs[i] = %+v", lv, pvcs[i])
		if err != nil {
			return false, err
		}
		lvGroup = append(lvGroup, lv)
	}
	// currently, just handle only one PVC.

	nodeCandidates, err := s.replicaScheduler.GetNodeCandidates(lvGroup[0])
	fmt.Printf("debug filterForNewPVCs nodeCandidates = %+v, node.Name= %+v, lvGroup[0] = %+v", nodeCandidates, node.Name, lvGroup[0])
	if err != nil {
		return false, err
	}
	for _, n := range nodeCandidates {
		if n.Name == node.Name {
			return true, nil
		}
	}
	//nodeCandidates, err := s.replicaScheduler.GetNodeCandidates()
	// Pending PVC is still waiting for the volume to be created as soon as the node is assigned to the pod.
	// So, should check if the volume can be allocated on this node or not

	return false, fmt.Errorf("no valid node")
}

func (s *LVMVolumeScheduler) constructLocalVolumeForPVC(pvc *corev1.PersistentVolumeClaim) (*localstoragev1alpha1.LocalVolume, error) {

	sc, err := s.scLister.Get(*pvc.Spec.StorageClassName)
	if err != nil {
		return nil, err
	}
	localVolume := localstoragev1alpha1.LocalVolume{}
	poolName, err := buildStoragePoolName(
		sc.Parameters[localstoragev1alpha1.VolumeParameterPoolClassKey],
		sc.Parameters[localstoragev1alpha1.VolumeParameterPoolTypeKey])
	if err != nil {
		return nil, err
	}

	localVolume.Spec.PoolName = poolName
	storage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	localVolume.Spec.RequiredCapacityBytes = storage.Value()
	replica, _ := strconv.Atoi(sc.Parameters[localstoragev1alpha1.VolumeParameterReplicaNumberKey])
	localVolume.Spec.ReplicaNumber = int64(replica)
	return &localVolume, nil
}

func buildStoragePoolName(poolClass string, poolType string) (string, error) {

	if poolClass == localstoragev1alpha1.DiskClassNameHDD && poolType == localstoragev1alpha1.PoolTypeRegular {
		return localstoragev1alpha1.PoolNameForHDD, nil
	}
	if poolClass == localstoragev1alpha1.DiskClassNameSSD && poolType == localstoragev1alpha1.PoolTypeRegular {
		return localstoragev1alpha1.PoolNameForSSD, nil
	}
	if poolClass == localstoragev1alpha1.DiskClassNameNVMe && poolType == localstoragev1alpha1.PoolTypeRegular {
		return localstoragev1alpha1.PoolNameForNVMe, nil
	}

	return "", fmt.Errorf("invalid pool info")
}
