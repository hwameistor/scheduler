package scheduler

import (
	"fmt"

	diskscheduler "github.com/hwameistor/local-storage/pkg/member/controller/scheduler"
	v1 "k8s.io/api/core/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

type DiskVolumeScheduler struct {
	fHandle framework.FrameworkHandle

	csiDriverName    string
	topoNodeLabelKey string

	replicaScheduler diskscheduler.Scheduler
	hwameiStorCache  cache.Cache
}

func NewDiskVolumeScheduler(f framework.FrameworkHandle, scheduler diskscheduler.Scheduler, hwameiStorCache cache.Cache) VolumeScheduler {
	sche := &DiskVolumeScheduler{
		fHandle:          f,
		topoNodeLabelKey: "topoKey",
		csiDriverName:    "disk.hwameistor.io",
		replicaScheduler: scheduler,
		hwameiStorCache:  hwameiStorCache,
	}

	return sche
}

func (s *DiskVolumeScheduler) CSIDriverName() string {
	return s.csiDriverName
}

func (s *DiskVolumeScheduler) Filter(lvs []string, pendingPVCs []*v1.PersistentVolumeClaim, node *v1.Node) (bool, error) {
	canSchedule, err := s.filterForExistingLocalVolumes(lvs, node)
	if err != nil {
		return false, err
	}
	if !canSchedule {
		return false, fmt.Errorf("filtered out the node %s", node.Name)
	}

	return s.filterForNewPVCs(pendingPVCs, node)
}

func (s *DiskVolumeScheduler) filterForExistingLocalVolumes(lvs []string, node *v1.Node) (bool, error) {
	if len(lvs) == 0 {
		return true, nil
	}

	// Bound PVC already has volume created in the cluster. Just check if this node has the expected volume

	return false, fmt.Errorf("not implemented")
}

func (s *DiskVolumeScheduler) filterForNewPVCs(pvcs []*v1.PersistentVolumeClaim, node *v1.Node) (bool, error) {
	if len(pvcs) == 0 {
		return true, nil
	}

	// New PVC is still waiting for the volume to be created as soon as the node is assigned to the pod.
	// So, should check if the volume can be allocated on this node or not

	return false, fmt.Errorf("not implemented")
}
