package lvm

import (
	"fmt"

	localstorageapis "github.com/hwameistor/local-storage/pkg/apis"
	localstoragev1alpha1 "github.com/hwameistor/local-storage/pkg/apis/localstorage/v1alpha1"
	volscheduler "github.com/hwameistor/local-storage/pkg/member/controller/scheduler"
	"github.com/hwameistor/scheduler/pkg/scheduler/interfaces"

	v1 "k8s.io/api/core/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

type Scheduler struct {
	fHandle framework.Handle

	csiDriverName    string
	topoNodeLabelKey string

	volumeScheduler volscheduler.Scheduler
}

func NewScheduler(f framework.Handle, scheduler volscheduler.Scheduler) interfaces.Scheduler {

	sche := &Scheduler{
		fHandle:          f,
		topoNodeLabelKey: localstorageapis.TopologyNodeKey,
		csiDriverName:    localstoragev1alpha1.CSIDriverName,
		volumeScheduler:  scheduler,
	}

	return sche
}

func (s *Scheduler) CSIDriverName() string {
	return s.csiDriverName
}

func (s *Scheduler) Filter(lvs []string, pendingPVCs []*v1.PersistentVolumeClaim, node *v1.Node) (bool, error) {
	canSchedule, err := s.filterForExistingLocalVolumes(lvs, node)
	if err != nil {
		return false, err
	}
	if !canSchedule {
		return false, fmt.Errorf("filtered out the node %s", node.Name)
	}

	return s.filterForPendingPVCs(pendingPVCs, node)
}

func (s *Scheduler) filterForExistingLocalVolumes(lvs []string, node *v1.Node) (bool, error) {

	if len(lvs) == 0 {
		return true, nil
	}

	// Bound PVC already has volume created in the cluster. Just check if this node has the expected volume

	return true, nil
}

func (s *Scheduler) filterForPendingPVCs(pvcs []*v1.PersistentVolumeClaim, node *v1.Node) (bool, error) {

	if len(pvcs) == 0 {
		return true, nil
	}

	// Pending PVC is still waiting for the volume to be created as soon as the node is assigned to the pod.
	// So, should check if the volume can be allocated on this node or not

	return true, nil
}
