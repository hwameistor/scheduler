package disk

import (
	"fmt"

	"github.com/hwameistor/scheduler/pkg/scheduler/interfaces"
	v1 "k8s.io/api/core/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

type Scheduler struct {
	fHandle framework.Handle

	csiDriverName    string
	topoNodeLabelKey string

	enable bool
}

func NewScheduler(f framework.Handle) interfaces.Scheduler {

	sche := &Scheduler{
		enable:           false,
		fHandle:          f,
		topoNodeLabelKey: "topoKey",
		csiDriverName:    "disk.hwameistor.io",
	}

	go sche.run()

	return sche
}

func (s *Scheduler) Filter(boundPVCs []*v1.PersistentVolumeClaim, pendingPVCs []*v1.PersistentVolumeClaim, node *v1.Node) (bool, error) {
	canSchedule, err := s.filterForBoundPVCs(boundPVCs, node)
	if err != nil {
		return false, err
	}
	if !canSchedule {
		return false, fmt.Errorf("filtered out the node %s", node.Name)
	}

	return s.filterForPendingPVCs(pendingPVCs, node)
}

func (s *Scheduler) CSIDriverName() string {
	return s.csiDriverName
}

func (s *Scheduler) run() {
}

func (s *Scheduler) filterForBoundPVCs(pvcs []*v1.PersistentVolumeClaim, node *v1.Node) (bool, error) {
	if len(pvcs) == 0 {
		return true, nil
	}
	if !s.enable {
		return false, fmt.Errorf("scheduler is not working for LVM volume")
	}

	return true, nil
}

func (s *Scheduler) filterForPendingPVCs(pvcs []*v1.PersistentVolumeClaim, node *v1.Node) (bool, error) {
	if len(pvcs) == 0 {
		return true, nil
	}
	if !s.enable {
		return false, fmt.Errorf("scheduler is not working for LVM volume")
	}

	return true, nil
}
