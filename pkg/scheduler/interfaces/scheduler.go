package interfaces

import (
	v1 "k8s.io/api/core/v1"
)

type Scheduler interface {
	Filter(boundPVCs []*v1.PersistentVolumeClaim, unboundPVCs []*v1.PersistentVolumeClaim, node *v1.Node) (bool, error)
	CSIDriverName() string
}
