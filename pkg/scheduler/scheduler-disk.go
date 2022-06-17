package scheduler

import (
	diskscheduler "github.com/hwameistor/local-disk-manager/pkg/csi/scheduler"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"
)

func NewDiskVolumeScheduler(f framework.Handle) VolumeScheduler {
	return diskscheduler.NewDiskVolumeSchedulerPlugin(f.SharedInformerFactory().Storage().V1().StorageClasses().Lister())
}
