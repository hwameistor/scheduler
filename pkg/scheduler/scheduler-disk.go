package scheduler

import (
	diskscheduler "github.com/hwameistor/local-disk-manager/pkg/csi/scheduler"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

func NewDiskVolumeScheduler(f framework.FrameworkHandle) VolumeScheduler {
	return diskscheduler.NewDiskVolumeSchedulerPlugin(f.SharedInformerFactory().Storage().V1().StorageClasses().Lister())
}
