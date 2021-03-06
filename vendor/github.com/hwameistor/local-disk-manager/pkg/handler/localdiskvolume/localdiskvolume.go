package localdiskvolume

import (
	"context"
	"fmt"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	ldm "github.com/hwameistor/local-disk-manager/pkg/apis/hwameistor/v1alpha1"
	"github.com/hwameistor/local-disk-manager/pkg/utils"
	lscsi "github.com/hwameistor/local-storage/pkg/member/csi"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// LocalDiskFinalizer for the LocalDiskVolume CR
	LocalDiskFinalizer string = "localdisk.hwameistor.io/finalizer"
)

// DiskVolumeHandler
type DiskVolumeHandler struct {
	client.Client
	record.EventRecorder
	Ldv     *ldm.LocalDiskVolume
	mounter lscsi.Mounter
}

// NewLocalDiskHandler
func NewLocalDiskVolumeHandler(cli client.Client, recorder record.EventRecorder) *DiskVolumeHandler {
	logger := log.WithField("Module", "CSIPlugin")
	return &DiskVolumeHandler{
		Client:        cli,
		EventRecorder: recorder,
		mounter:       lscsi.NewLinuxMounter(logger),
	}
}

func (v *DiskVolumeHandler) ReconcileMount() (reconcile.Result, error) {
	var err error
	var result reconcile.Result
	var devPath = v.GetDevPath()
	var mountPoints = v.GetMountPoints()

	if devPath == "" || len(mountPoints) == 0 {
		log.Infof("DevPath or MountPoints is empty, no operation here")
		return result, nil
	}

	// Mount RawBlock or FileSystem Volumes
	for _, mountPoint := range mountPoints {
		if mountPoint.Phase != ldm.MountPointToBeMounted {
			continue
		}

		switch mountPoint.VolumeCap.AccessType {
		case ldm.VolumeCapability_AccessType_Mount:
			err = v.MountFileSystem(devPath, mountPoint.TargetPath, mountPoint.FsTye, mountPoint.MountOptions...)
		case ldm.VolumeCapability_AccessType_Block:
			err = v.MountRawBlock(devPath, mountPoint.TargetPath)
		default:
			// record and skip this mountpoint
			v.RecordEvent(corev1.EventTypeWarning, "ValidAccessType", "AccessType of MountPoint %s "+
				"is invalid, ignore it", mountPoint)
		}

		if err != nil {
			log.WithError(err).Errorf("Failed to mount %s to %s", devPath, mountPoint.TargetPath)
			result.Requeue = true
			continue
		}
		v.UpdateMountPointPhase(mountPoint.TargetPath, ldm.MountPointMounted)
	}

	if !result.Requeue {
		v.SetupVolumeStatus(ldm.VolumeStateReady)
	}

	return result, v.UpdateLocalDiskVolume()
}

func (v *DiskVolumeHandler) ReconcileUnmount() (reconcile.Result, error) {
	var err error
	var result reconcile.Result
	var mountPoints = v.GetMountPoints()

	for _, mountPoint := range mountPoints {
		if mountPoint.Phase == ldm.MountPointToBeUnMount {
			if err = v.UnMount(mountPoint.TargetPath); err != nil {
				log.WithError(err).Errorf("Failed to unmount %s", mountPoint.TargetPath)
				result.Requeue = true
				continue
			}

			v.MoveMountPoint(mountPoint.TargetPath)
		}
	}
	if !result.Requeue {
		// Considering that a disk will be mounted to multiple pods,
		// if a mount point is processed, the entire LocalDiskVolume will be setup to the Ready state
		v.SetupVolumeStatus(ldm.VolumeStateReady)
	}

	return result, v.UpdateLocalDiskVolume()
}

func (v *DiskVolumeHandler) ReconcileToBeDeleted() (reconcile.Result, error) {
	var result reconcile.Result
	var mountPoints = v.GetMountPoints()

	if len(mountPoints) > 0 {
		log.Infof("Volume %s remains %d mountpoint, no operation here",
			v.Ldv.Name, len(mountPoints))
		result.Requeue = true
		return result, nil
	}

	return result, v.DeleteLocalDiskVolume()
}

func (v *DiskVolumeHandler) ReconcileDeleted() (reconcile.Result, error) {
	return reconcile.Result{}, v.Delete(context.Background(), v.Ldv)
}

func (v *DiskVolumeHandler) GetLocalDiskVolume(key client.ObjectKey) (volume *ldm.LocalDiskVolume, err error) {
	volume = &ldm.LocalDiskVolume{}
	err = v.Get(context.Background(), key, volume)
	return
}

func (v *DiskVolumeHandler) RecordEvent(eventtype, reason, messageFmt string, args ...interface{}) {
	v.EventRecorder.Eventf(v.Ldv, eventtype, reason, messageFmt, args...)
}

func (v *DiskVolumeHandler) UpdateLocalDiskVolume() error {
	return v.Update(context.Background(), v.Ldv)
}

func (v *DiskVolumeHandler) DeleteLocalDiskVolume() error {
	v.SetupVolumeStatus(ldm.VolumeStateDeleted)
	return v.UpdateLocalDiskVolume()
}

func (v *DiskVolumeHandler) RemoveFinalizers() error {
	// range all finalizers and notify all owners
	v.Ldv.Finalizers = nil
	return nil
}

func (v *DiskVolumeHandler) AddFinalizers(finalizer []string) {
	v.Ldv.Finalizers = finalizer
	return
}

func (v *DiskVolumeHandler) GetMountPoints() []ldm.MountPoint {
	return v.Ldv.Status.MountPoints
}

func (v *DiskVolumeHandler) GetDevPath() string {
	return v.Ldv.Status.DevPath
}

func (v *DiskVolumeHandler) MountRawBlock(devPath, mountPoint string) error {
	// fixme: should check mount points again when do mount
	// mount twice will cause error
	if err := v.mounter.MountRawBlock(devPath, mountPoint); err != nil {
		v.RecordEvent(corev1.EventTypeWarning, "MountFailed", "Failed to mount block %s to %s due to err: %v",
			devPath, mountPoint, err)
		return err
	}

	v.RecordEvent(corev1.EventTypeNormal, "MountSuccess", "MountRawBlock %s to %s successfully",
		devPath, mountPoint)
	return nil
}

func (v *DiskVolumeHandler) MountFileSystem(devPath, mountPoint, fsType string, options ...string) error {
	// fixme: should check mount points again when do mount
	// mount twice will cause error
	if err := v.mounter.FormatAndMount(devPath, mountPoint, fsType, options); err != nil {
		v.RecordEvent(corev1.EventTypeWarning, "MountFailed", "Failed to mount filesystem %s to %s due to err: %v",
			devPath, mountPoint, err)
		return err
	}

	v.RecordEvent(corev1.EventTypeNormal, "MountSuccess", "MountFileSystem %s to %s successfully",
		devPath, mountPoint)
	return nil
}

func (v *DiskVolumeHandler) UnMount(mountPoint string) error {
	if err := v.mounter.Unmount(mountPoint); err != nil {
		// fixme: need consider raw block
		if !v.IsDevMountPoint(mountPoint) {
			v.RecordEvent(corev1.EventTypeWarning, "UnMountSuccess", "Unmount skipped due to mountpoint %s is empty or not mounted by disk %s",
				mountPoint, v.Ldv.Status.DevPath)
			return nil
		}

		v.RecordEvent(corev1.EventTypeWarning, "UnMountFailed", "Failed to umount %s due to err: %v",
			mountPoint, err)
		return err
	}

	v.RecordEvent(corev1.EventTypeNormal, "UnMountSuccess", "Unmount %s successfully",
		mountPoint)
	return nil
}

// IsDevMountPoint judge if this mountpoint is mounted by the dev
func (v *DiskVolumeHandler) IsDevMountPoint(mountPoint string) bool {
	for _, p := range v.mounter.GetDeviceMountPoints(v.Ldv.Status.DevPath) {
		if p == mountPoint {
			return true
		}
	}
	return false
}

func (v *DiskVolumeHandler) VolumeState() ldm.State {
	return v.Ldv.Status.State
}

func (v *DiskVolumeHandler) ExistMountPoint(targetPath string) bool {
	for _, mountPoint := range v.GetMountPoints() {
		if mountPoint.TargetPath == targetPath {
			return true
		}
	}

	return false
}

func (v *DiskVolumeHandler) AppendMountPoint(targetPath string, volCap *csi.VolumeCapability) {
	mountPoint := ldm.MountPoint{TargetPath: targetPath, Phase: ldm.MountPointToBeMounted}
	switch volCap.AccessType.(type) {
	case *csi.VolumeCapability_Block:
		mountPoint.VolumeCap = ldm.VolumeCapability{AccessType: ldm.VolumeCapability_AccessType_Block}
	case *csi.VolumeCapability_Mount:
		mountPoint.FsTye = volCap.GetMount().FsType
		mountPoint.MountOptions = volCap.GetMount().MountFlags
		mountPoint.VolumeCap = ldm.VolumeCapability{AccessType: ldm.VolumeCapability_AccessType_Mount}
	default:
	}
	v.Ldv.Status.MountPoints = append(v.Ldv.Status.MountPoints, mountPoint)
}

func (v *DiskVolumeHandler) MoveMountPoint(targetPath string) {
	for i, mountPoint := range v.GetMountPoints() {
		if mountPoint.TargetPath == targetPath {
			v.Ldv.Status.MountPoints = append(v.Ldv.Status.MountPoints[:i], v.Ldv.Status.MountPoints[i+1:]...)
			return
		}
	}
}

func (v *DiskVolumeHandler) UpdateMountPointPhase(targetPath string, phase ldm.State) {
	for i, mountPoint := range v.GetMountPoints() {
		if mountPoint.TargetPath == targetPath {
			v.Ldv.Status.MountPoints[i].Phase = phase
			return
		}
	}
}

// WaitVolumeReady wait LocalDiskVolume Ready
func (v *DiskVolumeHandler) WaitVolumeReady(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("no deadline is set")
	}

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context error occured when wait volume ready: %v", ctx.Err())
		case <-timer.C:
		}

		if err := v.RefreshVolume(); err != nil {
			return err
		}
		if v.VolumeState() == ldm.VolumeStateReady {
			return nil
		}
		timer.Reset(1 * time.Second)
	}
}

func (v *DiskVolumeHandler) WaitVolume(ctx context.Context, state ldm.State) error {
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("no deadline is set")
	}

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context error occured when wait volume ready: %v", ctx.Err())
		case <-timer.C:
		}

		if err := v.RefreshVolume(); err != nil {
			return err
		}
		if v.VolumeState() == state {
			return nil
		}
		timer.Reset(1 * time.Second)
	}
}

// WaitVolumeUnmounted wait a special mountpoint is unmounted
func (v *DiskVolumeHandler) WaitVolumeUnmounted(ctx context.Context, mountPoint string) error {
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("no deadline is set")
	}

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context error occured when wait volume ready: %v", ctx.Err())
		case <-timer.C:
		}

		if err := v.RefreshVolume(); err != nil {
			return err
		}
		if v.VolumeState() == ldm.VolumeStateReady && !v.ExistMountPoint(mountPoint) {
			return nil
		}
		timer.Reset(1 * time.Second)
	}
}

func (v *DiskVolumeHandler) RefreshVolume() error {
	newVolume, err := v.GetLocalDiskVolume(client.ObjectKey{
		Namespace: v.Ldv.GetNamespace(),
		Name:      v.Ldv.GetName()})
	if err != nil {
		return err
	}

	v.For(newVolume)
	return nil
}

func (v *DiskVolumeHandler) SetupVolumeStatus(status ldm.State) {
	v.Ldv.Status.State = status
}

func (v *DiskVolumeHandler) CheckFinalizers() error {
	finalizers := v.Ldv.GetFinalizers()
	_, ok := utils.StrFind(finalizers, LocalDiskFinalizer)
	if ok {
		return nil
	}

	// volume is ready for delete, don't append finalizer
	if v.VolumeState() == ldm.VolumeStateDeleted {
		return nil
	}

	// add localdisk finalizers to prevent resource deleted by mistake
	finalizers = append(finalizers, LocalDiskFinalizer)
	v.AddFinalizers(finalizers)
	if err := v.UpdateLocalDiskVolume(); err != nil {
		return err
	}

	return v.RefreshVolume()
}

func (v *DiskVolumeHandler) GetBoundDisk() string {
	return v.Ldv.Status.LocalDiskName
}

func (v *DiskVolumeHandler) For(volume *ldm.LocalDiskVolume) {
	v.Ldv = volume
}
