/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package controller implements the KubeVirt DataMover controller
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// DefaultMaxConcurrentReconciles is the default number of concurrent reconciles
	DefaultMaxConcurrentReconciles = 3

	// DefaultTempPVCSize is the default size for temporary backup PVC
	DefaultTempPVCSize = "10Gi"

	// RequeueAfterShort is the short requeue duration for polling
	RequeueAfterShort = 5 * time.Second

	// RequeueAfterLong is the longer requeue duration
	RequeueAfterLong = 30 * time.Second

	// bslValidatedValue is the annotation value indicating BSL validation is complete
	bslValidatedValue = "true"

	// defaultCredentialKey is the default key in BSL credential secrets
	defaultCredentialKey = "cloud"

	// AnnotationVMBTName is the annotation key for the generated VMBT name on a DataUpload
	AnnotationVMBTName = "kubevirt-datamover.io/vmbt-name"

	// k8sGenerateNameRandomLen is the number of random characters K8s appends to GenerateName
	k8sGenerateNameRandomLen = 5

	// maxVMBNameLen is the max allowed VMB name length. KubeVirt constructs a
	// hotplug volume name as "<vmb-name>-backup-target-pvc" which must be a
	// valid DNS label (≤ 63 chars). Reserve 18 chars for that suffix.
	maxVMBNameLen = 63 - len("-backup-target-pvc") // = 45
)

// KubeVirtDataUploadReconciler reconciles DataUpload objects where Spec.DataMover is "kubevirt"
type KubeVirtDataUploadReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// OADPNamespace is the namespace where OADP and Velero resources are located
	OADPNamespace string

	// MaxConcurrentReconciles is the maximum number of concurrent Reconciles which can be run
	MaxConcurrentReconciles int

	// DatamoverImage is the image to use for datamover pods
	DatamoverImage string

	// DatamoverImagePullPolicy is the pull policy for the datamover image
	DatamoverImagePullPolicy corev1.PullPolicy

	// ObjectStoreFactory creates an ObjectStore from an UploaderConfig.
	// Defaults to uploader.InitObjectStore if nil. Override in tests to inject mocks.
	ObjectStoreFactory func(cfg *uploader.UploaderConfig) (velero.ObjectStore, error)
}

// +kubebuilder:rbac:groups=velero.io,resources=datauploads,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=datauploads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=backupstoragelocations,verbs=get;list;watch
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackups/status,verbs=get
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackuptrackers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackuptrackers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=kubevirt.io,resources=virtualmachines,verbs=get;list;watch

// Reconcile handles DataUpload resources where Spec.DataMover is "kubevirt"
func (r *KubeVirtDataUploadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the DataUpload
	dataUpload := &velerov2alpha1.DataUpload{}
	if err := r.Get(ctx, req.NamespacedName, dataUpload); err != nil {
		// Ignore not-found errors, as the object may have been deleted
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Skip if DataMover is not "kubevirt"
	if dataUpload.Spec.DataMover != common.DataMoverKubeVirt {
		logger.V(1).Info("Skipping DataUpload - DataMover is not kubevirt",
			"dataUpload", req.NamespacedName,
			"dataMover", dataUpload.Spec.DataMover)
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling DataUpload with kubevirt datamover",
		"dataUpload", req.NamespacedName,
		"phase", dataUpload.Status.Phase)

	// Handle based on current phase
	switch dataUpload.Status.Phase {
	case "", velerov2alpha1.DataUploadPhaseNew:
		return r.handleNew(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseAccepted:
		return r.handleAccepted(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhasePrepared:
		return r.handlePrepared(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseInProgress:
		return r.handleInProgress(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseCanceling:
		return r.handleCanceling(ctx, logger, dataUpload)

	case velerov2alpha1.DataUploadPhaseCompleted,
		velerov2alpha1.DataUploadPhaseFailed,
		velerov2alpha1.DataUploadPhaseCanceled:
		// Terminal states - nothing to do
		logger.V(1).Info("DataUpload is in terminal state", "phase", dataUpload.Status.Phase)
		return ctrl.Result{}, nil

	default:
		logger.Info("Unknown DataUpload phase", "phase", dataUpload.Status.Phase)
		return ctrl.Result{}, nil
	}
}

// handleNew processes DataUploads in New phase
// Validates prerequisites and transitions to Accepted
func (r *KubeVirtDataUploadReconciler) handleNew(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling New phase DataUpload")

	// Step 1: Validate VM annotation exists
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference from DataUpload")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("Found VM reference", "vmName", vmRef.Name, "vmNamespace", vmRef.Namespace)

	// Step 2: Fetch the VirtualMachine and validate prerequisites
	vm := &kubevirtcorev1.VirtualMachine{}
	if err := r.Get(ctx, types.NamespacedName{Name: vmRef.Name, Namespace: vmRef.Namespace}, vm); err != nil {
		if errors.IsNotFound(err) {
			logger.Error(err, "VirtualMachine not found")
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("VirtualMachine %s/%s not found", vmRef.Namespace, vmRef.Name)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get VirtualMachine: %w", err)
	}

	// Step 3: Validate VM is running and CBT is enabled
	if err := common.ValidateVMForBackup(vm); err != nil {
		logger.Error(err, "VM validation failed")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, err.Error()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("VM validation passed", "vmName", vmRef.Name, "vmNamespace", vmRef.Namespace)

	// Transition to Accepted phase
	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseAccepted, "DataUpload accepted by kubevirt datamover"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// handleAccepted processes DataUploads in Accepted phase
// Creates VMBT/VMB and transitions to Prepared when ready
//
//nolint:gocyclo // Phase handler with necessary validation steps
func (r *KubeVirtDataUploadReconciler) handleAccepted(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Accepted phase DataUpload")

	// Extract VirtualMachine reference from annotation
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Serialize per VM: only the oldest DataUpload for a given VM proceeds.
	// All younger DUs wait until the older one reaches a terminal phase (Completed/Failed).
	// This ensures the previous backup's checkpoint is uploaded to S3 before the
	// next backup starts, enabling incremental backups.
	shouldWait, blockingDU, err := r.hasOlderActiveDUForVM(ctx, du)
	if err != nil {
		return ctrl.Result{}, err
	}
	if shouldWait {
		logger.Info("Another DataUpload is still active for this VM, waiting",
			"vm", vmRef.Name, "blockingDU", blockingDU)
		return ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
	}

	// Step 0: Verify BSL exists and is Available before creating any resources.
	// BSL must be in Available phase — if it's not, fail immediately rather than
	// creating resources (PVC, VMBT, VMB) that will be wasted.
	if du.Spec.BackupStorageLocation != "" {
		bslObj, err := r.getBackupStorageLocation(ctx, du)
		if err != nil {
			logger.Error(err, "BackupStorageLocation not accessible")
			if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("BackupStorageLocation not accessible: %v", err)); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
		if bslObj.Status.Phase != velerov1.BackupStorageLocationPhaseAvailable {
			logger.Error(nil, "BackupStorageLocation is not in Available phase",
				"bsl", bslObj.Name, "phase", bslObj.Status.Phase)
			if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("BackupStorageLocation %q is not available (phase: %s)",
					bslObj.Name, bslObj.Status.Phase)); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
	}

	// Step 1: Create or get temporary PVC for backup output
	pvc, err := r.ensureTempPVC(ctx, logger, du, vmRef.Namespace)
	if err != nil {
		logger.Error(err, "Failed to ensure temporary PVC")
		return ctrl.Result{}, err
	}
	logger.Info("Temporary PVC ready", "pvc", pvc.Name)

	// Check if VMB already exists. If it does, the VMBT is already in the right
	// state (set before VMB creation) and we can skip straight to monitoring.
	// This avoids deleting/recreating the VMBT while an active VMB references it.
	vmb, err := r.findVMBForDataUpload(ctx, du, vmRef.Namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check for existing VMB: %w", err)
	}

	if vmb == nil && du.Annotations != nil && du.Annotations[AnnotationVMBTName] != "" {
		// The VMBT annotation is set but the VMB isn't visible yet in the cached client.
		// This happens when a rapid re-reconcile (triggered by the annotation update) runs
		// before the informer cache has the VMB. Requeue to let the cache catch up.
		// Without this guard, prepareVMBackupTracker would delete the VMBT that the
		// (not-yet-visible) VMB references, leaving the VMB permanently stuck.
		logger.Info("VMBT already prepared but VMB not yet visible in cache, requeuing",
			"vmbtName", du.Annotations[AnnotationVMBTName])
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	if vmb == nil {
		// Step 2: Prepare VirtualMachineBackupTracker (recreate from S3 state)
		vmbt, err := r.prepareVMBackupTracker(ctx, logger, du, vmRef.Name, vmRef.Namespace)
		if err != nil {
			logger.Error(err, "Failed to prepare VirtualMachineBackupTracker")
			return ctrl.Result{}, err
		}
		logger.Info("VirtualMachineBackupTracker ready", "vmbt", vmbt.Name)

		// Store generated VMBT name in annotation for later stages
		if du.Annotations == nil {
			du.Annotations = make(map[string]string)
		}
		du.Annotations[AnnotationVMBTName] = vmbt.Name
		if err := r.Update(ctx, du); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to annotate DataUpload with VMBT name: %w", err)
		}

		// Step 3: Determine backup mode (full vs incremental).
		forceFullBackup, checkpointLookup := r.resolveBackupMode(ctx, logger, du, vmRef)

		// Step 3b: Cross-check VMBT checkpoint against BSL chain validation result.
		// The VMBT's LatestCheckpoint (restored from archived vmbt.json) must agree
		// with the BSL chain validation (which independently walks the S3 index).
		// If they diverge, the archived VMBT is stale — force a full backup.
		if !forceFullBackup && checkpointLookup != nil && checkpointLookup.Found {
			vmbtCheckpoint := ""
			if vmbt.Status != nil && vmbt.Status.LatestCheckpoint != nil {
				vmbtCheckpoint = vmbt.Status.LatestCheckpoint.Name
			}
			if vmbtCheckpoint != checkpointLookup.LatestCheckpoint {
				logger.Info("VMBT checkpoint does not match BSL chain validation result, forcing full backup",
					"vmbtCheckpoint", vmbtCheckpoint,
					"bslLatestCheckpoint", checkpointLookup.LatestCheckpoint)
				forceFullBackup = true
			}
		}

		// Step 4: Create VirtualMachineBackup
		var created bool
		vmb, created, err = r.ensureVMBackup(ctx, logger, du, vmbt, pvc.Name, vmRef.Namespace, forceFullBackup)
		if err != nil {
			// Check if the error is due to another VMB being in progress for the same VM.
			// KubeVirt's admission webhook only allows one active (non-terminal) VMB per VM.
			// Requeue with a longer delay instead of returning an error (which causes
			// exponential backoff retry storm).
			if strings.Contains(err.Error(), "in progress for source") {
				logger.Info("Another VirtualMachineBackup is in progress for this VM, will retry",
					"reason", err.Error())
				return ctrl.Result{RequeueAfter: RequeueAfterLong}, nil
			}
			logger.Error(err, "Failed to ensure VirtualMachineBackup")
			return ctrl.Result{}, err
		}

		if created {
			logger.Info("Created VirtualMachineBackup", "generateName", vmb.GenerateName, "namespace", vmRef.Namespace)
			// Requeue to check status
			return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
		}
	} else if vmb != nil {
		logger.V(1).Info("VirtualMachineBackup already exists, skipping VMBT preparation", "vmb", vmb.Name)
	}

	// Step 5: Check VMB status
	if vmb.Status == nil {
		// Before blindly requeuing, check if the VMB's BackupTracker still exists.
		// If the VMBT was deleted (by a concurrent DataUpload), KubeVirt will never
		// set status on this VMB, so we'd requeue forever.
		vmbtName := vmb.Spec.Source.Name
		vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
		if err := r.Get(ctx, types.NamespacedName{Name: vmbtName, Namespace: vmb.Namespace}, vmbt); err != nil {
			if errors.IsNotFound(err) {
				logger.Error(nil, "VirtualMachineBackup's BackupTracker was deleted before processing started",
					"vmb", vmb.Name, "vmbt", vmbtName)
				if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
					fmt.Sprintf("VMBackup stuck: BackupTracker %s no longer exists", vmbtName)); updateErr != nil {
					return ctrl.Result{}, updateErr
				}
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, fmt.Errorf("failed to check BackupTracker %s existence: %w", vmbtName, err)
		}
		logger.Info("VirtualMachineBackup status not yet available, requeuing")
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	return r.evaluateVMBackupStatus(ctx, logger, du, vmb)
}

// evaluateVMBackupStatus checks conditions on a VirtualMachineBackup to determine
// whether the backup succeeded, failed, or is still running.
// KubeVirt condition combinations:
//
//	Progressing=True  + Done=False       → backup still running
//	Progressing=False + Done=True        → backup completed successfully
//	Progressing=False + Done=False       → backup failed
//	Initializing=True + Progressing=False + Done=absent → stuck (e.g. VMBT deleted)
func (r *KubeVirtDataUploadReconciler) evaluateVMBackupStatus(
	ctx context.Context, logger logr.Logger,
	du *velerov2alpha1.DataUpload, vmb *kubevirtbackupv1alpha1.VirtualMachineBackup,
) (ctrl.Result, error) {
	var doneCond, progressingCond, initializingCond *kubevirtbackupv1alpha1.Condition
	for i := range vmb.Status.Conditions {
		switch vmb.Status.Conditions[i].Type {
		case kubevirtbackupv1alpha1.ConditionDone:
			doneCond = &vmb.Status.Conditions[i]
		case kubevirtbackupv1alpha1.ConditionProgressing:
			progressingCond = &vmb.Status.Conditions[i]
		case kubevirtbackupv1alpha1.ConditionInitializing:
			initializingCond = &vmb.Status.Conditions[i]
		}
	}

	if doneCond != nil && doneCond.Status == corev1.ConditionTrue {
		logger.Info("VirtualMachineBackup completed",
			"vmb", vmb.Name,
			"type", vmb.Status.Type,
			"checkpoint", vmb.Status.CheckpointName)

		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhasePrepared,
			fmt.Sprintf("VMBackup completed (type=%s)", vmb.Status.Type)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	if doneCond != nil && doneCond.Status == corev1.ConditionFalse {
		if progressingCond != nil && progressingCond.Status == corev1.ConditionFalse {
			// Progressing=False + Done=False → actual failure
			logger.Error(nil, "VirtualMachineBackup failed", "reason", doneCond.Reason, "message", doneCond.Message)
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("VMBackup failed: %s", doneCond.Message)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		// Done=False but Progressing is True or absent → backup still running
		logger.Info("VirtualMachineBackup still in progress (Done=False, waiting for completion)", "vmb", vmb.Name)
	}

	// Detect stuck initialization: Initializing=True + Progressing=False + no Done condition.
	// This can be transient (e.g. PVC being hotplugged to VMI) or permanent (VMBT deleted).
	// To distinguish, we verify the VMB's referenced BackupTracker still exists.
	// If it's gone, the VMB will never progress — fail immediately.
	// If it still exists, this is a normal transient state — keep requeuing.
	if doneCond == nil && initializingCond != nil && initializingCond.Status == corev1.ConditionTrue &&
		progressingCond != nil && progressingCond.Status == corev1.ConditionFalse {
		vmbtName := vmb.Spec.Source.Name
		vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
		if err := r.Get(ctx, types.NamespacedName{Name: vmbtName, Namespace: vmb.Namespace}, vmbt); err != nil {
			if errors.IsNotFound(err) {
				logger.Error(nil, "VirtualMachineBackup's BackupTracker was deleted, failing",
					"vmb", vmb.Name, "vmbt", vmbtName)
				if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
					fmt.Sprintf("VMBackup stuck: BackupTracker %s no longer exists", vmbtName)); updateErr != nil {
					return ctrl.Result{}, updateErr
				}
				return ctrl.Result{}, nil
			}
			// Transient API error — requeue
			return ctrl.Result{}, fmt.Errorf("failed to check BackupTracker %s existence: %w", vmbtName, err)
		}
		// VMBT exists, initialization is in progress (e.g. PVC being attached) — keep waiting
		logger.Info("VirtualMachineBackup initializing (VMBT exists, waiting for progress)",
			"vmb", vmb.Name, "reason", initializingCond.Reason)
	}

	// No Done condition yet, or backup still running - requeue
	logger.Info("VirtualMachineBackup in progress, requeuing")
	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// resolveBackupMode determines whether to force a full backup or allow incremental.
// Returns (forceFullBackup, checkpointLookup) where checkpointLookup is the BSL
// chain validation result (nil if BSL was unreachable or validation was skipped).
// This covers two scenarios:
//  1. User explicitly requested force-full-backup via annotation on DataUpload.
//  2. BSL checkpoint validation found a broken chain, requiring a forced full backup.
func (r *KubeVirtDataUploadReconciler) resolveBackupMode(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmRef *common.VMReference) (bool, *uploader.CheckpointLookupResult) {
	// Check if force full backup is requested via annotation.
	if du.Annotations[common.AnnotationForceFullBackup] == bslValidatedValue {
		logger.Info("Force full backup requested via annotation, skipping BSL checkpoint lookup")
		return true, nil
	}

	// Validate checkpoint chain in BSL for incremental backup support.
	// This runs once per DataUpload (tracked via annotation) to avoid redundant
	// S3 queries on every reconcile, while still validating the BSL state for
	// each new backup.
	if du.Annotations[common.AnnotationBSLValidated] == bslValidatedValue {
		logger.V(1).Info("BSL validation already completed for this DataUpload")
		return false, nil
	}

	return r.validateBSLCheckpoint(ctx, logger, du, vmRef)
}

// validateBSLCheckpoint queries the BSL for a valid checkpoint chain and determines
// whether a forced full backup is required because the chain is broken.
// Returns (forceFullBackup, checkpointLookup) where checkpointLookup is the BSL
// chain validation result (nil if BSL was unreachable or lookup failed).
// It does NOT modify VMBT status — ForceFullBackup on the VMB is the sole mechanism
// for forcing full backups.
func (r *KubeVirtDataUploadReconciler) validateBSLCheckpoint(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmRef *common.VMReference) (bool, *uploader.CheckpointLookupResult) {
	forceFullBackup := false

	var checkpointLookup *uploader.CheckpointLookupResult
	bsl, bslErr := r.getBackupStorageLocation(ctx, du)
	if bslErr != nil {
		// BSL lookup failure is non-fatal. Validation will be retried on the
		// next reconcile.
		logger.Info("BSL not available for checkpoint lookup, skipping validation",
			"reason", bslErr.Error())
	} else {
		var err error
		checkpointLookup, err = r.lookupCheckpointFromBSL(ctx, bsl, vmRef.Namespace, vmRef.Name)
		if err != nil {
			// Checkpoint lookup failure is non-fatal. Validation will be retried
			// on the next reconcile.
			logger.Info("Checkpoint lookup failed, skipping validation",
				"reason", err.Error())
		} else if checkpointLookup != nil {
			logger.Info("Checkpoint lookup completed",
				"found", checkpointLookup.Found,
				"message", checkpointLookup.Message)
		}
	}

	// Determine whether to force full backup based on BSL validation result.
	// Only act on definitive results (checkpointLookup != nil). If BSL was
	// unreachable or lookup errored, checkpointLookup is nil and we skip
	// (don't force full backup due to transient failures).
	if checkpointLookup != nil && !checkpointLookup.IsChainValid {
		// Chain is broken — force full backup via VMB.Spec.ForceFullBackup
		logger.Info("BSL checkpoint chain is invalid, forcing full backup",
			"message", checkpointLookup.Message)
		forceFullBackup = true
	} else if checkpointLookup != nil && checkpointLookup.Found && checkpointLookup.IsChainValid {
		// Valid chain exists — allow incremental backup
		logger.Info("BSL checkpoint chain is valid, allowing incremental backup",
			"checkpoint", checkpointLookup.LatestCheckpoint,
			"chainLength", checkpointLookup.ChainLength)
	} else if checkpointLookup != nil && !checkpointLookup.Found {
		// No checkpoint found — first backup or all data deleted.
		// ForceFullBackup not strictly needed (KubeVirt will do full anyway
		// without a checkpoint), but set it for clarity.
		logger.Info("No valid checkpoint found in BSL, will perform full backup",
			"message", checkpointLookup.Message)
		forceFullBackup = true
	}

	// Mark BSL validation as done for this DataUpload to avoid redundant S3 queries.
	// Only set the annotation when we got a definitive result from BSL (checkpointLookup != nil).
	// If BSL was unreachable or the lookup failed due to transient errors (e.g., read-only
	// filesystem, network errors), don't set the annotation so validation will be retried
	// on the next reconcile.
	if checkpointLookup != nil {
		if du.Annotations == nil {
			du.Annotations = make(map[string]string)
		}
		du.Annotations[common.AnnotationBSLValidated] = bslValidatedValue
		if err := r.Update(ctx, du); err != nil {
			// Non-fatal: worst case we re-run BSL validation on next reconcile
			logger.Info("Failed to set BSL validated annotation, will retry",
				"reason", err.Error())
		}
	}

	return forceFullBackup, checkpointLookup
}

// handlePrepared processes DataUploads in Prepared phase
// Rebinds PV to OADP namespace, launches datamover pod, and transitions to InProgress
//
//nolint:gocyclo // Phase handler with necessary validation steps
func (r *KubeVirtDataUploadReconciler) handlePrepared(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Prepared phase DataUpload")

	// Get VM reference for namespace context
	vmRef, err := common.GetVMReference(du)
	if err != nil {
		logger.Error(err, "Failed to get VM reference")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Missing VM reference: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Datamover pod runs in OADP namespace (where credentials are accessible)
	podNamespace := r.getPodNamespace(du)

	// Check if datamover pod already exists (idempotency)
	pod, err := r.findPodForDataUpload(ctx, du, podNamespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check for existing datamover pod: %w", err)
	}
	if pod != nil {
		// Pod exists, transition to InProgress and monitor
		logger.Info("Datamover pod already exists, transitioning to InProgress", "pod", pod.Name)
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod running"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	// Validate BSL and VMB exist BEFORE rebinding PV
	// This prevents leaving PV in a bad state if these checks fail

	// Get BackupStorageLocation
	bsl, err := r.getBackupStorageLocation(ctx, du)
	if err != nil {
		logger.Error(err, "Failed to get BackupStorageLocation")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to get BSL: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Get VMB to extract checkpoint info
	vmb, err := r.findVMBForDataUpload(ctx, du, vmRef.Namespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get VirtualMachineBackup: %w", err)
	}
	if vmb == nil {
		logger.Error(nil, "VirtualMachineBackup not found for DataUpload")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, "VirtualMachineBackup not found"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Check if PV has already been rebound (idempotency)
	reboundPVCName := common.SafeResourceName(common.ReboundPVCNamePrefix, du.Name)
	reboundPVC := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, types.NamespacedName{Name: reboundPVCName, Namespace: podNamespace}, reboundPVC)
	pvAlreadyRebound := err == nil && reboundPVC.Status.Phase == corev1.ClaimBound

	if !pvAlreadyRebound {
		// Rebind PV from VM namespace to OADP namespace
		// This allows the datamover pod to access both the backup data AND credentials
		//
		// The temp PVC was created with GenerateName (random suffix), so we read
		// the actual name from the VMB spec rather than recomputing it.
		if vmb.Spec.PvcName == nil || *vmb.Spec.PvcName == "" {
			err := fmt.Errorf("VirtualMachineBackup %s has no PVC name in spec", vmb.Name)
			logger.Error(err, "Cannot determine source PVC for rebinding")
			if updateErr := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, err.Error()); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
		sourcePVCName := *vmb.Spec.PvcName

		logger.Info("Rebinding PV from VM namespace to OADP namespace",
			"sourcePVC", sourcePVCName,
			"sourceNamespace", vmRef.Namespace,
			"targetNamespace", podNamespace)

		rebindResult, err := r.rebindPVToNamespace(ctx, logger, sourcePVCName, vmRef.Namespace, podNamespace, du.Name, string(du.UID))
		if err != nil {
			// Fail without retry: PV rebind is a multi-step operation (delete PVC, patch PV, create new PVC).
			// If it fails partway through, automatic retries could leave resources in an inconsistent state.
			// Failing allows the user to investigate and take corrective action.
			logger.Error(err, "Failed to rebind PV to OADP namespace")
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to rebind PV: %v", err)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}

		logger.Info("Successfully rebound PV to OADP namespace",
			"newPVC", rebindResult.NewPVCName,
			"pv", rebindResult.PVName)

		reboundPVCName = rebindResult.NewPVCName
	} else {
		logger.Info("PV already rebound to OADP namespace", "pvc", reboundPVCName)
	}

	// Get backup type from VMB status
	backupType := "full"
	if vmb.Status != nil && vmb.Status.Type != "" {
		backupType = string(vmb.Status.Type)
	}

	// Get checkpoint name from VMB status
	checkpointName := ""
	if vmb.Status != nil && vmb.Status.CheckpointName != nil {
		checkpointName = *vmb.Status.CheckpointName
	}

	// Get VMBT name from annotation
	vmbtName := ""
	if du.Annotations != nil {
		vmbtName = du.Annotations[AnnotationVMBTName]
	}
	if vmbtName == "" {
		err := fmt.Errorf("VMBT name annotation %s not found on DataUpload", AnnotationVMBTName)
		logger.Error(err, "Failed to get VMBT name")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, err.Error()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Build datamover pod config - now using OADP namespace and rebound PVC
	podConfig, err := r.buildDatamoverPodConfig(du, bsl, vmb, vmRef, backupType, checkpointName, vmbtName)
	if err != nil {
		logger.Error(err, "Failed to build datamover pod config")
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Failed to build pod config: %v", err)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Override namespace and PVC name for the rebound resources
	podConfig.Namespace = podNamespace
	podConfig.SourcePVCName = reboundPVCName

	// Create the datamover pod
	podToCreate := buildDatamoverPod(podConfig)

	// Use GenerateName instead of a fixed name
	podToCreate.GenerateName = safeGenerateNamePrefix(fmt.Sprintf("%s%s-", common.DatamoverPodNamePrefix, du.Name), 63)
	podToCreate.Name = ""

	// Set owner reference so pod is cleaned up when DataUpload is deleted
	// This works now because pod is in the same namespace as DataUpload
	if err := controllerutil.SetOwnerReference(du, podToCreate, r.Scheme); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set owner reference on pod: %w", err)
	}

	if err := r.Create(ctx, podToCreate); err != nil {
		if errors.IsAlreadyExists(err) {
			// Race condition - pod was created between check and create
			logger.Info("Datamover pod already exists (race)", "generateName", podToCreate.GenerateName)
		} else {
			return ctrl.Result{}, fmt.Errorf("failed to create datamover pod: %w", err)
		}
	} else {
		logger.Info("Created datamover pod", "generateName", podToCreate.GenerateName, "namespace", podNamespace)
	}

	// Transition to InProgress
	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseInProgress, "Datamover pod launched"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// handleInProgress processes DataUploads in InProgress phase
// Monitors datamover pod and transitions to Completed/Failed
func (r *KubeVirtDataUploadReconciler) handleInProgress(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling InProgress phase DataUpload")

	// Datamover pod runs in OADP namespace
	podNamespace := r.getPodNamespace(du)

	// Get the datamover pod
	pod, err := r.findPodForDataUpload(ctx, du, podNamespace)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get datamover pod: %w", err)
	}
	if pod == nil {
		// Pod not found - this is unexpected in InProgress phase
		logger.Error(nil, "Datamover pod not found", "dataUpload", du.Name, "namespace", podNamespace)
		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, "Datamover pod not found"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Check pod status
	switch pod.Status.Phase {
	case corev1.PodSucceeded:
		logger.Info("Datamover pod completed successfully", "pod", pod.Name)

		// Cleanup resources
		r.cleanupDatamoverResources(ctx, logger, du, podNamespace)

		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseCompleted, "Data upload completed"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case corev1.PodFailed:
		failureMessage := extractPodFailureMessage(pod)
		logger.Error(nil, "Datamover pod failed", "pod", pod.Name, "message", failureMessage)

		// Skip cleanup on failure to preserve resources for debugging.
		// Resources (pod, rebound PVC/PV) can be manually cleaned up after investigation.

		if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed, fmt.Sprintf("Datamover pod failed: %s", failureMessage)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case corev1.PodPending, corev1.PodRunning:
		logger.V(1).Info("Datamover pod still running", "pod", pod.Name, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil

	default:
		logger.Info("Datamover pod in unknown phase", "pod", pod.Name, "phase", pod.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}
}

// cleanupDatamoverResources cleans up resources created during the datamover process
func (r *KubeVirtDataUploadReconciler) cleanupDatamoverResources(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, podNamespace string) {
	// Delete the datamover pod
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(podNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		logger.Error(err, "Failed to list datamover pods for cleanup")
	} else {
		for i := range podList.Items {
			pod := &podList.Items[i]
			if err := r.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete datamover pod", "pod", pod.Name)
			} else {
				logger.Info("Deleted datamover pod", "pod", pod.Name)
			}
		}
	}

	// Delete the rebound PVC and PV
	reboundPVCName := common.SafeResourceName(common.ReboundPVCNamePrefix, du.Name)
	if err := r.cleanupReboundPVCAndPV(ctx, logger, reboundPVCName, podNamespace, string(du.UID)); err != nil {
		logger.Error(err, "Failed to cleanup rebound PVC and PV", "pvc", reboundPVCName)
		// Continue - don't block completion on cleanup failures
	}

}

// cleanupVMBackupResources deletes VMB CRs in the VM namespace.
// Used during cancellation when the datamover pod won't run its own cleanup.
// The VMBT is intentionally preserved so KubeVirt can use it during VM lifecycle
// events (restarts, migrations) to redefine libvirt checkpoints.
// See https://github.com/migtools/kubevirt-datamover-controller/issues/32.
func (r *KubeVirtDataUploadReconciler) cleanupVMBackupResources(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmNamespace string) {
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := r.List(ctx, vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		logger.Error(err, "Failed to list VMBs for cleanup")
	} else {
		for i := range vmbList.Items {
			vmb := &vmbList.Items[i]
			if err := r.Delete(ctx, vmb); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete VMB", "vmb", vmb.Name)
			} else {
				logger.Info("Deleted VMB", "vmb", vmb.Name, "namespace", vmNamespace)
			}
		}
	}
}

// handleCanceling processes DataUploads in Canceling phase
// Cleans up resources and transitions to Canceled
func (r *KubeVirtDataUploadReconciler) handleCanceling(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Canceling phase DataUpload")

	// Datamover pod runs in OADP namespace
	podNamespace := r.getPodNamespace(du)

	// Clean up datamover resources in OADP namespace
	r.cleanupDatamoverResources(ctx, logger, du, podNamespace)

	// Clean up VMB and VMBT in the VM namespace.
	// When canceling, the datamover pod won't run its cleanup, so we handle it here.
	vmRef, _ := common.GetVMReference(du)
	if vmRef != nil {
		r.cleanupVMBackupResources(ctx, logger, du, vmRef.Namespace)
	}

	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseCanceled, "DataUpload canceled"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// updatePhase updates the DataUpload phase and status message
// Uses Update instead of Status().Patch() to match Velero's approach,
// which works regardless of whether the CRD has status subresource enabled
func (r *KubeVirtDataUploadReconciler) updatePhase(ctx context.Context, du *velerov2alpha1.DataUpload, phase velerov2alpha1.DataUploadPhase, message string) error {
	logger := log.FromContext(ctx)

	// Skip update if already at target phase with same message (idempotency)
	if du.Status.Phase == phase && du.Status.Message == message {
		logger.V(1).Info("DataUpload already at target phase with same message, skipping update",
			"dataUpload", du.Name,
			"phase", phase)
		return nil
	}

	du.Status.Phase = phase
	du.Status.Message = message

	if err := r.Update(ctx, du); err != nil {
		logger.Error(err, "Failed to update DataUpload phase",
			"dataUpload", du.Name,
			"phase", phase)
		return fmt.Errorf("failed to update DataUpload phase to %s: %w", phase, err)
	}

	logger.Info("Updated DataUpload phase",
		"dataUpload", du.Name,
		"phase", phase,
		"message", message)

	return nil
}

// getPodNamespace returns the namespace where datamover pods should run.
// Uses OADPNamespace if configured, otherwise falls back to the DataUpload's namespace.
func (r *KubeVirtDataUploadReconciler) getPodNamespace(du *velerov2alpha1.DataUpload) string {
	if r.OADPNamespace != "" {
		return r.OADPNamespace
	}
	return du.Namespace
}

// SetupWithManager sets up the controller with the Manager
func (r *KubeVirtDataUploadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrentReconciles
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov2alpha1.DataUpload{}).
		WithEventFilter(r.filterKubeVirtDataMover()).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrent,
		}).
		Named("kubevirt-dataupload").
		Complete(r)
}

// filterKubeVirtDataMover returns a predicate that filters for DataUploads
// where Spec.DataMover is "kubevirt"
func (r *KubeVirtDataUploadReconciler) filterKubeVirtDataMover() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		du, ok := obj.(*velerov2alpha1.DataUpload)
		if !ok {
			return false
		}
		return du.Spec.DataMover == common.DataMoverKubeVirt
	})
}

// ensureTempPVC creates or retrieves the temporary PVC for backup output.
// Note: We don't set an owner reference because the PVC is in VM namespace
// while DataUpload is in OADP namespace (cross-namespace owner refs not allowed).
// The PVC will be cleaned up during PV rebinding or explicit cleanup.
func (r *KubeVirtDataUploadReconciler) ensureTempPVC(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, namespace string) (*corev1.PersistentVolumeClaim, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList, client.InNamespace(namespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		return nil, fmt.Errorf("failed to list temporary PVCs: %w", err)
	}
	if len(pvcList.Items) > 1 {
		return nil, fmt.Errorf("found multiple temporary PVCs for DataUpload %s", du.Name)
	}
	if len(pvcList.Items) == 1 {
		logger.V(1).Info("Temporary PVC already exists", "pvc", pvcList.Items[0].Name)
		return &pvcList.Items[0], nil
	}

	// Create new PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: safeGenerateNamePrefix(fmt.Sprintf("kubevirt-backup-%s-", du.Name), 63),
			Namespace:    namespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: du.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(DefaultTempPVCSize),
				},
			},
		},
	}

	if err := r.Create(ctx, pvc); err != nil {
		return nil, fmt.Errorf("failed to create temporary PVC: %w", err)
	}

	logger.Info("Created temporary PVC", "generateName", pvc.GenerateName, "namespace", namespace)
	return pvc, nil
}

// prepareVMBackupTracker returns an existing on-cluster VirtualMachineBackupTracker
// for the VM if one exists, or creates a new one (restoring LatestCheckpoint from
// S3 if available). The VMBT is left on-cluster between backups so KubeVirt can
// use it during VM lifecycle events (restarts, migrations) to redefine libvirt
// checkpoints. See https://github.com/migtools/kubevirt-datamover-controller/issues/32.
func (r *KubeVirtDataUploadReconciler) prepareVMBackupTracker(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmName, vmNamespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackupTracker, error) {
	// Check for an existing on-cluster VMBT for this VM.
	existingVMBTList := &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerList{}
	if err := r.List(ctx, existingVMBTList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelVMNameHash: common.HashForLabel(vmName)}); err != nil {
		return nil, fmt.Errorf("failed to list existing VMBTs: %w", err)
	}

	// Reuse the on-cluster VMBT if one exists. KubeVirt needs it to persist
	// across backups for checkpoint redefinition during VM lifecycle events.
	if len(existingVMBTList.Items) > 0 {
		vmbt := &existingVMBTList.Items[0]
		logger.Info("Reusing existing on-cluster VMBT", "vmbt", vmbt.Name)
		return vmbt, nil
	}

	// No VMBT on-cluster (e.g., first backup or namespace was recreated).
	// Try to fetch the archived VMBT from S3 to restore LatestCheckpoint.
	// This is non-fatal: if BSL is unreachable or no VMBT exists yet (first backup),
	// we create a fresh VMBT without a checkpoint.
	var archivedVMBT *kubevirtbackupv1alpha1.VirtualMachineBackupTracker
	bsl, bslErr := r.getBackupStorageLocation(ctx, du)
	if bslErr != nil {
		logger.Info("BSL not available for VMBT lookup, creating fresh VMBT",
			"reason", bslErr.Error())
	} else {
		var err error
		archivedVMBT, err = r.lookupLatestVMBTFromBSL(ctx, bsl, vmNamespace, vmName)
		if err != nil {
			logger.Info("Failed to lookup VMBT from BSL, creating fresh VMBT",
				"reason", err.Error())
		} else if archivedVMBT != nil {
			cpName := ""
			if archivedVMBT.Status != nil && archivedVMBT.Status.LatestCheckpoint != nil {
				cpName = archivedVMBT.Status.LatestCheckpoint.Name
			}
			logger.Info("Found archived VMBT in BSL",
				"latestCheckpoint", cpName)
		}
	}

	// Create new VMBT
	apiGroup := "kubevirt.io"
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: safeGenerateNamePrefix(fmt.Sprintf("vmbt-%s-", vmName), 63),
			Namespace:    vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash:    common.HashForLabel(vmName),
				common.LabelDataUploadUID: string(du.UID),
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: du.Name,
				common.AnnotationVMName:         vmName,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	if err := r.Create(ctx, vmbt); err != nil {
		return nil, fmt.Errorf("failed to create VirtualMachineBackupTracker: %w", err)
	}

	logger.Info("Created VirtualMachineBackupTracker", "generateName", vmbt.GenerateName, "namespace", vmNamespace)

	// If we have an archived VMBT with LatestCheckpoint, set it on the new VMBT's status.
	// K8s ignores Status during creation, so we must use Status().Update() separately.
	if archivedVMBT != nil && archivedVMBT.Status != nil && archivedVMBT.Status.LatestCheckpoint != nil {
		vmbt.Status = &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: archivedVMBT.Status.LatestCheckpoint.DeepCopy(),
		}
		if err := r.Status().Update(ctx, vmbt); err != nil {
			return nil, fmt.Errorf("failed to update VMBT status with LatestCheckpoint: %w", err)
		}
		logger.Info("Set VMBT LatestCheckpoint from S3 archive",
			"vmbt", vmbt.Name,
			"latestCheckpoint", archivedVMBT.Status.LatestCheckpoint.Name)
	}

	return vmbt, nil
}

// lookupLatestVMBTFromBSL reads the VM's checkpoint index from the BSL, finds the
// latest checkpoint's vmbtObjectPath, fetches the archived vmbt.json, and returns
// the deserialized VMBT. Returns nil if no VMBT is archived (first backup or old-format index).
func (r *KubeVirtDataUploadReconciler) lookupLatestVMBTFromBSL(ctx context.Context, bsl *velerov1.BackupStorageLocation, vmNamespace, vmName string) (*kubevirtbackupv1alpha1.VirtualMachineBackupTracker, error) {
	store, cfg, err := r.initObjectStoreFromBSL(ctx, bsl)
	if err != nil {
		return nil, err
	}

	// Read the VM checkpoint index
	indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json", vmNamespace, vmName)
	exists, err := store.ObjectExists(cfg.Bucket, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check VM index existence: %w", err)
	}
	if !exists {
		return nil, nil // No index = first backup
	}

	reader, err := store.GetObject(cfg.Bucket, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read VM index: %w", err)
	}
	indexData, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read VM index data: %w", err)
	}

	var vmIndex uploader.VMIndex
	if err := json.Unmarshal(indexData, &vmIndex); err != nil {
		return nil, fmt.Errorf("failed to parse VM index: %w", err)
	}

	if len(vmIndex.Checkpoints) == 0 {
		return nil, nil // Empty index
	}

	// Get the latest checkpoint's VMBTObjectPath
	latestCP := vmIndex.Checkpoints[len(vmIndex.Checkpoints)-1]
	if latestCP.VMBTObjectPath == "" {
		return nil, nil // Old-format index without vmbtObjectPath
	}

	// Fetch the archived vmbt.json
	vmbtReader, err := store.GetObject(cfg.Bucket, latestCP.VMBTObjectPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read archived vmbt.json at %s: %w", latestCP.VMBTObjectPath, err)
	}
	vmbtData, err := io.ReadAll(vmbtReader)
	_ = vmbtReader.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read vmbt.json data: %w", err)
	}

	var vmbt kubevirtbackupv1alpha1.VirtualMachineBackupTracker
	if err := json.Unmarshal(vmbtData, &vmbt); err != nil {
		return nil, fmt.Errorf("failed to parse archived vmbt.json: %w", err)
	}

	return &vmbt, nil
}

// ensureVMBackup creates or retrieves the VirtualMachineBackup for this DataUpload.
// Returns the VMB, whether it was created (vs already existed), and any error.
// When forceFullBackup is true, the VMB is created with ForceFullBackup=true in its spec,
// which tells KubeVirt to perform a full backup regardless of any existing checkpoint.
// Note: We don't set an owner reference because VMB is in VM namespace
// while DataUpload is in OADP namespace (cross-namespace owner refs not allowed).
// VMB and VMBT are archived to S3 and deleted by the datamover pod after upload.
func (r *KubeVirtDataUploadReconciler) ensureVMBackup(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker, pvcName, namespace string, forceFullBackup bool) (*kubevirtbackupv1alpha1.VirtualMachineBackup, bool, error) {
	// Find existing VMB for this DataUpload
	existingVMB, err := r.findVMBForDataUpload(ctx, du, namespace)
	if err != nil {
		return nil, false, fmt.Errorf("failed to check for existing VMB: %w", err)
	}
	if existingVMB != nil {
		logger.V(1).Info("VirtualMachineBackup already exists", "vmb", existingVMB.Name)
		return existingVMB, false, nil
	}

	// Create new VMB referencing the VMBT (enables incremental backups)
	apiGroup := "backup.kubevirt.io"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: safeGenerateNamePrefix(fmt.Sprintf("vmb-%s-", du.Name), maxVMBNameLen),
			Namespace:    namespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: du.Name,
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName:         &pvcName,
			ForceFullBackup: forceFullBackup,
		},
	}

	if err := r.Create(ctx, vmb); err != nil {
		return nil, false, fmt.Errorf("failed to create VirtualMachineBackup: %w", err)
	}
	if forceFullBackup {
		logger.Info("Creating VirtualMachineBackup with ForceFullBackup=true", "vmb", vmb.Name)
	}

	logger.Info("Created VirtualMachineBackup", "generateName", vmb.GenerateName, "namespace", namespace, "tracker", vmbt.Name)
	return vmb, true, nil
}

// getBackupStorageLocation fetches the BSL referenced by the DataUpload
func (r *KubeVirtDataUploadReconciler) getBackupStorageLocation(ctx context.Context, du *velerov2alpha1.DataUpload) (*velerov1.BackupStorageLocation, error) {
	bslName := du.Spec.BackupStorageLocation
	if bslName == "" {
		return nil, fmt.Errorf("DataUpload %s has no BackupStorageLocation specified", du.Name)
	}

	// BSL is in the OADP namespace (where Velero runs)
	namespace := r.OADPNamespace
	if namespace == "" {
		// Fall back to the DataUpload's namespace
		namespace = du.Namespace
	}

	bsl := &velerov1.BackupStorageLocation{}
	if err := r.Get(ctx, types.NamespacedName{Name: bslName, Namespace: namespace}, bsl); err != nil {
		return nil, fmt.Errorf("failed to get BackupStorageLocation %s/%s: %w", namespace, bslName, err)
	}

	return bsl, nil
}

// findVMBForDataUpload finds the unique VirtualMachineBackup associated with a DataUpload.
func (r *KubeVirtDataUploadReconciler) findVMBForDataUpload(ctx context.Context, du *velerov2alpha1.DataUpload, namespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackup, error) {
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := r.List(ctx, vmbList, client.InNamespace(namespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		return nil, err
	}
	if len(vmbList.Items) == 0 {
		return nil, nil
	}
	if len(vmbList.Items) > 1 {
		return nil, fmt.Errorf("found multiple VirtualMachineBackups for DataUpload %s", du.Name)
	}
	return &vmbList.Items[0], nil
}

// isVMBTerminal returns true if the VMB has reached a terminal state:
// Done=True (success) or Done=False + Progressing=False (failure).
func isVMBTerminal(vmb *kubevirtbackupv1alpha1.VirtualMachineBackup) bool {
	if vmb.Status == nil {
		return false
	}
	var doneCond, progressingCond *kubevirtbackupv1alpha1.Condition
	for i := range vmb.Status.Conditions {
		switch vmb.Status.Conditions[i].Type {
		case kubevirtbackupv1alpha1.ConditionDone:
			doneCond = &vmb.Status.Conditions[i]
		case kubevirtbackupv1alpha1.ConditionProgressing:
			progressingCond = &vmb.Status.Conditions[i]
		}
	}
	if doneCond == nil {
		return false
	}
	if doneCond.Status == corev1.ConditionTrue {
		return true
	}
	return progressingCond != nil && progressingCond.Status == corev1.ConditionFalse
}

// hasOlderActiveDUForVM checks if an older DataUpload targeting the same VM is
// still in an active phase (Accepted, Prepared, InProgress). This serializes
// backups per VM so that each backup completes (including S3 upload) before the
// next one starts, enabling incremental backups through checkpoint chaining.
// The oldest DU (by CreationTimestamp, then UID) always wins.
func (r *KubeVirtDataUploadReconciler) hasOlderActiveDUForVM(ctx context.Context, du *velerov2alpha1.DataUpload) (bool, string, error) {
	vmName := du.Annotations[common.AnnotationVMName]
	if vmName == "" {
		return false, "", nil
	}
	vmNamespace := du.Annotations[common.AnnotationVMNamespace]
	if vmNamespace == "" {
		vmNamespace = du.Spec.SourceNamespace
	}

	duList := &velerov2alpha1.DataUploadList{}
	if err := r.List(ctx, duList, client.InNamespace(du.Namespace)); err != nil {
		return false, "", fmt.Errorf("failed to list DataUploads: %w", err)
	}

	for i := range duList.Items {
		other := &duList.Items[i]
		if other.UID == du.UID {
			continue
		}
		if other.Spec.DataMover != common.DataMoverKubeVirt {
			continue
		}

		// Must target the same VM
		otherVMName := other.Annotations[common.AnnotationVMName]
		if otherVMName != vmName {
			continue
		}
		otherVMNs := other.Annotations[common.AnnotationVMNamespace]
		if otherVMNs == "" {
			otherVMNs = other.Spec.SourceNamespace
		}
		if otherVMNs != vmNamespace {
			continue
		}

		// Must be in an active (non-terminal) phase
		switch other.Status.Phase {
		case "", velerov2alpha1.DataUploadPhaseNew,
			velerov2alpha1.DataUploadPhaseAccepted,
			velerov2alpha1.DataUploadPhasePrepared,
			velerov2alpha1.DataUploadPhaseInProgress:
		default:
			continue
		}

		// Older DU takes priority; same timestamp uses UID as tiebreaker
		if other.CreationTimestamp.Before(&du.CreationTimestamp) {
			return true, other.Name, nil
		}
		if other.CreationTimestamp.Equal(&du.CreationTimestamp) && string(other.UID) < string(du.UID) {
			return true, other.Name, nil
		}
	}
	return false, "", nil
}

// getVeleroBackupName extracts the Velero backup name from DataUpload labels
func getVeleroBackupName(du *velerov2alpha1.DataUpload) string {
	if du.Labels == nil {
		return ""
	}
	return du.Labels[common.LabelVeleroBackupName]
}

// safeGenerateNamePrefix truncates a GenerateName prefix so that the final
// name (prefix + 5 random chars) does not exceed maxNameLen.
func safeGenerateNamePrefix(prefix string, maxNameLen int) string {
	maxPrefix := max(maxNameLen-k8sGenerateNameRandomLen, 1)
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
	}
	return prefix
}

// buildDatamoverPodConfig assembles the configuration for the datamover pod
func (r *KubeVirtDataUploadReconciler) buildDatamoverPodConfig(
	du *velerov2alpha1.DataUpload,
	bsl *velerov1.BackupStorageLocation,
	vmb *kubevirtbackupv1alpha1.VirtualMachineBackup,
	vmRef *common.VMReference,
	backupType string,
	checkpointName string,
	vmbtName string,
) (*DatamoverPodConfig, error) {
	cfg, err := extractBSLConfig(bsl)
	if err != nil {
		return nil, err
	}

	if cfg.CredentialName == "" {
		return nil, fmt.Errorf("BSL %s has no credential secret configured", bsl.Name)
	}

	// Determine datamover image
	image := r.DatamoverImage
	if image == "" {
		return nil, fmt.Errorf("datamover image not configured")
	}

	pullPolicy := r.DatamoverImagePullPolicy
	if pullPolicy == "" {
		pullPolicy = corev1.PullAlways
	}

	return &DatamoverPodConfig{
		Name:                 du.Name, // Used as a prefix for GenerateName
		Namespace:            vmRef.Namespace,
		Image:                image,
		ImagePullPolicy:      pullPolicy,
		BSLProvider:          cfg.Provider,
		BSLBucket:            cfg.Bucket,
		BSLPrefix:            cfg.Prefix,
		BSLRegion:            cfg.Region,
		CredentialSecretName: cfg.CredentialName,
		CredentialSecretKey:  cfg.CredentialKey,
		VMName:               vmRef.Name,
		VMNamespace:          vmRef.Namespace,
		CheckpointName:       checkpointName,
		BackupType:           backupType,
		VeleroBackupName:     getVeleroBackupName(du),
		DataUploadName:       du.Name,
		DataUploadUID:        string(du.UID),
		VMBName:              vmb.Name,
		VMBTName:             vmbtName,
		SourcePVCName:        "", // overridden by handlePrepared with the rebound PVC name
		Labels:               make(map[string]string),
	}, nil
}

// extractPodFailureMessage extracts the failure message from a failed pod
func extractPodFailureMessage(pod *corev1.Pod) string {
	// Check container statuses for termination message
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason
		}
	}

	// Check init container statuses
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}
	}

	// Fall back to pod conditions
	for _, cond := range pod.Status.Conditions {
		if cond.Status == corev1.ConditionFalse && cond.Message != "" {
			return cond.Message
		}
	}

	return "unknown error"
}

// bslConfig holds extracted BSL configuration used by both the datamover pod
// and the checkpoint lookup.
type bslConfig struct {
	Provider       string
	Bucket         string
	Prefix         string // Datamover prefix (e.g., "velero-kubevirt-datamover")
	Region         string
	CredentialName string
	CredentialKey  string
}

// extractBSLConfig extracts and validates common BSL configuration fields.
// The returned prefix includes the datamover suffix (e.g., "velero-kubevirt-datamover").
func extractBSLConfig(bsl *velerov1.BackupStorageLocation) (*bslConfig, error) {
	bucket := ""
	prefix := ""
	if bsl.Spec.ObjectStorage != nil {
		bucket = bsl.Spec.ObjectStorage.Bucket
		prefix = bsl.Spec.ObjectStorage.Prefix
	}
	if bucket == "" {
		return nil, fmt.Errorf("BSL %s has no bucket configured", bsl.Name)
	}

	// Add our datamover prefix to the BSL prefix
	if prefix != "" {
		prefix = prefix + "-" + common.DatamoverBSLPrefix
	} else {
		prefix = common.DatamoverBSLPrefix
	}

	region := ""
	if bsl.Spec.Config != nil {
		region = bsl.Spec.Config["region"]
	}

	credName := ""
	credKey := defaultCredentialKey
	if bsl.Spec.Credential != nil {
		credName = bsl.Spec.Credential.Name
		if bsl.Spec.Credential.Key != "" {
			credKey = bsl.Spec.Credential.Key
		}
	}

	return &bslConfig{
		Provider:       bsl.Spec.Provider,
		Bucket:         bucket,
		Prefix:         prefix,
		Region:         region,
		CredentialName: credName,
		CredentialKey:  credKey,
	}, nil
}

// lookupCheckpointFromBSL reads the VM's checkpoint index from the BSL and returns
// the latest valid checkpoint for incremental backup support.
func (r *KubeVirtDataUploadReconciler) lookupCheckpointFromBSL(ctx context.Context, bsl *velerov1.BackupStorageLocation, vmNamespace, vmName string) (*uploader.CheckpointLookupResult, error) {
	store, cfg, err := r.initObjectStoreFromBSL(ctx, bsl)
	if err != nil {
		return nil, err
	}

	// Lookup the latest checkpoint
	result, err := uploader.LookupLatestCheckpoint(ctx, store, cfg.Bucket, vmNamespace, vmName)
	if err != nil {
		return nil, fmt.Errorf("checkpoint lookup failed: %w", err)
	}

	return result, nil
}

// initObjectStoreFromBSL extracts BSL config, fetches credentials, and initializes
// an ObjectStore client. Used by both lookupCheckpointFromBSL and lookupLatestVMBTFromBSL.
func (r *KubeVirtDataUploadReconciler) initObjectStoreFromBSL(
	ctx context.Context, bsl *velerov1.BackupStorageLocation,
) (velero.ObjectStore, *bslConfig, error) {
	cfg, err := extractBSLConfig(bsl)
	if err != nil {
		return nil, nil, err
	}

	if cfg.CredentialName == "" {
		return nil, nil, fmt.Errorf("BSL %s has no credential configured", bsl.Name)
	}

	credData, err := r.getCredentialsFromBSL(ctx, bsl)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get BSL credentials: %w", err)
	}

	factory := r.ObjectStoreFactory
	if factory == nil {
		factory = func(c *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return uploader.InitObjectStore(c)
		}
	}

	store, err := factory(&uploader.UploaderConfig{
		BSLProvider:     cfg.Provider,
		BSLBucket:       cfg.Bucket,
		BSLPrefix:       cfg.Prefix,
		BSLRegion:       cfg.Region,
		CredentialsData: credData,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize object store: %w", err)
	}

	return store, cfg, nil
}

// getCredentialsFromBSL reads the BSL credential secret and returns the raw
// credential bytes. The credentials are kept in memory and never written to disk.
func (r *KubeVirtDataUploadReconciler) getCredentialsFromBSL(
	ctx context.Context, bsl *velerov1.BackupStorageLocation,
) ([]byte, error) {
	if bsl.Spec.Credential == nil {
		return nil, fmt.Errorf("BSL %s has no credential configured", bsl.Name)
	}

	secretName := bsl.Spec.Credential.Name
	secretKey := bsl.Spec.Credential.Key
	if secretKey == "" {
		secretKey = defaultCredentialKey
	}

	// BSL credentials secret is in the same namespace as the BSL
	namespace := bsl.Namespace
	if namespace == "" {
		namespace = r.OADPNamespace
	}

	// Fetch the secret
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name: secretName, Namespace: namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf(
			"failed to get credential secret %s/%s: %w",
			namespace, secretName, err,
		)
	}

	credData, ok := secret.Data[secretKey]
	if !ok {
		return nil, fmt.Errorf(
			"credential secret %s/%s does not contain key %q",
			namespace, secretName, secretKey,
		)
	}

	return credData, nil
}

// findPodForDataUpload finds the unique datamover pod associated with a DataUpload.
func (r *KubeVirtDataUploadReconciler) findPodForDataUpload(ctx context.Context, du *velerov2alpha1.DataUpload, namespace string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		return nil, err
	}
	if len(podList.Items) == 0 {
		return nil, nil
	}
	if len(podList.Items) > 1 {
		return nil, fmt.Errorf("found multiple datamover pods for DataUpload %s", du.Name)
	}
	return &podList.Items[0], nil
}
