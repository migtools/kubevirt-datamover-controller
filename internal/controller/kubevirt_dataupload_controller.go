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
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
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
}

// +kubebuilder:rbac:groups=velero.io,resources=datauploads,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=velero.io,resources=datauploads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackups/status,verbs=get
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackuptrackers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=backup.kubevirt.io,resources=virtualmachinebackuptrackers/status,verbs=get
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
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

	// Step 1: Create or get temporary PVC for backup output
	pvc, err := r.ensureTempPVC(ctx, logger, du, vmRef.Namespace)
	if err != nil {
		logger.Error(err, "Failed to ensure temporary PVC")
		return ctrl.Result{}, err
	}
	logger.Info("Temporary PVC ready", "pvc", pvc.Name)

	// Step 2: Create or get VirtualMachineBackupTracker
	vmbt, err := r.ensureVMBackupTracker(ctx, logger, du, vmRef.Name, vmRef.Namespace)
	if err != nil {
		logger.Error(err, "Failed to ensure VirtualMachineBackupTracker")
		return ctrl.Result{}, err
	}
	logger.Info("VirtualMachineBackupTracker ready", "vmbt", vmbt.Name)

	// Step 3: Create VirtualMachineBackup if it doesn't exist
	vmb, created, err := r.ensureVMBackup(ctx, logger, du, vmbt, pvc.Name, vmRef.Namespace)
	if err != nil {
		logger.Error(err, "Failed to ensure VirtualMachineBackup")
		return ctrl.Result{}, err
	}

	if created {
		logger.Info("Created VirtualMachineBackup", "vmb", vmb.Name)
		// Requeue to check status
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	// Step 4: Check VMB status
	if vmb.Status == nil {
		logger.Info("VirtualMachineBackup status not yet available, requeuing")
		return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
	}

	// Simple logic: check the Done condition only
	for _, cond := range vmb.Status.Conditions {
		if cond.Type == kubevirtbackupv1alpha1.ConditionDone {
			if cond.Status == corev1.ConditionTrue {
				// Success
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
			// Done: False means explicit failure
			logger.Error(nil, "VirtualMachineBackup failed", "reason", cond.Reason, "message", cond.Message)
			if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseFailed,
				fmt.Sprintf("VMBackup failed: %s", cond.Message)); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// No Done condition yet - still in progress
	logger.Info("VirtualMachineBackup in progress, requeuing")
	return ctrl.Result{RequeueAfter: RequeueAfterShort}, nil
}

// handlePrepared processes DataUploads in Prepared phase
// Launches datamover pod and transitions to InProgress
func (r *KubeVirtDataUploadReconciler) handlePrepared(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Prepared phase DataUpload")

	// TODO Phase 3: Launch datamover pod
	// - Create pod with temp PVC mount and BSL credentials
	// - Pod uploads qcow2 files to object storage

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

	// TODO Phase 3: Monitor datamover pod
	// - Check pod status
	// - On success: update index.json, create manifests, transition to Completed
	// - On failure: transition to Failed

	// TODO Phase 5: Cleanup
	// - Delete temporary PVC
	// - Optionally delete VMB

	logger.Info("Datamover pod monitoring not yet implemented")

	return ctrl.Result{}, nil
}

// handleCanceling processes DataUploads in Canceling phase
// Cleans up resources and transitions to Canceled
func (r *KubeVirtDataUploadReconciler) handleCanceling(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Canceling phase DataUpload")

	// TODO: Cancel in-progress operations
	// - Delete datamover pod if running
	// - Delete VMB if exists
	// - Delete temporary PVC
	// - Transition to Canceled

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

// ensureTempPVC creates or retrieves the temporary PVC for backup output
func (r *KubeVirtDataUploadReconciler) ensureTempPVC(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, namespace string) (*corev1.PersistentVolumeClaim, error) {
	pvcName := fmt.Sprintf("kubevirt-backup-%s", du.Name)

	// Check if PVC already exists
	existingPVC := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: namespace}, existingPVC)
	if err == nil {
		logger.V(1).Info("Temporary PVC already exists", "pvc", pvcName)
		return existingPVC, nil
	}

	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to check for existing PVC: %w", err)
	}

	// Create new PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
			Labels: map[string]string{
				common.LabelDataUploadName: du.Name,
				common.LabelDataUploadUID:  string(du.UID),
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

	// Set owner reference so PVC is cleaned up when DataUpload is deleted
	if err := controllerutil.SetOwnerReference(du, pvc, r.Scheme); err != nil {
		return nil, fmt.Errorf("failed to set owner reference on PVC: %w", err)
	}

	if err := r.Create(ctx, pvc); err != nil {
		return nil, fmt.Errorf("failed to create temporary PVC: %w", err)
	}

	logger.Info("Created temporary PVC", "pvc", pvcName, "namespace", namespace)
	return pvc, nil
}

// ensureVMBackupTracker creates or retrieves the VirtualMachineBackupTracker for the VM
func (r *KubeVirtDataUploadReconciler) ensureVMBackupTracker(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmName, vmNamespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackupTracker, error) {
	// Use a consistent name based on VM name for tracking across backups
	vmbtName := fmt.Sprintf("vmbt-%s", vmName)

	// Check if VMBT already exists
	existingVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	err := r.Get(ctx, types.NamespacedName{Name: vmbtName, Namespace: vmNamespace}, existingVMBT)
	if err == nil {
		logger.V(1).Info("VirtualMachineBackupTracker already exists", "vmbt", vmbtName)
		return existingVMBT, nil
	}

	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to check for existing VMBT: %w", err)
	}

	// Create new VMBT
	apiGroup := "kubevirt.io"
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmbtName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadName: du.Name,
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

	logger.Info("Created VirtualMachineBackupTracker", "vmbt", vmbtName, "namespace", vmNamespace)
	return vmbt, nil
}

// ensureVMBackup creates or retrieves the VirtualMachineBackup for this DataUpload
// Returns the VMB, whether it was created (vs already existed), and any error
func (r *KubeVirtDataUploadReconciler) ensureVMBackup(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload, vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker, pvcName, namespace string) (*kubevirtbackupv1alpha1.VirtualMachineBackup, bool, error) {
	// Use DataUpload name for VMB to ensure 1:1 mapping
	vmbName := fmt.Sprintf("vmb-%s", du.Name)

	// Check if VMB already exists
	existingVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	err := r.Get(ctx, types.NamespacedName{Name: vmbName, Namespace: namespace}, existingVMB)
	if err == nil {
		logger.V(1).Info("VirtualMachineBackup already exists", "vmb", vmbName)
		return existingVMB, false, nil
	}

	if !errors.IsNotFound(err) {
		return nil, false, fmt.Errorf("failed to check for existing VMB: %w", err)
	}

	// Create new VMB referencing the VMBT (enables incremental backups)
	apiGroup := "backup.kubevirt.io"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmbName,
			Namespace: namespace,
			Labels: map[string]string{
				common.LabelDataUploadName: du.Name,
				common.LabelDataUploadUID:  string(du.UID),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VirtualMachineBackupTracker",
				Name:     vmbt.Name,
			},
			PvcName: &pvcName,
		},
	}

	// Set owner reference so VMB is cleaned up when DataUpload is deleted
	if err := controllerutil.SetOwnerReference(du, vmb, r.Scheme); err != nil {
		return nil, false, fmt.Errorf("failed to set owner reference on VMB: %w", err)
	}

	if err := r.Create(ctx, vmb); err != nil {
		return nil, false, fmt.Errorf("failed to create VirtualMachineBackup: %w", err)
	}

	logger.Info("Created VirtualMachineBackup", "vmb", vmbName, "namespace", namespace, "tracker", vmbt.Name)
	return vmb, true, nil
}
