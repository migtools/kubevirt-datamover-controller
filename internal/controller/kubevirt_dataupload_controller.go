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

	"github.com/go-logr/logr"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// DataMoverKubeVirt is the datamover value that indicates this controller should handle the DataUpload
	DataMoverKubeVirt = "kubevirt"

	// DefaultMaxConcurrentReconciles is the default number of concurrent reconciles
	DefaultMaxConcurrentReconciles = 3
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
	if dataUpload.Spec.DataMover != DataMoverKubeVirt {
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

	// TODO Phase 2: Validate prerequisites
	// - Check VM annotation exists
	// - Validate VM is running
	// - Check CBT is enabled on VM

	// Transition to Accepted phase
	if err := r.updatePhase(ctx, du, velerov2alpha1.DataUploadPhaseAccepted, "DataUpload accepted by kubevirt datamover"); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

// handleAccepted processes DataUploads in Accepted phase
// Creates VMBT/VMB and transitions to Prepared when ready
func (r *KubeVirtDataUploadReconciler) handleAccepted(ctx context.Context, logger logr.Logger, du *velerov2alpha1.DataUpload) (ctrl.Result, error) {
	logger.Info("Handling Accepted phase DataUpload")

	// TODO Phase 2: VMBT/VMB Creation
	// - Extract VirtualMachine reference from annotation
	// - Query BSL for latest checkpoint
	// - Create/update VirtualMachineBackupTracker
	// - Create VirtualMachineBackup CR
	// - Create temporary PVC for backup output

	// TODO: Transition to Prepared when VMB is ready
	// For now, just log and return
	logger.Info("VMBT/VMB creation not yet implemented")

	return ctrl.Result{}, nil
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

	return ctrl.Result{Requeue: true}, nil
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
		return du.Spec.DataMover == DataMoverKubeVirt
	})
}
