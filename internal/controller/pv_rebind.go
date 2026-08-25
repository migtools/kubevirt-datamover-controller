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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// BindTargetMode selects how rebindPVToNamespace obtains the destination PVC.
type BindTargetMode int

const (
	// BindTargetCreate creates a brand-new, auto-named PVC in the target namespace
	// (the upload path's use case: nothing else references that PVC's name).
	BindTargetCreate BindTargetMode = iota

	// BindTargetExisting binds to a PVC that already exists in the target namespace
	// under an exact, caller-supplied name (the download path's use case: Velero's
	// restore has already created the target PVC placeholder).
	BindTargetExisting
)

var (
	// pvRebindTimeout is the maximum time to wait for PV rebinding operations.
	// Var (not const) so tests can shrink it instead of waiting out a real timeout.
	pvRebindTimeout = 2 * time.Minute

	// pvRebindPollInterval is the interval between polling for PV binding status.
	// Var (not const) so tests can shrink it instead of waiting out a real timeout.
	pvRebindPollInterval = 2 * time.Second
)

const (
	// KubeAnnBoundByController is the annotation added by Kubernetes PV controller
	KubeAnnBoundByController = "pv.kubernetes.io/bound-by-controller"

	// PatchRetryAttempts is the number of times to retry patch operations
	PatchRetryAttempts = 3

	// PatchRetryInterval is the interval between patch retry attempts
	PatchRetryInterval = 1 * time.Second

	// pvOriginalReclaimPolicyAnnotation records a PV's reclaim policy from before
	// Step 2 forced it to Retain, so a later idempotent-resume (Step 1.5, on a
	// retry after the policy has already been changed) can report the true
	// original instead of the current (already-mutated) Retain value.
	pvOriginalReclaimPolicyAnnotation = "kubevirt-datamover.io/original-reclaim-policy"
)

// PVRebindResult contains the result of a PV rebind operation
type PVRebindResult struct {
	// NewPVCName is the name of the new PVC in the target namespace
	NewPVCName string
	// NewPVCNamespace is the namespace of the new PVC
	NewPVCNamespace string
	// PVName is the name of the PV that was rebound
	PVName string
	// OriginalReclaimPolicy is the original reclaim policy (to restore later)
	OriginalReclaimPolicy corev1.PersistentVolumeReclaimPolicy
}

// resolveSourcePV implements rebindPVToNamespace's Step 1: get the source PVC
// and its bound PV. Tolerates two crash-recovery states instead of failing
// outright:
//   - Source PVC still present but ClaimLost (not just ClaimBound): a prior
//     invocation completed Step 5 (PV claimRef repointed to the destination)
//     but crashed before Step 3 deleted this PVC. Step 1.5's BindTargetExisting
//     idempotent-short-circuit is what actually detects and finishes this case;
//     this function only needs to not fail before reaching it.
//   - Source PVC genuinely missing (#153): recovers via a UID-label PV search
//     instead -- this could be a prior invocation that crashed after Step 3
//     deleted the source PVC but before Step 5 completed the claimRef patch,
//     and the label is unique to this resource's own rebind operation, so the
//     PV is findable this way regardless of exactly how far that prior
//     invocation got. Returns sourcePVCAlreadyGone true in that recovered
//     case, telling the caller to skip Step 1.5's leftover-cleanup delete and
//     Step 3's delete-and-wait entirely (the returned sourcePVC is a
//     zero-value placeholder then, not a real object).
func resolveSourcePV(ctx context.Context, k8sClient client.Client, logger logr.Logger, sourcePVCName, sourceNamespace, uidLabelKey, resourceUID string) (pv *corev1.PersistentVolume, pvName string, sourcePVC *corev1.PersistentVolumeClaim, sourcePVCAlreadyGone bool, err error) {
	sourcePVC = &corev1.PersistentVolumeClaim{}
	getErr := k8sClient.Get(ctx, types.NamespacedName{Name: sourcePVCName, Namespace: sourceNamespace}, sourcePVC)

	switch {
	case getErr == nil:
		// ClaimLost (not just ClaimBound) is also acceptable here: it's exactly
		// the state a source PVC ends up in if a prior invocation completed
		// Step 5 (PV claimRef repointed to the destination) but crashed before
		// Step 3 deleted this PVC -- the PV controller marks an orphaned PVC
		// Lost once its bound PV's claimRef no longer names it. Spec.VolumeName
		// is still trustworthy in that state, and Step 1.5's BindTargetExisting
		// idempotent-short-circuit is exactly what detects and finishes that
		// case; failing outright here would prevent ever reaching it.
		if sourcePVC.Status.Phase != corev1.ClaimBound && sourcePVC.Status.Phase != corev1.ClaimLost {
			return nil, "", nil, false, fmt.Errorf("source PVC %s/%s is not bound (phase: %s)", sourceNamespace, sourcePVCName, sourcePVC.Status.Phase)
		}
		pvName = sourcePVC.Spec.VolumeName
		if pvName == "" {
			return nil, "", nil, false, fmt.Errorf("source PVC %s/%s has no volume name (phase: %s)", sourceNamespace, sourcePVCName, sourcePVC.Status.Phase)
		}
		pv = &corev1.PersistentVolume{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvName}, pv); err != nil {
			return nil, "", nil, false, fmt.Errorf("failed to get PV %s: %w", pvName, err)
		}
		logger.Info("Found PV bound to source PVC", "pv", pvName, "sourcePVC", sourcePVCName, "sourcePVCPhase", sourcePVC.Status.Phase)
		return pv, pvName, sourcePVC, false, nil

	case errors.IsNotFound(getErr):
		found, err := findPVByUIDLabel(ctx, k8sClient, uidLabelKey, resourceUID)
		if err != nil {
			return nil, "", nil, false, fmt.Errorf("failed to search for PV by label after source PVC %s/%s was not found: %w", sourceNamespace, sourcePVCName, err)
		}
		if found == nil {
			return nil, "", nil, false, fmt.Errorf("failed to get source PVC %s/%s: %w", sourceNamespace, sourcePVCName, getErr)
		}
		logger.Info("Source PVC not found but a PV carrying this resource's UID label was -- resuming a prior invocation that crashed after deleting the source PVC",
			"pv", found.Name)
		return found, found.Name, sourcePVC, true, nil

	default:
		return nil, "", nil, false, fmt.Errorf("failed to get source PVC %s/%s: %w", sourceNamespace, sourcePVCName, getErr)
	}
}

// rebindPVToNamespace rebinds a PV from a PVC in the source namespace to a PVC in the target namespace.
// This follows the same pattern as Velero's generic restore exposer, using Patch operations
// to avoid conflicts with Kubernetes PV controller.
//
// Steps:
//  1. Get the PV bound to the source PVC
//  2. Set PV reclaim policy to Retain (using Patch)
//  3. Delete the source PVC (PV stays due to Retain)
//  4. Obtain the destination PVC in the target namespace: either create a new,
//     auto-named one (bindMode == BindTargetCreate) or bind to an existing PVC by
//     exact name (bindMode == BindTargetExisting, existingPVCName required)
//  5. Reset PV binding: set claimRef to destination PVC, add labels (using Patch)
//  6. Wait for PV to bind to the destination PVC
//
// This function is intentionally long: it is a linear multi-step sequence
// with crash-recovery branches at nearly every step, not deeply nested
// conditional logic -- splitting it further would scatter the step-by-step
// narrative this function's own numbered comment (and its callers) depend
// on.
//
//nolint:gocyclo // Linear multi-step sequence with crash-recovery branches at nearly every step.
func rebindPVToNamespace(
	ctx context.Context,
	k8sClient client.Client,
	logger logr.Logger,
	sourcePVCName string,
	sourceNamespace string,
	targetNamespace string,
	resourceName string,
	resourceUID string,
	uidLabelKey string,
	nameAnnotationKey string,
	bindMode BindTargetMode,
	existingPVCName string,
) (*PVRebindResult, error) {
	if bindMode == BindTargetExisting && existingPVCName == "" {
		return nil, fmt.Errorf("bindMode BindTargetExisting requires a non-empty existingPVCName")
	}

	// Step 1: Get the source PVC and its bound PV
	pv, pvName, sourcePVC, sourcePVCAlreadyGone, err := resolveSourcePV(ctx, k8sClient, logger, sourcePVCName, sourceNamespace, uidLabelKey, resourceUID)
	if err != nil {
		return nil, err
	}

	// Step 1.5 (BindTargetExisting only): validate destination compatibility BEFORE
	// mutating the source (Steps 2-3), so an incompatible pairing fails fast without
	// tearing down the source PVC/reclaim policy. This also makes the rebind
	// idempotent: if the destination is already bound to this exact PV, a prior
	// invocation already completed it and there's nothing left to do.
	var targetPVC *corev1.PersistentVolumeClaim
	if bindMode == BindTargetExisting {
		var err error
		targetPVC, err = validateExistingPVCForBind(ctx, k8sClient, logger, pv, targetNamespace, existingPVCName)
		if err != nil {
			return nil, err
		}
		if targetPVC.Status.Phase == corev1.ClaimBound {
			// validateExistingPVCForBind already confirmed the PVC's own Spec.VolumeName names
			// this PV when Bound; also check the PV's claimRef names this PVC back --
			// if it doesn't, this isn't actually the idempotent-complete case (an
			// inconsistent bind state that shouldn't occur in a real cluster, since
			// the PV controller only sets a PVC Bound once both sides agree, but
			// worth guarding against rather than trusting blindly). Fall through to
			// the normal Steps 2-6 instead of short-circuiting, so the claimRef gets
			// (re-)patched properly rather than misreporting success.
			claimRefIsTarget := pv.Spec.ClaimRef != nil &&
				pv.Spec.ClaimRef.Namespace == targetNamespace &&
				pv.Spec.ClaimRef.Name == targetPVC.Name
			if claimRefIsTarget {
				logger.Info("Target PVC already bound to this PV, rebind already complete",
					"pvc", targetPVC.Name, "namespace", targetNamespace, "pv", pvName)
				// A prior invocation's patchPVBinding sets the claimRef and the UID
				// label in the same merge patch, so under normal circumstances the
				// label is already present here too -- but isRestoreAlreadyProvisioned
				// and the Cancel/timeout-vs-provisioned guards all find this PV by that
				// label, so self-heal it if it's ever missing rather than assuming.
				if pv.Labels[uidLabelKey] != resourceUID {
					if err := patchPVBinding(ctx, k8sClient, pv, targetPVC, map[string]string{uidLabelKey: resourceUID}); err != nil {
						return nil, fmt.Errorf("failed to restore missing UID label on already-bound PV %s: %w", pvName, err)
					}
				}
				// The PV's claimRef already points at the destination, so the source PVC
				// (found still present in Step 1, meaning a prior invocation's Step 3
				// delete either never ran or didn't finish) is now just leftover cruft --
				// safe to delete since the PV itself is no longer bound to it. Skipped
				// entirely when sourcePVCAlreadyGone: sourcePVC is a zero-value
				// placeholder in that case (Step 1's Get returned NotFound), not a real
				// object to delete.
				if !sourcePVCAlreadyGone {
					if err := k8sClient.Delete(ctx, sourcePVC); err != nil && !errors.IsNotFound(err) {
						return nil, fmt.Errorf("failed to delete leftover source PVC %s/%s: %w", sourceNamespace, sourcePVCName, err)
					}
					if err := waitForPVCDeletion(ctx, k8sClient, sourcePVCName, sourceNamespace); err != nil {
						return nil, fmt.Errorf("failed waiting for leftover source PVC %s/%s deletion: %w", sourceNamespace, sourcePVCName, err)
					}
				}
				return &PVRebindResult{
					NewPVCName:            targetPVC.Name,
					NewPVCNamespace:       targetNamespace,
					PVName:                pvName,
					OriginalReclaimPolicy: originalReclaimPolicyOf(pv),
				}, nil
			}
			logger.Info("Target PVC Bound to this PV's name but PV's claimRef doesn't name it back, proceeding with normal rebind instead of short-circuiting",
				"pvc", targetPVC.Name, "namespace", targetNamespace, "pv", pvName)
		}
	}

	// Step 2: Set PV reclaim policy to Retain using Patch
	originalReclaimPolicy := originalReclaimPolicyOf(pv)
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		if err := patchPVOriginalReclaimPolicyAnnotation(ctx, k8sClient, pv, originalReclaimPolicy); err != nil {
			return nil, fmt.Errorf("failed to record PV %s original reclaim policy: %w", pvName, err)
		}
		if err := patchPVReclaimPolicy(ctx, k8sClient, pv, corev1.PersistentVolumeReclaimRetain); err != nil {
			return nil, fmt.Errorf("failed to set PV %s reclaim policy to Retain: %w", pvName, err)
		}
		logger.Info("Set PV reclaim policy to Retain", "pv", pvName, "originalPolicy", originalReclaimPolicy)
	}

	// Stamp the UID label now, before Step 3 deletes the source PVC: findPVByUIDLabel's
	// crash-recovery path (used by resolveSourcePV and the Cancel/timeout-vs-provisioned
	// guards) can only find this PV by that label, so it must already be present for
	// any crash from this point on to be recoverable -- previously the label was only
	// set in Step 5, leaving the Step 3-to-5 window unrecoverable if a crash landed
	// there (source PVC gone, PV unlabeled, nothing to resolve it by).
	if pv.Labels[uidLabelKey] != resourceUID {
		if err := patchPVLabels(ctx, k8sClient, pv, map[string]string{uidLabelKey: resourceUID}); err != nil {
			return nil, fmt.Errorf("failed to stamp UID label on PV %s: %w", pvName, err)
		}
		if pv.Labels == nil {
			pv.Labels = make(map[string]string)
		}
		pv.Labels[uidLabelKey] = resourceUID
	}

	// Step 3: Delete the source PVC -- skipped entirely when sourcePVCAlreadyGone
	// (Step 1 already confirmed it's gone via NotFound; sourcePVC is a zero-value
	// placeholder, not a real object, and there's nothing left to wait out).
	if !sourcePVCAlreadyGone {
		if err := k8sClient.Delete(ctx, sourcePVC); err != nil && !errors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to delete source PVC %s/%s: %w", sourceNamespace, sourcePVCName, err)
		}
		logger.Info("Deleted source PVC", "pvc", sourcePVCName, "namespace", sourceNamespace)

		if err := waitForPVCDeletion(ctx, k8sClient, sourcePVCName, sourceNamespace); err != nil {
			return nil, fmt.Errorf("failed waiting for source PVC deletion: %w", err)
		}
	}

	// Step 4: Obtain the destination PVC in the target namespace. BindTargetExisting
	// already resolved and validated targetPVC in Step 1.5 above.
	if bindMode != BindTargetExisting {
		var err error
		specSource := sourcePVC
		if sourcePVCAlreadyGone {
			// sourcePVC is the zero-value placeholder resolveSourcePV returned in
			// this case (there's no real source PVC object left to read), so
			// createNewBoundPVC's AccessModes/Resources/StorageClassName/VolumeMode
			// copy would otherwise silently produce a PVC missing all of them.
			// The recovered PV carries equivalent values -- derive from it instead.
			specSource = pvcSpecFromPV(pv)
		}
		targetPVC, err = createNewBoundPVC(ctx, k8sClient, logger, specSource, targetNamespace, resourceName, resourceUID, uidLabelKey, nameAnnotationKey, pvName)
		if err != nil {
			return nil, err
		}
	}

	// Step 5: Reset PV binding using Patch (like Velero's ResetPVBinding)
	// Re-fetch PV to get latest version
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvName}, pv); err != nil {
		return nil, fmt.Errorf("failed to re-fetch PV %s: %w", pvName, err)
	}
	// Re-validate the destination PVC too (BindTargetExisting only --
	// createNewBoundPVC's return is already fresh): Steps 2-3 above (Retain patch,
	// source-PVC delete + wait) can take real wall-clock time, during which the
	// destination could have changed in a way Step 1.5's earlier validation can no
	// longer vouch for (e.g. started being deleted, or a conflicting bind appeared).
	// Re-running the full validateExistingPVCForBind check (not just a raw re-Get) catches that,
	// and its refreshed object also carries the current UID the claimRef needs.
	if bindMode == BindTargetExisting {
		refreshed, err := validateExistingPVCForBind(ctx, k8sClient, logger, pv, targetNamespace, targetPVC.Name)
		if err != nil {
			// Unrecoverable at this point: the source PVC is already gone (deleted
			// in Step 3 above), and PV pvName is left Retain'd with a now-stale
			// claimRef -- an operator needs to manually inspect and either fix the
			// destination PVC or reclaim the PV directly.
			return nil, fmt.Errorf("target PVC %s/%s no longer eligible for binding after source PVC %s/%s was already deleted; PV %s is left Retain'd with a stale claimRef and needs manual recovery: %w",
				targetNamespace, targetPVC.Name, sourceNamespace, sourcePVCName, pvName, err)
		}
		targetPVC = refreshed
	}

	labels := map[string]string{uidLabelKey: resourceUID}
	if err := patchPVBinding(ctx, k8sClient, pv, targetPVC, labels); err != nil {
		return nil, fmt.Errorf("failed to reset PV %s binding: %w", pvName, err)
	}
	logger.Info("Reset PV binding to target PVC", "pv", pvName, "targetPVC", targetPVC.Name, "namespace", targetNamespace)

	// Step 6: Wait for PV to bind to the destination PVC
	if err := waitForPVCBound(ctx, k8sClient, targetPVC.Name, targetNamespace, pvName); err != nil {
		return nil, fmt.Errorf("failed waiting for target PVC to bind: %w", err)
	}
	logger.Info("Target PVC is bound to PV", "pvc", targetPVC.Name, "pv", pvName)

	return &PVRebindResult{
		NewPVCName:            targetPVC.Name,
		NewPVCNamespace:       targetNamespace,
		PVName:                pvName,
		OriginalReclaimPolicy: originalReclaimPolicy,
	}, nil
}

// pvcSpecFromPV builds a minimal PersistentVolumeClaim carrying just the
// fields createNewBoundPVC reads (AccessModes, Resources, StorageClassName,
// VolumeMode), derived directly from a PV. Used by rebindPVToNamespace's
// Step 4 (BindTargetCreate path) when resolveSourcePV recovered via a
// UID-label search instead of the real source PVC (sourcePVCAlreadyGone) --
// that recovery path has no source PVC object left to copy these fields
// from, only the PV itself, which carries equivalent values.
func pvcSpecFromPV(pv *corev1.PersistentVolume) *corev1.PersistentVolumeClaim {
	requests := corev1.ResourceList{}
	if capacity, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok {
		requests[corev1.ResourceStorage] = capacity
	}
	var storageClassName *string
	if pv.Spec.StorageClassName != "" {
		sc := pv.Spec.StorageClassName
		storageClassName = &sc
	}
	return &corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      pv.Spec.AccessModes,
			Resources:        corev1.VolumeResourceRequirements{Requests: requests},
			StorageClassName: storageClassName,
			VolumeMode:       pv.Spec.VolumeMode,
		},
	}
}

// createNewBoundPVC creates a brand-new, auto-named PVC in the target namespace,
// statically bound to pvName via VolumeName + a label selector. Used by the upload
// path (BindTargetCreate), where nothing else references the destination PVC's name.
func createNewBoundPVC(
	ctx context.Context,
	k8sClient client.Client,
	logger logr.Logger,
	sourcePVC *corev1.PersistentVolumeClaim,
	targetNamespace string,
	resourceName string,
	resourceUID string,
	uidLabelKey string,
	nameAnnotationKey string,
	pvName string,
) (*corev1.PersistentVolumeClaim, error) {
	newPVCName := common.SafeResourceName(common.ReboundPVCNamePrefix, resourceName)
	newPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      newPVCName,
			Namespace: targetNamespace,
			Labels: map[string]string{
				uidLabelKey: resourceUID,
			},
			Annotations: map[string]string{
				nameAnnotationKey: resourceName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: sourcePVC.Spec.AccessModes,
			Resources:   sourcePVC.Spec.Resources,
			// Direct binding via volumeName
			VolumeName: pvName,
			// Label selector binding
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					uidLabelKey: resourceUID,
				},
			},
			StorageClassName: sourcePVC.Spec.StorageClassName,
			VolumeMode:       sourcePVC.Spec.VolumeMode,
		},
	}

	if err := k8sClient.Create(ctx, newPVC); err != nil {
		if !errors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create new PVC %s/%s: %w", targetNamespace, newPVCName, err)
		}
		// Create leaves newPVC's ObjectMeta untouched on failure (no server-assigned
		// UID), but the caller patches the PV's claimRef.UID from this object -- an
		// empty UID there would silently drop the UID-based binding safety check for
		// whatever concurrent attempt already created this PVC. Re-fetch the real
		// object instead of returning the local, UID-less one.
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: newPVCName, Namespace: targetNamespace}, newPVC); err != nil {
			return nil, fmt.Errorf("failed to get already-existing PVC %s/%s: %w", targetNamespace, newPVCName, err)
		}
		logger.Info("Rebound PVC already exists", "pvc", newPVCName, "namespace", targetNamespace)
	} else {
		logger.Info("Created new PVC in target namespace", "pvc", newPVCName, "namespace", targetNamespace)
	}

	return newPVC, nil
}

// validateExistingPVCForBind validates that an already-existing target PVC (created by an
// external actor, e.g. Velero's restore) can be bound to the given PV, and returns the live
// PVC. patchPVBinding (called later in rebindPVToNamespace) commits the actual claimRef bind;
// the only write this function performs itself is the one described below for a selector-
// bearing PVC. Used by the download path (BindTargetExisting), where the destination PVC's
// exact name is dictated by DataDownload.Spec.TargetVolume.PVC.
//
// Storage compatibility (StorageClassName, requested capacity, VolumeMode, AccessModes) is
// validated up front so an incompatible pairing fails fast with a specific error, instead of
// silently wedging in Pending until waitForPVCBound's timeout with no useful diagnostic.
//
// A matchLabels-only Spec.Selector is treated as a request to reconcile, not a conflict: this
// is the same technique Velero's own built-in CSI DataDownload restore path uses (see
// velero.io/dynamic-pv-restore) to stop the dynamic provisioner from racing this rebind --
// setting Selector on a PVC makes provisioners skip it outright, independent of the target
// StorageClass's volumeBindingMode. Since our PV is one this controller fully owns, its
// labels are patched to satisfy the selector rather than requiring the caller (the restore
// plugin) to coordinate a label value with us in advance. matchExpressions is rejected: there's
// no general way to synthesize a label set satisfying an arbitrary expression.
func validateExistingPVCForBind(
	ctx context.Context,
	k8sClient client.Client,
	logger logr.Logger,
	pv *corev1.PersistentVolume,
	targetNamespace string,
	existingPVCName string,
) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: existingPVCName, Namespace: targetNamespace}, pvc); err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("target PVC %s/%s not found: %w", targetNamespace, existingPVCName, err)
		}
		return nil, fmt.Errorf("failed to get target PVC %s/%s: %w", targetNamespace, existingPVCName, err)
	}

	if pvc.DeletionTimestamp != nil {
		return nil, fmt.Errorf("target PVC %s/%s is being deleted", targetNamespace, existingPVCName)
	}

	if pvc.Status.Phase == corev1.ClaimBound {
		if pvc.Spec.VolumeName != pv.Name {
			return nil, fmt.Errorf("target PVC %s/%s is already bound to PV %q, expected %q",
				targetNamespace, existingPVCName, pvc.Spec.VolumeName, pv.Name)
		}
		logger.V(1).Info("Target PVC already bound to this PV", "pvc", existingPVCName, "namespace", targetNamespace, "pv", pv.Name)
		return pvc, nil
	}

	// Not yet Bound, but Spec.VolumeName may already be set awaiting the binder
	// (e.g. a retry, or a pre-set static-binding request) -- if it names a
	// different PV, patching our PV's claimRef onto this PVC would conflict with
	// that expectation instead of completing it.
	if pvc.Spec.VolumeName != "" && pvc.Spec.VolumeName != pv.Name {
		return nil, fmt.Errorf("target PVC %s/%s already requests volume %q, expected %q",
			targetNamespace, existingPVCName, pvc.Spec.VolumeName, pv.Name)
	}

	pvcStorageClass := ""
	if pvc.Spec.StorageClassName != nil {
		pvcStorageClass = *pvc.Spec.StorageClassName
	}
	if pvcStorageClass != pv.Spec.StorageClassName {
		return nil, fmt.Errorf("target PVC %s/%s storageClassName %q does not match PV %s storageClassName %q",
			targetNamespace, existingPVCName, pvcStorageClass, pv.Name, pv.Spec.StorageClassName)
	}

	if requested, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		capacity, hasCapacity := pv.Spec.Capacity[corev1.ResourceStorage]
		if !hasCapacity {
			return nil, fmt.Errorf("PV %s has no storage capacity recorded, cannot validate target PVC %s/%s request of %s",
				pv.Name, targetNamespace, existingPVCName, requested.String())
		}
		if requested.Cmp(capacity) > 0 {
			return nil, fmt.Errorf("target PVC %s/%s requests %s storage which exceeds PV %s capacity %s",
				targetNamespace, existingPVCName, requested.String(), pv.Name, capacity.String())
		}
	}

	pvcVolumeMode := corev1.PersistentVolumeFilesystem
	if pvc.Spec.VolumeMode != nil {
		pvcVolumeMode = *pvc.Spec.VolumeMode
	}
	pvVolumeMode := corev1.PersistentVolumeFilesystem
	if pv.Spec.VolumeMode != nil {
		pvVolumeMode = *pv.Spec.VolumeMode
	}
	if pvcVolumeMode != pvVolumeMode {
		return nil, fmt.Errorf("target PVC %s/%s volumeMode %s does not match PV %s volumeMode %s",
			targetNamespace, existingPVCName, pvcVolumeMode, pv.Name, pvVolumeMode)
	}

	// Every access mode the PVC requests must be supported by the PV -- a partial
	// (any-one) match isn't sufficient, since the PVC's consumer expects all of them.
	for _, am := range pvc.Spec.AccessModes {
		if !slices.Contains(pv.Spec.AccessModes, am) {
			return nil, fmt.Errorf("target PVC %s/%s requires access mode %q which PV %s does not support (has %v)",
				targetNamespace, existingPVCName, am, pv.Name, pv.Spec.AccessModes)
		}
	}

	if pvc.Spec.Selector != nil {
		if len(pvc.Spec.Selector.MatchExpressions) > 0 {
			return nil, fmt.Errorf("target PVC %s/%s has an unsupported label selector (matchExpressions is not supported for restore rebinding, only matchLabels): %v",
				targetNamespace, existingPVCName, pvc.Spec.Selector.MatchExpressions)
		}
		var toPatch map[string]string
		for k, v := range pvc.Spec.Selector.MatchLabels {
			if pv.Labels[k] != v {
				if toPatch == nil {
					toPatch = make(map[string]string, len(pvc.Spec.Selector.MatchLabels))
				}
				toPatch[k] = v
			}
		}
		if toPatch != nil {
			if err := patchPVLabels(ctx, k8sClient, pv, toPatch); err != nil {
				return nil, fmt.Errorf("failed to apply target PVC %s/%s selector labels to PV %s: %w",
					targetNamespace, existingPVCName, pv.Name, err)
			}
			if pv.Labels == nil {
				pv.Labels = make(map[string]string, len(toPatch))
			}
			maps.Copy(pv.Labels, toPatch)
		}
	}

	logger.Info("Target PVC is compatible with PV, will bind", "pvc", existingPVCName, "namespace", targetNamespace, "pv", pv.Name)
	return pvc, nil
}

// originalReclaimPolicyOf returns the PV's true original reclaim policy: the
// value recorded in pvOriginalReclaimPolicyAnnotation if Step 2 has already
// forced it to Retain in a prior invocation, otherwise the PV's current spec
// value (which, if this is the first time touching this PV, IS the original).
func originalReclaimPolicyOf(pv *corev1.PersistentVolume) corev1.PersistentVolumeReclaimPolicy {
	// The annotation is only meaningful once Step 2 has already overwritten the
	// spec with Retain; if the spec isn't Retain, it IS the original, and any
	// leftover annotation (e.g. from a previous cycle on a reused PV) is stale.
	if pv.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimRetain {
		if recorded, ok := pv.Annotations[pvOriginalReclaimPolicyAnnotation]; ok && recorded != "" {
			return corev1.PersistentVolumeReclaimPolicy(recorded)
		}
	}
	return pv.Spec.PersistentVolumeReclaimPolicy
}

// patchPVOriginalReclaimPolicyAnnotation records policy as the PV's original
// reclaim policy, so originalReclaimPolicyOf can recover it on a later
// idempotent-resume after Step 2 has already overwritten the live spec value
// with Retain. Includes retry logic for transient errors, matching
// patchPVReclaimPolicy.
func patchPVOriginalReclaimPolicyAnnotation(ctx context.Context, k8sClient client.Client, pv *corev1.PersistentVolume, policy corev1.PersistentVolumeReclaimPolicy) error {
	var lastErr error
	for attempt := 1; attempt <= PatchRetryAttempts; attempt++ {
		err := doPatchPVOriginalReclaimPolicyAnnotation(ctx, k8sClient, pv, policy)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < PatchRetryAttempts {
			time.Sleep(PatchRetryInterval)
			if fetchErr := k8sClient.Get(ctx, types.NamespacedName{Name: pv.Name}, pv); fetchErr != nil {
				return fmt.Errorf("failed to re-fetch PV after patch error: %w", fetchErr)
			}
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", PatchRetryAttempts, lastErr)
}

func doPatchPVOriginalReclaimPolicyAnnotation(ctx context.Context, k8sClient client.Client, pv *corev1.PersistentVolume, policy corev1.PersistentVolumeReclaimPolicy) error {
	origBytes, err := json.Marshal(pv)
	if err != nil {
		return fmt.Errorf("error marshaling original PV: %w", err)
	}

	updated := pv.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Annotations[pvOriginalReclaimPolicyAnnotation] = string(policy)

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("error marshaling updated PV: %w", err)
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return fmt.Errorf("error creating merge patch for PV: %w", err)
	}

	return k8sClient.Patch(ctx, pv, client.RawPatch(types.MergePatchType, patchBytes))
}

// patchPVLabels merges extra into the PV's existing labels (like
// patchPVOriginalReclaimPolicyAnnotation, but for labels rather than an
// annotation). Includes retry logic for transient errors, matching
// patchPVReclaimPolicy.
func patchPVLabels(ctx context.Context, k8sClient client.Client, pv *corev1.PersistentVolume, extra map[string]string) error {
	var lastErr error
	for attempt := 1; attempt <= PatchRetryAttempts; attempt++ {
		err := doPatchPVLabels(ctx, k8sClient, pv, extra)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < PatchRetryAttempts {
			time.Sleep(PatchRetryInterval)
			if fetchErr := k8sClient.Get(ctx, types.NamespacedName{Name: pv.Name}, pv); fetchErr != nil {
				return fmt.Errorf("failed to re-fetch PV after patch error: %w", fetchErr)
			}
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", PatchRetryAttempts, lastErr)
}

func doPatchPVLabels(ctx context.Context, k8sClient client.Client, pv *corev1.PersistentVolume, extra map[string]string) error {
	origBytes, err := json.Marshal(pv)
	if err != nil {
		return fmt.Errorf("error marshaling original PV: %w", err)
	}

	updated := pv.DeepCopy()
	if updated.Labels == nil {
		updated.Labels = make(map[string]string)
	}
	maps.Copy(updated.Labels, extra)

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("error marshaling updated PV: %w", err)
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return fmt.Errorf("error creating merge patch for PV: %w", err)
	}

	return k8sClient.Patch(ctx, pv, client.RawPatch(types.MergePatchType, patchBytes))
}

// patchPVReclaimPolicy patches a PV to set its reclaim policy (like Velero's SetPVReclaimPolicy).
// Includes retry logic for transient errors.
func patchPVReclaimPolicy(ctx context.Context, k8sClient client.Client, pv *corev1.PersistentVolume, policy corev1.PersistentVolumeReclaimPolicy) error {
	var lastErr error
	for attempt := 1; attempt <= PatchRetryAttempts; attempt++ {
		err := doPatchPVReclaimPolicy(ctx, k8sClient, pv, policy)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < PatchRetryAttempts {
			time.Sleep(PatchRetryInterval)
			if fetchErr := k8sClient.Get(ctx, types.NamespacedName{Name: pv.Name}, pv); fetchErr != nil {
				return fmt.Errorf("failed to re-fetch PV after patch error: %w", fetchErr)
			}
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", PatchRetryAttempts, lastErr)
}

func doPatchPVReclaimPolicy(ctx context.Context, k8sClient client.Client, pv *corev1.PersistentVolume, policy corev1.PersistentVolumeReclaimPolicy) error {
	origBytes, err := json.Marshal(pv)
	if err != nil {
		return fmt.Errorf("error marshaling original PV: %w", err)
	}

	updated := pv.DeepCopy()
	updated.Spec.PersistentVolumeReclaimPolicy = policy

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("error marshaling updated PV: %w", err)
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return fmt.Errorf("error creating merge patch for PV: %w", err)
	}

	return k8sClient.Patch(ctx, pv, client.RawPatch(types.MergePatchType, patchBytes))
}

// patchPVBinding patches a PV to reset its binding info (like Velero's ResetPVBinding).
// Includes retry logic for transient errors.
func patchPVBinding(ctx context.Context, k8sClient client.Client, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim, labels map[string]string) error {
	var lastErr error
	for attempt := 1; attempt <= PatchRetryAttempts; attempt++ {
		err := doPatchPVBinding(ctx, k8sClient, pv, pvc, labels)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < PatchRetryAttempts {
			time.Sleep(PatchRetryInterval)
			if fetchErr := k8sClient.Get(ctx, types.NamespacedName{Name: pv.Name}, pv); fetchErr != nil {
				return fmt.Errorf("failed to re-fetch PV after patch error: %w", fetchErr)
			}
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", PatchRetryAttempts, lastErr)
}

func doPatchPVBinding(ctx context.Context, k8sClient client.Client, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim, labels map[string]string) error {
	origBytes, err := json.Marshal(pv)
	if err != nil {
		return fmt.Errorf("error marshaling original PV: %w", err)
	}

	updated := pv.DeepCopy()
	updated.Spec.ClaimRef = &corev1.ObjectReference{
		Kind:      "PersistentVolumeClaim",
		Namespace: pvc.Namespace,
		Name:      pvc.Name,
		UID:       pvc.UID,
	}
	if updated.Annotations != nil {
		delete(updated.Annotations, KubeAnnBoundByController)
	}
	if labels != nil {
		if updated.Labels == nil {
			updated.Labels = make(map[string]string)
		}
		maps.Copy(updated.Labels, labels)
	}

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("error marshaling updated PV: %w", err)
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return fmt.Errorf("error creating merge patch for PV: %w", err)
	}

	return k8sClient.Patch(ctx, pv, client.RawPatch(types.MergePatchType, patchBytes))
}

// waitForPVCDeletion waits for a PVC to be fully deleted.
func waitForPVCDeletion(ctx context.Context, k8sClient client.Client, pvcName, namespace string) error {
	return wait.PollUntilContextTimeout(ctx, pvRebindPollInterval, pvRebindTimeout, true, func(ctx context.Context) (bool, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: namespace}, pvc)
		if errors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}

// waitForPVCBound waits for a PVC to be bound to the given PV. Phase == Bound
// alone isn't proof the rebind succeeded: in BindTargetExisting mode the target
// PVC's Spec.VolumeName is never pre-set, so between the claimRef patch and the
// PV controller completing the bind, a racing dynamic provisioner can bind the
// PVC to a freshly-provisioned (empty) PV instead -- e.g. when the restored VM
// is started mid-restore and its virt-launcher pod triggers WaitForFirstConsumer
// provisioning on the target PVC. Verify the PVC actually bound to the expected
// PV, and fail immediately on a foreign VolumeName (it's immutable once set, so
// the wrong bind can never resolve itself) rather than misreporting success or
// polling to timeout.
func waitForPVCBound(ctx context.Context, k8sClient client.Client, pvcName, namespace, pvName string) error {
	return wait.PollUntilContextTimeout(ctx, pvRebindPollInterval, pvRebindTimeout, true, func(ctx context.Context) (bool, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: namespace}, pvc); err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if pvc.Spec.VolumeName != "" && pvc.Spec.VolumeName != pvName {
			return false, fmt.Errorf("PVC %s/%s bound to unexpected volume %q, want %q (a racing provisioner may have claimed it)",
				namespace, pvcName, pvc.Spec.VolumeName, pvName)
		}
		return pvc.Status.Phase == corev1.ClaimBound, nil
	})
}

// findPVByUIDLabel finds a PV carrying the given UID label -- used both to
// recover a rebind operation's PV when the source PVC that would normally
// resolve it is gone (#153), and to locate a rebound PV for cleanup once its
// PVC is already deleted. Step 2 stamps the label (alongside the Retain
// patch) before Step 3 deletes the source PVC, so a PV carrying this exact
// resourceUID's label is reliably findable this way regardless of how far a
// prior, interrupted invocation got. Returns (nil, nil)
// if no PV carries the label -- not an error, since callers use this as a
// fallback path where "none found" is a valid, meaningful outcome.
func findPVByUIDLabel(ctx context.Context, k8sClient client.Client, uidLabelKey, resourceUID string) (*corev1.PersistentVolume, error) {
	pvList := &corev1.PersistentVolumeList{}
	if err := k8sClient.List(ctx, pvList, client.MatchingLabels{uidLabelKey: resourceUID}); err != nil {
		return nil, fmt.Errorf("failed to list PVs by label: %w", err)
	}
	if len(pvList.Items) == 0 {
		return nil, nil
	}
	if len(pvList.Items) > 1 {
		names := make([]string, 0, len(pvList.Items))
		for i := range pvList.Items {
			names = append(names, pvList.Items[i].Name)
		}
		// Picking arbitrarily here risks rebinding/cleaning up the wrong PV --
		// fail instead so this gets investigated rather than silently guessing.
		return nil, fmt.Errorf("found %d PVs carrying label %s=%s (%v); expected exactly one, refusing to pick arbitrarily",
			len(pvList.Items), uidLabelKey, resourceUID, names)
	}
	return &pvList.Items[0], nil
}

// cleanupReboundPVCAndPV deletes the rebound PVC and PV after a datamover operation completes.
// The resourceUID is used to find the PV by label if the PVC is already gone,
// preventing storage leakage.
func cleanupReboundPVCAndPV(
	ctx context.Context,
	k8sClient client.Client,
	logger logr.Logger,
	pvcName string,
	pvcNamespace string,
	resourceUID string,
	uidLabelKey string,
) error {
	var pvName string

	pvc := &corev1.PersistentVolumeClaim{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: pvcNamespace}, pvc)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.V(1).Info("Rebound PVC already deleted, will find PV by label", "pvc", pvcName)
		} else {
			return fmt.Errorf("failed to get rebound PVC %s/%s: %w", pvcNamespace, pvcName, err)
		}
	} else {
		pvName = pvc.Spec.VolumeName

		if err := k8sClient.Delete(ctx, pvc); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete rebound PVC %s/%s: %w", pvcNamespace, pvcName, err)
		}
		logger.Info("Deleted rebound PVC", "pvc", pvcName, "namespace", pvcNamespace)

		if err := waitForPVCDeletion(ctx, k8sClient, pvcName, pvcNamespace); err != nil {
			logger.Error(err, "Timeout waiting for PVC deletion", "pvc", pvcName)
		}
	}

	pv := &corev1.PersistentVolume{}
	if pvName != "" {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvName}, pv); err != nil {
			if errors.IsNotFound(err) {
				logger.V(1).Info("PV already deleted", "pv", pvName)
				return nil
			}
			return fmt.Errorf("failed to get PV %s: %w", pvName, err)
		}
	} else {
		found, err := findPVByUIDLabel(ctx, k8sClient, uidLabelKey, resourceUID)
		if err != nil {
			return fmt.Errorf("failed to list PVs by label: %w", err)
		}
		if found == nil {
			logger.V(1).Info("No PV found with label, already cleaned up", "label", uidLabelKey, "value", resourceUID)
			return nil
		}
		pv = found
		pvName = pv.Name
		logger.Info("Found PV by label", "pv", pvName, "label", uidLabelKey)
	}

	if err := patchPVReclaimPolicy(ctx, k8sClient, pv, corev1.PersistentVolumeReclaimDelete); err != nil {
		logger.Error(err, "Failed to set PV reclaim policy to Delete after retries", "pv", pvName)
		return fmt.Errorf("cannot delete PV %s: failed to set reclaim policy to Delete (would cause storage leakage): %w", pvName, err)
	}

	if err := k8sClient.Delete(ctx, pv); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete PV %s: %w", pvName, err)
	}
	logger.Info("Deleted PV", "pv", pvName)

	return nil
}
