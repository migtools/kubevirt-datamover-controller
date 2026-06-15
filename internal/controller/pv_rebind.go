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

const (
	// PVRebindTimeout is the maximum time to wait for PV rebinding operations
	PVRebindTimeout = 2 * time.Minute

	// PVRebindPollInterval is the interval between polling for PV binding status
	PVRebindPollInterval = 2 * time.Second

	// KubeAnnBoundByController is the annotation added by Kubernetes PV controller
	KubeAnnBoundByController = "pv.kubernetes.io/bound-by-controller"

	// PatchRetryAttempts is the number of times to retry patch operations
	PatchRetryAttempts = 3

	// PatchRetryInterval is the interval between patch retry attempts
	PatchRetryInterval = 1 * time.Second
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

// rebindPVToNamespace rebinds a PV from a PVC in the source namespace to a new PVC in the target namespace.
// This follows the same pattern as Velero's generic restore exposer, using Patch operations
// to avoid conflicts with Kubernetes PV controller.
//
// Steps:
// 1. Get the PV bound to the source PVC
// 2. Set PV reclaim policy to Retain (using Patch)
// 3. Delete the source PVC (PV stays due to Retain)
// 4. Create new PVC in target namespace with volumeName and selector
// 5. Reset PV binding: set claimRef to new PVC, add labels (using Patch)
// 6. Wait for PV to bind to new PVC
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
) (*PVRebindResult, error) {
	// Step 1: Get the source PVC and its bound PV
	sourcePVC := &corev1.PersistentVolumeClaim{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: sourcePVCName, Namespace: sourceNamespace}, sourcePVC); err != nil {
		return nil, fmt.Errorf("failed to get source PVC %s/%s: %w", sourceNamespace, sourcePVCName, err)
	}

	if sourcePVC.Status.Phase != corev1.ClaimBound {
		return nil, fmt.Errorf("source PVC %s/%s is not bound (phase: %s)", sourceNamespace, sourcePVCName, sourcePVC.Status.Phase)
	}

	pvName := sourcePVC.Spec.VolumeName
	if pvName == "" {
		return nil, fmt.Errorf("source PVC %s/%s has no volume name", sourceNamespace, sourcePVCName)
	}

	pv := &corev1.PersistentVolume{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvName}, pv); err != nil {
		return nil, fmt.Errorf("failed to get PV %s: %w", pvName, err)
	}

	logger.Info("Found PV bound to source PVC", "pv", pvName, "sourcePVC", sourcePVCName)

	// Step 2: Set PV reclaim policy to Retain using Patch
	originalReclaimPolicy := pv.Spec.PersistentVolumeReclaimPolicy
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		if err := patchPVReclaimPolicy(ctx, k8sClient, pv, corev1.PersistentVolumeReclaimRetain); err != nil {
			return nil, fmt.Errorf("failed to set PV %s reclaim policy to Retain: %w", pvName, err)
		}
		logger.Info("Set PV reclaim policy to Retain", "pv", pvName, "originalPolicy", originalReclaimPolicy)
	}

	// Step 3: Delete the source PVC
	if err := k8sClient.Delete(ctx, sourcePVC); err != nil && !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to delete source PVC %s/%s: %w", sourceNamespace, sourcePVCName, err)
	}
	logger.Info("Deleted source PVC", "pvc", sourcePVCName, "namespace", sourceNamespace)

	// Wait for PVC to be fully deleted
	if err := waitForPVCDeletion(ctx, k8sClient, sourcePVCName, sourceNamespace); err != nil {
		return nil, fmt.Errorf("failed waiting for source PVC deletion: %w", err)
	}

	// Step 4: Create new PVC in target namespace with volumeName and selector
	labelKey := uidLabelKey
	labelValue := resourceUID
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
					labelKey: labelValue,
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
		logger.Info("Rebound PVC already exists", "pvc", newPVCName, "namespace", targetNamespace)
	} else {
		logger.Info("Created new PVC in target namespace", "pvc", newPVCName, "namespace", targetNamespace)
	}

	// Step 5: Reset PV binding using Patch (like Velero's ResetPVBinding)
	// Re-fetch PV to get latest version
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvName}, pv); err != nil {
		return nil, fmt.Errorf("failed to re-fetch PV %s: %w", pvName, err)
	}

	labels := map[string]string{labelKey: labelValue}
	if err := patchPVBinding(ctx, k8sClient, pv, newPVC, labels); err != nil {
		return nil, fmt.Errorf("failed to reset PV %s binding: %w", pvName, err)
	}
	logger.Info("Reset PV binding to new PVC", "pv", pvName, "newPVC", newPVCName, "namespace", targetNamespace)

	// Step 6: Wait for PV to bind to new PVC
	if err := waitForPVCBound(ctx, k8sClient, newPVCName, targetNamespace); err != nil {
		return nil, fmt.Errorf("failed waiting for new PVC to bind: %w", err)
	}
	logger.Info("New PVC is bound to PV", "pvc", newPVCName, "pv", pvName)

	return &PVRebindResult{
		NewPVCName:            newPVCName,
		NewPVCNamespace:       targetNamespace,
		PVName:                pvName,
		OriginalReclaimPolicy: originalReclaimPolicy,
	}, nil
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
	return wait.PollUntilContextTimeout(ctx, PVRebindPollInterval, PVRebindTimeout, true, func(ctx context.Context) (bool, error) {
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

// waitForPVCBound waits for a PVC to be bound.
func waitForPVCBound(ctx context.Context, k8sClient client.Client, pvcName, namespace string) error {
	return wait.PollUntilContextTimeout(ctx, PVRebindPollInterval, PVRebindTimeout, true, func(ctx context.Context) (bool, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: namespace}, pvc); err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return pvc.Status.Phase == corev1.ClaimBound, nil
	})
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
		pvList := &corev1.PersistentVolumeList{}
		if err := k8sClient.List(ctx, pvList, client.MatchingLabels{uidLabelKey: resourceUID}); err != nil {
			return fmt.Errorf("failed to list PVs by label: %w", err)
		}
		if len(pvList.Items) == 0 {
			logger.V(1).Info("No PV found with label, already cleaned up", "label", uidLabelKey, "value", resourceUID)
			return nil
		}
		if len(pvList.Items) > 1 {
			logger.Info("Warning: multiple PVs found with label, cleaning up first one", "count", len(pvList.Items))
		}
		pv = &pvList.Items[0]
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
