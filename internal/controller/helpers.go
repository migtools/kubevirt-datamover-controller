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
	"fmt"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"context"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

// getBackupStorageLocation fetches the BSL by name from the OADP namespace,
// falling back to fallbackNamespace if oadpNamespace is empty.
func getBackupStorageLocation(ctx context.Context, k8sClient client.Client, bslName, oadpNamespace, fallbackNamespace string) (*velerov1.BackupStorageLocation, error) {
	if bslName == "" {
		return nil, fmt.Errorf("no BackupStorageLocation name specified")
	}

	namespace := oadpNamespace
	if namespace == "" {
		namespace = fallbackNamespace
	}

	bsl := &velerov1.BackupStorageLocation{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: bslName, Namespace: namespace}, bsl); err != nil {
		return nil, fmt.Errorf("failed to get BackupStorageLocation %s/%s: %w", namespace, bslName, err)
	}

	return bsl, nil
}

// findPodByUID finds the unique datamover pod associated with a resource UID.
func findPodByUID(ctx context.Context, k8sClient client.Client, uidLabelKey, uid, namespace string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{uidLabelKey: uid}); err != nil {
		return nil, err
	}
	if len(podList.Items) == 0 {
		return nil, nil
	}
	if len(podList.Items) > 1 {
		return nil, fmt.Errorf("found multiple datamover pods for UID %s", uid)
	}
	return &podList.Items[0], nil
}

// cleanupPodsByUID deletes all pods matching a UID label in the given namespace.
func cleanupPodsByUID(ctx context.Context, k8sClient client.Client, uidLabelKey, uid, namespace string, logger logr.Logger) {
	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{uidLabelKey: uid}); err != nil {
		logger.Error(err, "Failed to list datamover pods for cleanup")
		return
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		if err := k8sClient.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete datamover pod", "pod", pod.Name)
		} else {
			logger.Info("Deleted datamover pod", "pod", pod.Name)
		}
	}
}

// extractPodFailureMessage extracts the failure message from a failed pod.
func extractPodFailureMessage(pod *corev1.Pod) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason
		}
	}

	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
			return cs.State.Terminated.Message
		}
	}

	for _, cond := range pod.Status.Conditions {
		if cond.Status == corev1.ConditionFalse && cond.Message != "" {
			return cond.Message
		}
	}

	return "unknown error"
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

// addOverhead returns qty increased by the given percentage.
// For example, addOverhead(30Gi, 20) returns 36Gi.
func addOverhead(qty resource.Quantity, percent int64) resource.Quantity {
	base := qty.Value()
	overhead := base * percent / 100
	result := resource.NewQuantity(base+overhead, resource.BinarySI)
	return *result
}

// getVeleroBackupName extracts the Velero backup name from resource labels.
func getVeleroBackupName(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	return labels[common.LabelVeleroBackupName]
}
