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
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
)

// DefaultOperationTimeout bounds how long a DataUpload/DataDownload may remain
// in a non-terminal phase after being Accepted when Spec.OperationTimeout is
// unset or zero. Matches Velero server's own default item-operation-timeout.
const DefaultOperationTimeout = 4 * time.Hour

// immediateRequeueDelay is used by capRequeueToOperationDeadline in place of a
// zero RequeueAfter: returning ctrl.Result{RequeueAfter: 0} with a nil error
// is treated by controller-runtime as "don't requeue," not "requeue now."
const immediateRequeueDelay = time.Second

// operationTimeoutExceeded reports whether the time elapsed since acceptedAt
// exceeds the effective operation timeout: specTimeout when positive, otherwise
// DefaultOperationTimeout. Returns exceeded=false if acceptedAt is nil (nothing
// to measure against yet).
func operationTimeoutExceeded(acceptedAt *metav1.Time, specTimeout time.Duration) (exceeded bool, elapsed, effective time.Duration) {
	if acceptedAt == nil {
		return false, 0, 0
	}
	effective = specTimeout
	if effective <= 0 {
		effective = DefaultOperationTimeout
	}
	elapsed = time.Since(acceptedAt.Time)
	return elapsed >= effective, elapsed, effective
}

// capRequeueToOperationDeadline caps result.RequeueAfter so a phase handler's
// own requeue delay (e.g. RequeueAfterLong) can never push the next reconcile
// past the operation's timeout deadline -- otherwise a short Spec.OperationTimeout
// could be overshot by however long the handler's own poll interval is before
// checkOperationTimeout gets a chance to re-evaluate it.
func capRequeueToOperationDeadline(result ctrl.Result, acceptedAt *metav1.Time, specTimeout time.Duration) ctrl.Result {
	if result.RequeueAfter <= 0 || acceptedAt == nil {
		return result
	}
	_, elapsed, effective := operationTimeoutExceeded(acceptedAt, specTimeout)
	remaining := effective - elapsed
	switch {
	case remaining <= 0:
		// The deadline has already passed -- e.g. the phase handler itself took
		// long enough to run that it crossed the deadline after checkOperationTimeout
		// last evaluated it. Requeue almost immediately instead of preserving the
		// handler's original (possibly long) delay, so the next reconcile can fail
		// it right away rather than waiting out a stale poll interval.
		result.RequeueAfter = immediateRequeueDelay
	case remaining < result.RequeueAfter:
		result.RequeueAfter = remaining
	}
	return result
}

// operationTimeoutTarget adapts a DataUpload/DataDownload to checkOperationTimeoutCore
// via accessors, since the two are distinct vendored Velero types (different Phase
// enums, different updatePhase methods) with no shared interface to dispatch on directly.
// This keeps the backfill / exceeded-check / failure-message logic in exactly one
// place instead of duplicated per controller.
type operationTimeoutTarget struct {
	// acceptedTimestamp returns the resource's current Status.AcceptedTimestamp.
	acceptedTimestamp func() *metav1.Time
	// setAcceptedTimestamp backfills Status.AcceptedTimestamp on the in-memory object.
	setAcceptedTimestamp func(*metav1.Time)
	// operationTimeout is the resource's Spec.OperationTimeout.Duration.
	operationTimeout time.Duration
	// phase returns the resource's current Status.Phase as a string, for logging
	// and the failure message.
	phase func() string
	// persist writes back an in-place mutation (the AcceptedTimestamp backfill)
	// without also changing phase.
	persist func(ctx context.Context) error
	// fail transitions the resource to its Failed phase with the given message.
	fail func(ctx context.Context, message string) error
}

// checkOperationTimeoutCore fails the target if too much time has elapsed since
// it was accepted, per its effective OperationTimeout (falling back to
// DefaultOperationTimeout when unset -- see operationTimeoutExceeded). Self-heals
// a missing AcceptedTimestamp -- e.g. a resource already past New when this check
// was introduced -- by backfilling it to now rather than leaving the operation
// unbounded forever.
func checkOperationTimeoutCore(ctx context.Context, logger logr.Logger, resourceKind string, t operationTimeoutTarget) (failed bool, err error) {
	if t.acceptedTimestamp() == nil {
		now := metav1.Now()
		t.setAcceptedTimestamp(&now)
		logger.Info("Backfilling missing AcceptedTimestamp", "kind", resourceKind, "phase", t.phase())
		if err := t.persist(ctx); err != nil {
			return false, fmt.Errorf("failed to backfill AcceptedTimestamp: %w", err)
		}
		return false, nil
	}

	exceeded, elapsed, effective := operationTimeoutExceeded(t.acceptedTimestamp(), t.operationTimeout)
	if !exceeded {
		return false, nil
	}

	logger.Error(nil, resourceKind+" exceeded operation timeout",
		"phase", t.phase(), "elapsed", elapsed.Round(time.Second), "timeout", effective)
	// t.fail may fail here either because it couldn't stop a still-running pod or
	// because persisting the Failed phase itself failed -- either way, propagate
	// so the reconcile retries rather than silently leaving a pod running behind
	// a resource that was never actually marked Failed.
	if err := t.fail(ctx, fmt.Sprintf("operation timed out after %s in phase %s (limit %s)", elapsed.Round(time.Second), t.phase(), effective)); err != nil {
		return false, fmt.Errorf("failed to fail %s on operation timeout: %w", resourceKind, err)
	}
	return true, nil
}

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

// isTransientBSLLookupError reports whether an error from getBackupStorageLocation
// (or its ...ForDU/...ForDD wrappers) is worth retrying (a transient API
// hiccup or cache-not-yet-synced 404) rather than a definitive "BSL doesn't
// exist" failure. An empty bslName is always definitive -- a spec-configuration
// error retrying can never fix -- regardless of what the error itself says.
// A genuine apierrors.NotFound (the BSL object doesn't exist) is also
// definitive. Anything else (timeouts, throttling, other API errors) is
// treated as transient so the caller returns the error for controller-runtime
// to retry with backoff, instead of terminally failing on something that might
// resolve on its own.
func isTransientBSLLookupError(err error, bslName string) bool {
	return bslName != "" && !errors.IsNotFound(err)
}

// findPodByUID finds the unique datamover pod associated with a resource UID.
// Tries the cached client first; if it finds nothing, retries via apiReader
// (an uncached read) before the caller concludes the pod is genuinely absent --
// controller-runtime's informer cache is only eventually consistent, so a pod
// this same controller created moments ago in an earlier reconcile may not yet
// be visible to a cached List. apiReader may be nil (falls back to cached-only
// behavior), so existing callers/tests that don't wire one still work.
func findPodByUID(ctx context.Context, k8sClient client.Client, apiReader client.Reader, uidLabelKey, uid, namespace string) (*corev1.Pod, error) {
	pod, err := listPodByUID(ctx, k8sClient, uidLabelKey, uid, namespace)
	if err != nil || pod != nil || apiReader == nil {
		return pod, err
	}
	return listPodByUID(ctx, apiReader, uidLabelKey, uid, namespace)
}

func listPodByUID(ctx context.Context, reader client.Reader, uidLabelKey, uid, namespace string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := reader.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{uidLabelKey: uid}); err != nil {
		return nil, err
	}
	if len(podList.Items) == 0 {
		return nil, nil
	}
	if len(podList.Items) > 1 {
		return nil, fmt.Errorf("found multiple datamover pods in namespace %s with label %s=%s", namespace, uidLabelKey, uid)
	}
	return &podList.Items[0], nil
}

// cleanupPodsByUID deletes all pods matching a UID label in the given namespace
// and reports whether it's NOT yet safe to proceed as though they're gone.
// A true return folds together several cases callers should treat the same way
// (retry on a later reconcile): a List failure (unknown state, so
// conservatively assume cleanup isn't done yet), a Delete call that failed for
// a reason other than the pod already being gone, or a pod still present
// after the delete calls (Delete only requests removal -- kubelet must still
// terminate containers and unmount volumes before the pod object actually
// disappears).
func cleanupPodsByUID(ctx context.Context, k8sClient client.Client, uidLabelKey, uid, namespace string, logger logr.Logger) bool {
	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{uidLabelKey: uid}); err != nil {
		logger.Error(err, "Failed to list datamover pods for cleanup")
		return true
	}
	deleteFailed := false
	for i := range podList.Items {
		pod := &podList.Items[i]
		switch err := k8sClient.Delete(ctx, pod); {
		case err == nil:
			logger.Info("Deleted datamover pod", "pod", pod.Name)
		case errors.IsNotFound(err):
			logger.V(1).Info("Datamover pod already gone", "pod", pod.Name)
		default:
			logger.Error(err, "Failed to delete datamover pod", "pod", pod.Name)
			deleteFailed = true
		}
	}
	if deleteFailed {
		return true
	}

	if err := k8sClient.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels{uidLabelKey: uid}); err != nil {
		logger.Error(err, "Failed to re-check datamover pods after delete")
		return true
	}
	return len(podList.Items) > 0
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
		if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" {
			return cs.State.Terminated.Reason
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

// NewPodLogCollector returns a PodLogCollector function that reads the last
// tailLines of log output from a pod using the Kubernetes API.
func NewPodLogCollector(clientset kubernetes.Interface, tailLines int64) func(ctx context.Context, podName, podNamespace string) (string, error) {
	return func(ctx context.Context, podName, podNamespace string) (string, error) {
		opts := &corev1.PodLogOptions{
			TailLines: &tailLines,
		}
		stream, err := clientset.CoreV1().Pods(podNamespace).GetLogs(podName, opts).Stream(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to stream pod logs: %w", err)
		}
		defer func() { _ = stream.Close() }()
		data, err := io.ReadAll(stream)
		if err != nil {
			return "", fmt.Errorf("failed to read pod logs: %w", err)
		}
		return string(data), nil
	}
}
