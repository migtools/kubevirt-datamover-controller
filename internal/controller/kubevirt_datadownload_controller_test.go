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

//nolint:goconst // Test files use repeated string literals for readability
package controller

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/downloader"
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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// ddScheme returns a scheme with only the types the DataDownload controller
// needs. Deliberately does NOT register kubevirtcorev1 -- handleNew/handleAccepted
// must never fetch a live VirtualMachine object; if they tried, the fake client
// would fail with "no kind registered" and these tests would catch it.
func ddScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func TestDataDownloadReconcile(t *testing.T) {
	scheme := ddScheme()

	tests := []struct {
		name            string
		dataDownload    *velerov2alpha1.DataDownload
		expectedRequeue bool
		expectedPhase   velerov2alpha1.DataDownloadPhase
	}{
		{
			name: "skip non-kubevirt datamover",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: "velero"},
			},
			expectedRequeue: false,
			expectedPhase:   "",
		},
		{
			name: "new phase without VM annotation fails",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "terminal phase is a no-op",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseCompleted},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseCompleted,
		},
		{
			name: "Cancel requested while InProgress routes to Canceling instead of the normal handler",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, Cancel: true},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
			},
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseCanceling,
		},
		{
			name: "Cancel requested while already terminal is ignored",
			dataDownload: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, Cancel: true},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseFailed},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataDownloadPhaseFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.dataDownload).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

			result, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: tt.dataDownload.Name, Namespace: tt.dataDownload.Namespace},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if (result.RequeueAfter > 0) != tt.expectedRequeue {
				t.Errorf("requeue = %v, want %v", result.RequeueAfter > 0, tt.expectedRequeue)
			}

			var updated velerov2alpha1.DataDownload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.dataDownload.Name, Namespace: tt.dataDownload.Namespace}, &updated); err != nil {
				t.Fatalf("failed to get DataDownload: %v", err)
			}
			if updated.Status.Phase != tt.expectedPhase {
				t.Errorf("phase = %q, want %q", updated.Status.Phase, tt.expectedPhase)
			}
		})
	}

	t.Run("not found is ignored", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}
		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing", Namespace: "ns"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue, got %v", result.RequeueAfter)
		}
	})

	t.Run("Cancel requested but restore already provisioned completes instead of canceling", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp", UID: types.UID("dd-uid-provisioned")},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				Cancel:    true,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC:       "restored-disk-1",
					Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
		}
		targetPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "restored-disk-1", Namespace: "restore-ns", UID: types.UID("restored-disk-1-uid"),
			},
		}
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-rebound",
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{Name: "restored-disk-1", Namespace: "restore-ns", UID: targetPVC.UID},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, reboundPV, targetPVC).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue, got %v", result.RequeueAfter)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Errorf("phase = %q, want %q (Cancel must not override an already-provisioned restore)",
				updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted)
		}
	})
}

// TestDataDownloadReconcile_OperationTimeout covers Spec.OperationTimeout
// enforcement across several independent t.Run subtests.
//
//nolint:gocyclo // Table of independent subtests, not complex control flow
func TestDataDownloadReconcile_OperationTimeout(t *testing.T) {
	scheme := ddScheme()

	get := func(t *testing.T, c client.Client, name, namespace string) *velerov2alpha1.DataDownload {
		t.Helper()
		var out velerov2alpha1.DataDownload
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &out); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		return &out
	}

	t.Run("Accepted phase past default operation timeout fails", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-timeout", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue after timeout failure, got %v", result.RequeueAfter)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		if !strings.Contains(updated.Status.Message, "operation timed out") {
			t.Errorf("message = %q, want it to mention the timeout", updated.Status.Message)
		}
	})

	t.Run("Prepared phase past default operation timeout fails", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-prepared-timeout", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhasePrepared,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("expected no requeue after timeout failure, got %v", result.RequeueAfter)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		if !strings.Contains(updated.Status.Message, "operation timed out") {
			t.Errorf("message = %q, want it to mention the timeout", updated.Status.Message)
		}
	})

	t.Run("InProgress phase respects custom Spec.OperationTimeout", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-custom-timeout", Namespace: "openshift-adp"},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover:        common.DataMoverKubeVirt,
				OperationTimeout: metav1.Duration{Duration: time.Hour},
			},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-2 * time.Hour)),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q (custom 1h OperationTimeout exceeded by 2h elapsed)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
	})

	t.Run("nil AcceptedTimestamp is backfilled without failing", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-backfill", Namespace: "openshift-adp",
				Annotations: map[string]string{common.AnnotationVMName: "vm-1", common.AnnotationVMNamespace: "vm-ns"},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC: "restored-disk-1", Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseAccepted},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
			t.Errorf("phase = %q, want %q (backfilling a missing AcceptedTimestamp must not fail the DataDownload, message: %q)",
				updated.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted, updated.Status.Message)
		}
		if updated.Status.AcceptedTimestamp == nil {
			t.Errorf("expected AcceptedTimestamp to be backfilled, got nil")
		}
	})

	t.Run("Canceling phase is not subject to operation timeout", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-canceling", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseCanceling,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Hour))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCanceled {
			t.Errorf("phase = %q, want %q (Canceling must run to completion, not be preempted by the timeout check)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCanceled)
		}
	})

	t.Run("handler's RequeueAfterLong is capped to the remaining custom OperationTimeout", func(t *testing.T) {
		// handleAccepted's target-PVC-not-found branch normally requeues after
		// RequeueAfterLong (30s) -- with a custom OperationTimeout that has
		// nearly elapsed, that would overshoot the deadline. The returned
		// RequeueAfter must be capped to (roughly) what's left instead. Uses a
		// 30s timeout with 27s elapsed (a wider margin than a few-second
		// timeout) so the test isn't flaky against real wall-clock execution
		// overhead.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-cap-requeue", Namespace: "openshift-adp",
				Annotations: map[string]string{common.AnnotationVMName: "vm-1", common.AnnotationVMNamespace: "vm-ns"},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover:        common.DataMoverKubeVirt,
				OperationTimeout: metav1.Duration{Duration: 30 * time.Second},
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC: "not-yet-created", Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now().Add(-27 * time.Second)),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter <= 0 || result.RequeueAfter >= RequeueAfterLong {
			t.Errorf("RequeueAfter = %v, want it capped below RequeueAfterLong (%v) to the ~3s remaining before the 30s OperationTimeout deadline", result.RequeueAfter, RequeueAfterLong)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
			t.Errorf("phase = %q, want %q (timeout not yet exceeded)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
		}
	})

	t.Run("handler's RequeueAfterLong is capped using the default timeout when Spec.OperationTimeout is unset", func(t *testing.T) {
		// Same as above, but with Spec.OperationTimeout left at its zero value:
		// capRequeueToOperationDeadline must fall back to DefaultOperationTimeout
		// (via operationTimeoutExceeded), consistently with checkOperationTimeout,
		// rather than treating an unset timeout as "no cap."
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-cap-requeue-default-timeout", Namespace: "openshift-adp",
				Annotations: map[string]string{common.AnnotationVMName: "vm-1", common.AnnotationVMNamespace: "vm-ns"},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC: "not-yet-created", Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout - 3*time.Second))),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter <= 0 || result.RequeueAfter >= RequeueAfterLong {
			t.Errorf("RequeueAfter = %v, want it capped below RequeueAfterLong (%v) to the ~3s remaining before the DefaultOperationTimeout deadline", result.RequeueAfter, RequeueAfterLong)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
			t.Errorf("phase = %q, want %q (timeout not yet exceeded)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
		}
	})

	t.Run("New phase's first requeue is capped when Spec.OperationTimeout is shorter than RequeueAfterShort", func(t *testing.T) {
		// handleNew sets AcceptedTimestamp and transitions New -> Accepted in the
		// same reconcile that creates it, returning RequeueAfterShort (5s). With a
		// custom OperationTimeout shorter than that, the very first requeue --
		// not just subsequent ones -- must already be capped to the deadline.
		bslAvailable := &velerov1.BackupStorageLocation{
			ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
			Status:     velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
		}
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-new-cap-requeue", Namespace: "openshift-adp",
				Annotations: map[string]string{common.AnnotationVMName: "vm-1", common.AnnotationVMNamespace: "vm-ns"},
			},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover:             common.DataMoverKubeVirt,
				BackupStorageLocation: "default",
				OperationTimeout:      metav1.Duration{Duration: 2 * time.Second},
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC: "restored-disk-1", Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseNew},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, bslAvailable).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
			t.Fatalf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
		}
		if result.RequeueAfter <= 0 || result.RequeueAfter >= RequeueAfterShort {
			t.Errorf("RequeueAfter = %v, want it capped below RequeueAfterShort (%v) to the ~2s remaining before the 2s OperationTimeout deadline", result.RequeueAfter, RequeueAfterShort)
		}
	})

	t.Run("terminal transition within the same reconcile is never given a nonzero requeue by the cap", func(t *testing.T) {
		// handleAccepted's own (unrelated-to-timeout) failure path -- e.g. a
		// missing VM reference -- transitions straight to Failed and returns
		// ctrl.Result{} (no requeue) in the same reconcile that ran
		// checkOperationTimeout. Guards against capRequeueToOperationDeadline
		// ever being applied to a terminal transition's result.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-terminal-no-requeue", Namespace: "openshift-adp"},
			Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseAccepted,
				AcceptedTimestamp: ptrTime(time.Now()),
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter != 0 {
			t.Errorf("RequeueAfter = %v, want 0 (terminal transition must not be requeued)", result.RequeueAfter)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Fatalf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		if strings.Contains(updated.Status.Message, "operation timed out") {
			t.Errorf("message = %q, want a handler-level failure (missing VM reference), not a timeout", updated.Status.Message)
		}
	})

	t.Run("timeout does not override a restore that already provisioned the target volume", func(t *testing.T) {
		// Mirrors the Cancel-vs-provisioned race already handled at the top of
		// Reconcile: a restore whose rebind already committed (PV claimRef set)
		// but whose Completed phase update hasn't persisted yet (e.g. a transient
		// API error right after a successful rebind) must not be misreported as
		// Failed just because Spec.OperationTimeout has since expired.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "dd-timeout-provisioned", Namespace: "openshift-adp", UID: types.UID("dd-timeout-provisioned-uid")},
			Spec: velerov2alpha1.DataDownloadSpec{
				DataMover: common.DataMoverKubeVirt,
				TargetVolume: velerov2alpha1.TargetVolumeSpec{
					PVC:       "restored-disk-1",
					Namespace: "restore-ns",
				},
			},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		targetPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "restored-disk-1", Namespace: "restore-ns", UID: types.UID("restored-disk-1-uid"),
			},
		}
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-rebound-timeout",
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{Name: "restored-disk-1", Namespace: "restore-ns", UID: targetPVC.UID},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, reboundPV, targetPVC).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Errorf("phase = %q, want %q (an already-provisioned restore must finalize as Completed, not be failed by an expired timeout)",
				updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted)
		}
	})

	t.Run("timeout failure stops the still-running downloader pod", func(t *testing.T) {
		// A timeout can fire while the downloader pod is still Pending/Running --
		// that's exactly the unbounded-wait branch being guarded against -- unlike
		// other Failed paths where the pod has already terminated on its own.
		// Verifies checkOperationTimeoutCore's fail callback actually stops it
		// rather than leaving it running indefinitely against a terminal DataDownload.
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-timeout-pod-cleanup", Namespace: "openshift-adp",
				UID: types.UID("dd-timeout-pod-cleanup-uid"),
			},
			Spec: velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:             velerov2alpha1.DataDownloadPhaseInProgress,
				AcceptedTimestamp: ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			},
		}
		runningPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "dd-timeout-pod-cleanup-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(dd.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd, runningPod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		updated := get(t, fakeClient, dd.Name, dd.Namespace)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Fatalf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		var pod corev1.Pod
		err := fakeClient.Get(context.Background(), types.NamespacedName{Name: runningPod.Name, Namespace: runningPod.Namespace}, &pod)
		if !errors.IsNotFound(err) {
			t.Errorf("expected downloader pod to be deleted after timeout failure, got err=%v", err)
		}
	})
}

func TestDataDownloadUpdatePhase(t *testing.T) {
	scheme := ddScheme()
	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
		Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseNew},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

	if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseAccepted, "accepted"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dd.Status.Phase != velerov2alpha1.DataDownloadPhaseAccepted {
		t.Errorf("phase = %q, want %q", dd.Status.Phase, velerov2alpha1.DataDownloadPhaseAccepted)
	}

	var before velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &before); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}

	// Idempotent: same phase + message should skip the update (no error, no-op).
	if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseAccepted, "accepted"); err != nil {
		t.Fatalf("unexpected error on idempotent update: %v", err)
	}

	var after velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &after); err != nil {
		t.Fatalf("failed to re-get DataDownload: %v", err)
	}
	if before.ResourceVersion != after.ResourceVersion {
		t.Errorf("expected no write on idempotent update, resourceVersion changed %s -> %s",
			before.ResourceVersion, after.ResourceVersion)
	}
}

// TestDataDownloadUpdatePhase_TimestampsSet covers #155: updatePhase must
// populate Status.StartTimestamp on entering InProgress and
// Status.CompletionTimestamp on entering any terminal phase, matching
// Velero's own built-in data movers so `velero restore describe` reports
// timing consistently. Both must be idempotent -- set once, not reset on a
// later call with an already-set value.
func TestDataDownloadUpdatePhase_TimestampsSet(t *testing.T) {
	scheme := ddScheme()

	t.Run("StartTimestamp set on entering InProgress", func(t *testing.T) {
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
			Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhasePrepared},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseInProgress, "Downloader pod launched"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dd.Status.StartTimestamp == nil {
			t.Fatal("expected StartTimestamp to be set")
		}
	})

	t.Run("CompletionTimestamp set on entering a terminal phase", func(t *testing.T) {
		for _, phase := range []velerov2alpha1.DataDownloadPhase{
			velerov2alpha1.DataDownloadPhaseCompleted,
			velerov2alpha1.DataDownloadPhaseFailed,
			velerov2alpha1.DataDownloadPhaseCanceled,
		} {
			dd := &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "test-dd-" + string(phase), Namespace: "openshift-adp"},
				Status:     velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseInProgress},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

			if err := r.updatePhase(context.Background(), dd, phase, "done"); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if dd.Status.CompletionTimestamp == nil {
				t.Errorf("phase %s: expected CompletionTimestamp to be set", phase)
			}
		}
	})

	t.Run("timestamps are idempotent, not reset by a later call", func(t *testing.T) {
		earlier := metav1.NewTime(time.Now().Add(-time.Hour))
		dd := &velerov2alpha1.DataDownload{
			ObjectMeta: metav1.ObjectMeta{Name: "test-dd", Namespace: "openshift-adp"},
			Status: velerov2alpha1.DataDownloadStatus{
				Phase:               velerov2alpha1.DataDownloadPhaseInProgress,
				StartTimestamp:      &earlier,
				CompletionTimestamp: &earlier,
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard()}

		if err := r.updatePhase(context.Background(), dd, velerov2alpha1.DataDownloadPhaseCompleted, "done"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !dd.Status.StartTimestamp.Equal(&earlier) {
			t.Errorf("StartTimestamp = %v, want it unchanged from %v", dd.Status.StartTimestamp, earlier)
		}
		if !dd.Status.CompletionTimestamp.Equal(&earlier) {
			t.Errorf("CompletionTimestamp = %v, want it unchanged from %v", dd.Status.CompletionTimestamp, earlier)
		}
	})
}

func TestHandleNewDataDownload(t *testing.T) {
	scheme := ddScheme()

	bslAvailable := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
		Status:     velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}
	bslUnavailable := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "openshift-adp"},
		Status:     velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseUnavailable},
	}

	tests := []struct {
		name          string
		dd            *velerov2alpha1.DataDownload
		bsl           *velerov1.BackupStorageLocation
		expectedPhase velerov2alpha1.DataDownloadPhase
	}{
		{
			name: "missing VM annotation fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{Name: "dd-1", Namespace: "openshift-adp"},
				Spec:       velerov2alpha1.DataDownloadSpec{DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default"},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "BSL not found fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-2", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "missing-bsl",
					SourceNamespace: "vm-ns",
					TargetVolume:    velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "BSL not available fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-3", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
					SourceNamespace: "vm-ns",
					TargetVolume:    velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslUnavailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
		{
			name: "valid annotation and available BSL transitions to Accepted",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-4", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
					SourceNamespace: "vm-ns",
					TargetVolume:    velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
				},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseAccepted,
		},
		{
			name: "missing TargetVolume fails",
			dd: &velerov2alpha1.DataDownload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dd-5", Namespace: "openshift-adp",
					Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
				},
				Spec: velerov2alpha1.DataDownloadSpec{
					DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
					SourceNamespace: "vm-ns",
				},
			},
			bsl:           bslAvailable,
			expectedPhase: velerov2alpha1.DataDownloadPhaseFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.dd, tt.bsl).Build()
			r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

			// This would panic/error if handleNew ever tried to Get a VirtualMachine,
			// since kubevirtcorev1 isn't registered in ddScheme().
			if _, err := r.handleNew(context.Background(), logr.Discard(), tt.dd); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated velerov2alpha1.DataDownload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: tt.dd.Name, Namespace: tt.dd.Namespace}, &updated); err != nil {
				t.Fatalf("failed to get DataDownload: %v", err)
			}
			if updated.Status.Phase != tt.expectedPhase {
				t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, tt.expectedPhase, updated.Status.Message)
			}
			if tt.expectedPhase == velerov2alpha1.DataDownloadPhaseAccepted && updated.Status.AcceptedTimestamp == nil {
				t.Error("AcceptedTimestamp not set when transitioning to Accepted")
			}
		})
	}
}

// TestHandleNewDataDownload_BSLTransientError_RequeuesWithoutFailing covers
// #123: a transient error from the BSL lookup (API hiccup, cache-not-yet-synced
// -- anything other than a genuine NotFound) must not terminally fail the
// DataDownload the way a real "BSL doesn't exist" does. It should be returned
// so controller-runtime retries with backoff, since the condition may resolve
// on its own.
func TestHandleNewDataDownload_BSLTransientError_RequeuesWithoutFailing(t *testing.T) {
	scheme := ddScheme()

	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dd-bsl-transient", Namespace: "openshift-adp",
			Annotations: map[string]string{common.AnnotationVMName: "vm-1"},
		},
		Spec: velerov2alpha1.DataDownloadSpec{
			DataMover: common.DataMoverKubeVirt, BackupStorageLocation: "default",
			SourceNamespace: "vm-ns",
			TargetVolume:    velerov2alpha1.TargetVolumeSpec{PVC: "restored-disk-1", Namespace: "restore-ns"},
		},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dd).Build()
	transientErr := fmt.Errorf("simulated transient API error")
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*velerov1.BackupStorageLocation); ok {
				return transientErr
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &KubeVirtDataDownloadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	_, err := r.handleNew(context.Background(), logr.Discard(), dd)
	if err == nil {
		t.Fatal("expected the transient error to be returned for retry, got nil")
	}
	if !strings.Contains(err.Error(), transientErr.Error()) {
		t.Errorf("error = %v, want it to contain %v", err, transientErr)
	}

	var updated velerov2alpha1.DataDownload
	if getErr := baseClient.Get(context.Background(), types.NamespacedName{Name: dd.Name, Namespace: dd.Namespace}, &updated); getErr != nil {
		t.Fatalf("failed to get DataDownload: %v", getErr)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
		t.Error("phase must not be Failed on a transient BSL lookup error -- only a genuine NotFound is terminal")
	}
}

func TestCalculateScratchPVCSize(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       "cp-1",
				PVCs:     []string{"pvc-1", "pvc-2"},
				PVCSizes: []resource.Quantity{resource.MustParse("10Gi"), resource.MustParse("5Gi")},
				Files: []uploader.CheckpointFile{
					{DiskName: "disk-1"},
					{DiskName: "disk-2"},
				},
			},
			{
				ID:       "cp-2",
				PVCs:     []string{"pvc-1"},
				PVCSizes: []resource.Quantity{resource.MustParse("15Gi")},
				Files: []uploader.CheckpointFile{
					{DiskName: "disk-1"},
				},
			},
		},
	}

	// Computed via the same addOverhead helper the production code uses, rather
	// than a hand-picked literal, so this pins the formula (max PVCSize + sum file
	// sizes + overhead) without risking a hand-computed value drifting from it.
	maxPVCSize := resource.MustParse("15Gi") // max(10Gi, 15Gi) across the chain
	expectedBase := maxPVCSize.DeepCopy()
	expectedBase.Add(resource.MustParse("2Gi")) // sum of file sizes (1Gi + 1Gi)
	expectedSized := addOverhead(expectedBase, sizeOverheadPercent)

	tests := []struct {
		name               string
		chain              []string
		targetVolume       string
		files              []uploader.CheckpointFile
		targetDiskCapacity resource.Quantity
		expectExact        *resource.Quantity
	}{
		{
			name:         "sized from max PVCSizes across chain plus file sizes",
			chain:        []string{"cp-1", "cp-2"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
			},
			expectExact: &expectedSized,
		},
		{
			name:         "no matching checkpoints or files falls back to default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files:        nil,
			expectExact:  new(resource.MustParse(DefaultScratchPVCSize)),
		},
		{
			name:         "missing PVCSizes floored by target disk capacity before overhead",
			chain:        []string{"cp-99"}, // no chain match -> maxDiskSize from PVCSizes stays zero
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi
			},
			targetDiskCapacity: resource.MustParse("20Gi"),
			// floor(20Gi) + 1Gi file = 21Gi, then overhead -- computed the same way
			// as expectedSized above, not hand-picked.
			expectExact: func() *resource.Quantity {
				base := resource.MustParse("20Gi")
				base.Add(resource.MustParse("1Gi"))
				q := addOverhead(base, sizeOverheadPercent)
				return &q
			}(),
		},
		{
			// No raw-disk-size floor at all (no PVCSizes match, no target capacity),
			// but the chain itself has files -- sizing off the chain size alone
			// (undoubled) would under-provision for the flattened raw output the
			// downloader writes alongside the still-present chain. Small chain size
			// stays under the default, so the plain default wins.
			name:         "maxDiskSize zero with small file chain returns plain default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 1 * 1024 * 1024 * 1024}, // 1Gi, well under the 10Gi default
			},
			expectExact: new(resource.MustParse(DefaultScratchPVCSize)),
		},
		{
			// Same no-floor scenario, but the chain size itself exceeds the default --
			// must size off the chain doubled (chain + flattened raw coexist), not the
			// now-insufficient default.
			name:         "maxDiskSize zero with large file chain doubles chain size instead of default",
			chain:        []string{"cp-99"},
			targetVolume: "disk-1",
			files: []uploader.CheckpointFile{
				{Size: 15 * 1024 * 1024 * 1024}, // 15Gi, exceeds the 10Gi default
			},
			expectExact: func() *resource.Quantity {
				q := addOverhead(*resource.NewQuantity(30*1024*1024*1024, resource.BinarySI), sizeOverheadPercent)
				return &q
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateScratchPVCSize(logr.Discard(), vmIndex, tt.chain, tt.targetVolume, tt.files, tt.targetDiskCapacity)
			if got.Cmp(*tt.expectExact) != 0 {
				t.Errorf("size = %s, want exactly %s", got.String(), tt.expectExact.String())
			}
		})
	}
}

// TestCalculateWorkPVCSize covers the Block-mode restore's work PVC sizing:
// unlike calculateScratchPVCSize, it must drop the raw-disk-size component
// entirely (the flattened image lands on the separate output PVC instead) and
// size purely off the resolved checkpoint files' sizes.
func TestCalculateWorkPVCSize(t *testing.T) {
	tests := []struct {
		name        string
		files       []uploader.CheckpointFile
		expectExact resource.Quantity
	}{
		{
			name: "sized from file sizes plus overhead",
			files: []uploader.CheckpointFile{
				{Size: 4 * 1024 * 1024 * 1024}, // 4Gi
				{Size: 2 * 1024 * 1024 * 1024}, // 2Gi
			},
			expectExact: addOverhead(*resource.NewQuantity(6*1024*1024*1024, resource.BinarySI), sizeOverheadPercent),
		},
		{
			name:        "no files floors at 1Gi",
			files:       nil,
			expectExact: resource.MustParse("1Gi"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateWorkPVCSize(logr.Discard(), tt.files)
			if got.Cmp(tt.expectExact) != 0 {
				t.Errorf("size = %s, want exactly %s", got.String(), tt.expectExact.String())
			}
		})
	}
}

// TestCalculateOutputPVCSize covers the Block-mode restore's output PVC
// sizing: unlike calculateScratchPVCSize, it must drop the chain-file
// component entirely (the qcow2 chain lives on the separate work PVC instead)
// and size purely off the max known original-disk capacity.
func TestCalculateOutputPVCSize(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       "cp-1",
				PVCs:     []string{"pvc-1"},
				PVCSizes: []resource.Quantity{resource.MustParse("10Gi")},
				Files:    []uploader.CheckpointFile{{DiskName: "disk-1"}},
			},
		},
	}

	t.Run("sized from max PVCSizes across chain, ignoring file sizes", func(t *testing.T) {
		got := calculateOutputPVCSize(logr.Discard(), vmIndex, []string{"cp-1"}, "disk-1", resource.Quantity{})
		want := addOverhead(resource.MustParse("10Gi"), sizeOverheadPercent)
		if got.Cmp(want) != 0 {
			t.Errorf("size = %s, want exactly %s", got.String(), want.String())
		}
	})

	t.Run("floored by target disk capacity when no chain match", func(t *testing.T) {
		got := calculateOutputPVCSize(logr.Discard(), vmIndex, []string{"cp-99"}, "disk-1", resource.MustParse("20Gi"))
		want := addOverhead(resource.MustParse("20Gi"), sizeOverheadPercent)
		if got.Cmp(want) != 0 {
			t.Errorf("size = %s, want exactly %s", got.String(), want.String())
		}
	})

	t.Run("no metadata at all falls back to default", func(t *testing.T) {
		got := calculateOutputPVCSize(logr.Discard(), vmIndex, []string{"cp-99"}, "disk-1", resource.Quantity{})
		want := resource.MustParse(DefaultScratchPVCSize)
		if got.Cmp(want) != 0 {
			t.Errorf("size = %s, want exactly %s", got.String(), want.String())
		}
	})
}

// TestResolveTargetDiskName_ChainTipWins covers a real fix: a VM's disk-to-PVC
// mapping is normally stable across a chain, but if it changed between the full
// backup and a later incremental (e.g. a differently-named DataVolume reattached
// to the same PVC slot), the newest (chain-tip) checkpoint's mapping must win,
// not whichever checkpoint happens to be found first.
func TestResolveTargetDiskName_ChainTipWins(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "vm-1",
		Namespace: "vm-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:   "cp-full",
				PVCs: []string{"restored-disk"},
				Files: []uploader.CheckpointFile{
					{DiskName: "old-disk-name"},
				},
			},
			{
				ID:   "cp-incremental",
				PVCs: []string{"restored-disk"},
				Files: []uploader.CheckpointFile{
					{DiskName: "new-disk-name"},
				},
			},
		},
	}

	// Chain is ordered full-first, incrementals-after (matching
	// pkg/downloader.types.go's documented CheckpointChain convention).
	got, err := resolveTargetDiskName(vmIndex, []string{"cp-full", "cp-incremental"}, "restored-disk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "new-disk-name" {
		t.Errorf("resolveTargetDiskName() = %q, want %q (the chain-tip/newest checkpoint's mapping)", got, "new-disk-name")
	}
}

func TestResolveTargetDiskName_Errors(t *testing.T) {
	tests := []struct {
		name          string
		vmIndex       uploader.VMIndex
		chain         []string
		targetPVCName string
		errorContains string
	}{
		{
			name: "PVC listed but no matching file entry",
			vmIndex: uploader.VMIndex{
				Checkpoints: []uploader.CheckpointEntry{
					{ID: "cp-full", PVCs: []string{"restored-disk"}, Files: []uploader.CheckpointFile{}},
				},
			},
			chain:         []string{"cp-full"},
			targetPVCName: "restored-disk",
			errorContains: "no matching file entry",
		},
		{
			name: "matching file has an empty disk name",
			vmIndex: uploader.VMIndex{
				Checkpoints: []uploader.CheckpointEntry{
					{ID: "cp-full", PVCs: []string{"restored-disk"}, Files: []uploader.CheckpointFile{{DiskName: ""}}},
				},
			},
			chain:         []string{"cp-full"},
			targetPVCName: "restored-disk",
			errorContains: "empty disk name",
		},
		{
			name: "target PVC not found in any checkpoint",
			vmIndex: uploader.VMIndex{
				Checkpoints: []uploader.CheckpointEntry{
					{ID: "cp-full", PVCs: []string{"some-other-pvc"}, Files: []uploader.CheckpointFile{{DiskName: "disk1"}}},
				},
			},
			chain:         []string{"cp-full"},
			targetPVCName: "restored-disk",
			errorContains: "not found in any checkpoint",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolveTargetDiskName(tt.vmIndex, tt.chain, tt.targetPVCName)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.errorContains) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errorContains)
			}
		})
	}
}

// startFakeBinder mimics the real Kubernetes PV binder, which fake clients
// don't run: once handleInProgress's rebind sets the PV's claimRef to point at
// the target PVC (what the code under test actually writes), this completes
// the bind by setting the PVC's Spec.VolumeName and Status.Phase, polling
// rather than a fixed sleep that could race ahead of the patch. Call site must
// `defer` the returned function immediately, so the goroutine is waited on
// (bounded by timeout) and its status-update error is asserted before the
// test returns -- extracted out of the caller to keep its cyclomatic
// complexity down (gocyclo) as much as to avoid duplicating this boilerplate.
func startFakeBinder(t *testing.T, fakeClient client.Client, scratchPV *corev1.PersistentVolume, targetPVC *corev1.PersistentVolumeClaim, timeout time.Duration) func() {
	t.Helper()
	binderDone := make(chan struct{})
	var binderErr error
	bound := false
	go func() {
		defer close(binderDone)
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			pv := &corev1.PersistentVolume{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: scratchPV.Name}, pv); err != nil {
				binderErr = fmt.Errorf("get PV: %w", err)
				return
			}
			if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == targetPVC.Name && pv.Spec.ClaimRef.Namespace == targetPVC.Namespace {
				pvc := &corev1.PersistentVolumeClaim{}
				if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: targetPVC.Name, Namespace: targetPVC.Namespace}, pvc); err != nil {
					binderErr = fmt.Errorf("get target PVC: %w", err)
					return
				}
				pvc.Spec.VolumeName = scratchPV.Name
				if err := fakeClient.Update(context.Background(), pvc); err != nil {
					binderErr = fmt.Errorf("update target PVC: %w", err)
					return
				}
				pvc.Status.Phase = corev1.ClaimBound
				binderErr = fakeClient.Status().Update(context.Background(), pvc)
				bound = binderErr == nil
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	return func() {
		select {
		case <-binderDone:
			if binderErr != nil {
				t.Errorf("fake-binder goroutine failed: %v", binderErr)
			} else if !bound {
				t.Errorf("fake-binder never observed a claimRef naming target PVC %s/%s", targetPVC.Namespace, targetPVC.Name)
			}
		case <-time.After(timeout + time.Second):
			t.Errorf("timed out waiting for fake-binder goroutine to finish")
		}
	}
}

// ddTestFixture bundles the objects needed to exercise handleAccepted/handlePrepared/
// handleInProgress against a mock object store seeded with a manifest+index.
type ddTestFixture struct {
	dd        *velerov2alpha1.DataDownload
	bsl       *velerov1.BackupStorageLocation
	credSec   *corev1.Secret
	targetPVC *corev1.PersistentVolumeClaim
	mockStore *uploader.MockObjectStore
}

func newDDTestFixture(t *testing.T) *ddTestFixture {
	t.Helper()

	const (
		vmName      = "test-vm"
		vmNamespace = "vm-ns"
		oadpNS      = "openshift-adp"
		backupName  = "backup-001"
		targetPVC   = "restored-disk-1"
		diskName    = "disk1" // KubeVirt volume name -- deliberately different from targetPVC
		restoreNS   = "restore-ns"
	)

	dd := &velerov2alpha1.DataDownload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dd",
			Namespace: oadpNS,
			UID:       types.UID("dd-uid-123"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
			Labels: map[string]string{
				common.LabelVeleroBackupName: backupName,
			},
		},
		Spec: velerov2alpha1.DataDownloadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
			TargetVolume: velerov2alpha1.TargetVolumeSpec{
				PVC:       targetPVC,
				Namespace: restoreNS,
			},
		},
		Status: velerov2alpha1.DataDownloadStatus{Phase: velerov2alpha1.DataDownloadPhaseAccepted},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: oadpNS},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: "test-bucket", Prefix: "velero"},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}

	credSec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-creds", Namespace: oadpNS},
		Data:       map[string][]byte{"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n")},
	}

	targetPVCObj := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: targetPVC, Namespace: restoreNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: new("standard"),
			VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}

	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	checkpointID := "cp-001"
	vmIndex := uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       checkpointID,
				Type:     "full",
				PVCs:     []string{targetPVC},
				PVCSizes: []resource.Quantity{resource.MustParse("10Gi")},
				Files: []uploader.CheckpointFile{
					{
						Filename:   "vmb-" + checkpointID + "-" + diskName + ".qcow2",
						DiskName:   diskName,
						Size:       1024 * 1024 * 1024,
						ObjectPath: "checkpoints/" + vmNamespace + "/" + vmName + "/" + checkpointID + "/vmb-" + checkpointID + "-" + diskName + ".qcow2",
					},
				},
			},
		},
	}
	if err := uploader.PutVMIndex(mockStore, vmNamespace, vmName, "test-bucket", vmIndex); err != nil {
		t.Fatalf("failed to seed VM index: %v", err)
	}

	manifest := uploader.VMBackupManifest{
		Namespace:       vmNamespace,
		Name:            vmName,
		CheckpointChain: []string{checkpointID},
		BackupName:      backupName,
	}
	if err := uploader.PutVMBackupManifest(mockStore, vmNamespace, vmName, backupName, "test-bucket", manifest); err != nil {
		t.Fatalf("failed to seed VM backup manifest: %v", err)
	}

	return &ddTestFixture{dd: dd, bsl: bsl, credSec: credSec, targetPVC: targetPVCObj, mockStore: mockStore}
}

// TestFindScratchPVC_APIReaderFallback covers the informer-cache-lag case: a
// scratch PVC that genuinely exists but hasn't yet propagated to the cached
// client's informer (e.g. created by this same controller moments ago in an
// earlier reconcile) must still be found via the uncached APIReader fallback,
// not misreported as absent -- which would otherwise create a duplicate
// scratch PVC via ensureScratchPVC's GenerateName create.
func TestFindScratchPVC_APIReaderFallback(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}

	t.Run("cached client is empty (cache lag), APIReader fallback finds it", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, scratchPVC.DeepCopy()).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, APIReader: apiReader, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.findScratchPVC(context.Background(), f.dd, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.Name != scratchPVC.Name {
			t.Errorf("expected APIReader fallback to find scratch PVC, got %+v", got)
		}
	})

	t.Run("nil APIReader falls back to cached-only behavior", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.findScratchPVC(context.Background(), f.dd, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil, got %+v", got)
		}
	})
}

// TestListAllScratchPVCs_APIReaderFallback covers the same informer-cache-lag
// case as TestFindScratchPVC_APIReaderFallback, but for listAllScratchPVCs --
// used by handleCanceling's deleteAllScratchPVCs, a terminal path where a
// scratch PVC missed here (cached client hasn't caught up yet) would never
// get cleaned up at all, since Canceled never reconciles again.
func TestListAllScratchPVCs_APIReaderFallback(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}

	t.Run("cached client is empty (cache lag), APIReader fallback finds it", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, scratchPVC.DeepCopy()).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, APIReader: apiReader, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.listAllScratchPVCs(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].Name != scratchPVC.Name {
			t.Errorf("expected APIReader fallback to find scratch PVC, got %+v", got)
		}
	})

	t.Run("nil APIReader falls back to cached-only behavior", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
		r := &KubeVirtDataDownloadReconciler{Client: cached, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		got, err := r.listAllScratchPVCs(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty, got %+v", got)
		}
	})
}

func TestHandleAcceptedDataDownload(t *testing.T) {
	t.Run("target PVC not found requeues without failing", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build() // no targetPVC
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
		}

		result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected requeue when target PVC is missing")
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
			t.Error("missing target PVC should requeue, not fail -- Velero may not have created it yet")
		}
	})

	t.Run("missing manifest fails", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
		emptyStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return emptyStore, nil },
		}

		if _, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
	})

	t.Run("happy path resolves chain, creates scratch PVC, transitions to Prepared", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
		}

		result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected requeue after transitioning to Prepared")
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhasePrepared {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhasePrepared, updated.Status.Message)
		}

		scratchPVC, err := r.findScratchPVC(context.Background(), f.dd, "")
		if err != nil {
			t.Fatalf("failed to find scratch PVC: %v", err)
		}
		if scratchPVC == nil {
			t.Fatal("expected scratch PVC to be created")
		}
		if scratchPVC.Namespace != "openshift-adp" {
			t.Errorf("scratch PVC namespace = %q, want %q", scratchPVC.Namespace, "openshift-adp")
		}
		if scratchPVC.Spec.StorageClassName == nil || *scratchPVC.Spec.StorageClassName != "standard" {
			t.Errorf("scratch PVC storageClassName = %v, want %q", scratchPVC.Spec.StorageClassName, "standard")
		}
		if scratchPVC.Spec.VolumeMode == nil || *scratchPVC.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
			t.Errorf("scratch PVC volumeMode = %v, want %q", scratchPVC.Spec.VolumeMode, corev1.PersistentVolumeFilesystem)
		}
		if got := updated.Annotations[AnnotationTargetDiskName]; got != "disk1" {
			t.Errorf("annotation %s = %q, want %q", AnnotationTargetDiskName, got, "disk1")
		}
	})

	fastFailCases := []struct {
		name         string
		mutate       func(pvc *corev1.PersistentVolumeClaim)
		noScratchMsg string
	}{
		{
			name: "target PVC with spec.selector fails without creating a scratch PVC",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"some-label": "some-value"}}
			},
			noScratchMsg: "expected no scratch PVC to be created for a target PVC with spec.selector set",
		},
		{
			name: "target PVC already bound to a foreign PV fails without creating a scratch PVC",
			mutate: func(pvc *corev1.PersistentVolumeClaim) {
				pvc.Spec.VolumeName = "some-other-pv"
				pvc.Status.Phase = corev1.ClaimBound
			},
			noScratchMsg: "expected no scratch PVC to be created for an already-bound target PVC",
		},
		{
			name:         "target PVC with pending Spec.VolumeName fails without creating a scratch PVC",
			mutate:       func(pvc *corev1.PersistentVolumeClaim) { pvc.Spec.VolumeName = "some-other-pv" },
			noScratchMsg: "expected no scratch PVC to be created for a target PVC with a pending Spec.VolumeName",
		},
	}

	for _, tc := range fastFailCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDDTestFixture(t)
			scheme := ddScheme()
			tc.mutate(f.targetPVC)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
			r := &KubeVirtDataDownloadReconciler{
				Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
				ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
			}

			if _, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var updated velerov2alpha1.DataDownload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
				t.Fatalf("failed to get DataDownload: %v", err)
			}
			if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
				t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed, updated.Status.Message)
			}

			scratchPVC, err := r.findScratchPVC(context.Background(), f.dd, "")
			if err != nil {
				t.Fatalf("failed to check for scratch PVC: %v", err)
			}
			if scratchPVC != nil {
				t.Error(tc.noScratchMsg)
			}
		})
	}
}

// TestHandleAcceptedDataDownload_BlockMode covers a Block-mode restore target:
// handleAccepted must provision both a Filesystem-mode work PVC (staging the
// qcow2 chain) and a Block-mode output PVC (sized to target capacity, holding
// the final flattened image) instead of the single Filesystem-mode scratch
// PVC a Filesystem-mode target uses. Split out from TestHandleAcceptedDataDownload
// to keep that function's cyclomatic complexity down (gocyclo).
func TestHandleAcceptedDataDownload_BlockMode(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	f.targetPVC.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, f.targetPVC).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		ObjectStoreFactory: func(_ *common.ObjectStoreConfig) (velero.ObjectStore, error) { return f.mockStore, nil },
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue after transitioning to Prepared")
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhasePrepared {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhasePrepared, updated.Status.Message)
	}
	if got := updated.Annotations[AnnotationRestoreBlockMode]; got != "true" {
		t.Errorf("annotation %s = %q, want %q", AnnotationRestoreBlockMode, got, "true")
	}

	workPVC, err := r.findScratchPVC(context.Background(), f.dd, common.ScratchVolumeRoleWork)
	if err != nil {
		t.Fatalf("failed to find work PVC: %v", err)
	}
	if workPVC == nil {
		t.Fatal("expected work PVC to be created")
	}
	if workPVC.Spec.VolumeMode == nil || *workPVC.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		t.Errorf("work PVC volumeMode = %v, want %q", workPVC.Spec.VolumeMode, corev1.PersistentVolumeFilesystem)
	}

	outputPVC, err := r.findScratchPVC(context.Background(), f.dd, common.ScratchVolumeRoleOutput)
	if err != nil {
		t.Fatalf("failed to find output PVC: %v", err)
	}
	if outputPVC == nil {
		t.Fatal("expected output PVC to be created")
	}
	if outputPVC.Spec.VolumeMode == nil || *outputPVC.Spec.VolumeMode != corev1.PersistentVolumeBlock {
		t.Errorf("output PVC volumeMode = %v, want %q", outputPVC.Spec.VolumeMode, corev1.PersistentVolumeBlock)
	}
	if outputPVC.Spec.StorageClassName == nil || *outputPVC.Spec.StorageClassName != "standard" {
		t.Errorf("output PVC storageClassName = %v, want %q", outputPVC.Spec.StorageClassName, "standard")
	}
}

func TestHandlePreparedDataDownload(t *testing.T) {
	t.Run("existing pod transitions to InProgress without creating a new one", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		existingPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "existing-downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, existingPod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 1 {
			t.Errorf("expected exactly 1 pod (no new one created), got %d", len(pods.Items))
		}
	})

	t.Run("happy path creates downloader pod and transitions to InProgress", func(t *testing.T) {
		f := newDDTestFixture(t)
		// handlePrepared reads the disk name resolved by handleAccepted in an
		// earlier reconcile; set it explicitly since this test calls handlePrepared
		// directly, bypassing Accepted.
		f.dd.Annotations[AnnotationTargetDiskName] = "disk1"
		scheme := ddScheme()
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, scratchPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest",
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress, updated.Status.Message)
		}

		pod, err := r.findPodForDataDownload(context.Background(), f.dd, "openshift-adp")
		if err != nil {
			t.Fatalf("failed to find pod: %v", err)
		}
		if pod == nil {
			t.Fatal("expected downloader pod to be created")
		}
		if pod.Labels[common.LabelDatamoverPod] != "download" {
			t.Errorf("pod label %s = %q, want %q", common.LabelDatamoverPod, pod.Labels[common.LabelDatamoverPod], "download")
		}
		if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Name != f.dd.Name {
			t.Errorf("expected owner reference to DataDownload %q, got %+v", f.dd.Name, pod.OwnerReferences)
		}

		// The pod's target-volume env var must carry the resolved KubeVirt disk
		// name ("disk1"), not the target PVC name ("restored-disk-1") -- these
		// deliberately differ in this fixture to prove resolveTargetDiskName's
		// translation is what actually reaches the pod, not the raw PVC name.
		var targetVolumeEnv string
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == downloader.EnvTargetVolume {
				targetVolumeEnv = env.Value
				break
			}
		}
		if targetVolumeEnv != "disk1" {
			t.Errorf("env %s = %q, want %q", downloader.EnvTargetVolume, targetVolumeEnv, "disk1")
		}
		if targetVolumeEnv == "restored-disk-1" {
			t.Error("env carries the target PVC name instead of the resolved disk name")
		}
	})

	t.Run("missing target disk name annotation fails without creating a pod", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, scratchPVC).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest",
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed, updated.Status.Message)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 0 {
			t.Errorf("expected no pod to be created, got %d", len(pods.Items))
		}
	})

	t.Run("missing scratch PVC fails without creating a pod", func(t *testing.T) {
		f := newDDTestFixture(t)
		f.dd.Annotations[AnnotationTargetDiskName] = "disk1"
		scheme := ddScheme()
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build()
		r := &KubeVirtDataDownloadReconciler{
			Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
			DatamoverImage: "quay.io/test/datamover:latest",
		}

		if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed, updated.Status.Message)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 0 {
			t.Errorf("expected no pod to be created, got %d", len(pods.Items))
		}
	})
}

// TestHandlePreparedDataDownload_BlockMode covers a Block-mode restore target:
// the downloader pod must mount the work PVC as a filesystem volume (staging
// the qcow2 chain) and the output PVC as a raw volumeDevice (receiving the
// final flattened image), with KUBEVIRT_DM_TARGET_IS_BLOCK_DEVICE set so the
// downloader knows which publish path to take. Split out from
// TestHandlePreparedDataDownload to keep that function's cyclomatic
// complexity down (gocyclo).
func TestHandlePreparedDataDownload_BlockMode(t *testing.T) {
	f := newDDTestFixture(t)
	f.dd.Annotations[AnnotationTargetDiskName] = "disk1"
	f.dd.Annotations[AnnotationRestoreBlockMode] = "true"
	scheme := ddScheme()
	workPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "work-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
			},
		},
	}
	outputPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "output-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleOutput,
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec, workPVC, outputPVC).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		DatamoverImage: "quay.io/test/datamover:latest",
	}

	if _, err := r.handlePrepared(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseInProgress {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseInProgress, updated.Status.Message)
	}

	pod, err := r.findPodForDataDownload(context.Background(), f.dd, "openshift-adp")
	if err != nil {
		t.Fatalf("failed to find pod: %v", err)
	}
	if pod == nil {
		t.Fatal("expected downloader pod to be created")
	}
	if len(pod.Spec.Containers[0].VolumeDevices) != 1 {
		t.Fatalf("expected exactly 1 volumeDevice, got %d", len(pod.Spec.Containers[0].VolumeDevices))
	}
	if pod.Spec.Containers[0].VolumeDevices[0].Name != restoreOutputVolumeName {
		t.Errorf("volumeDevice name = %q, want %q", pod.Spec.Containers[0].VolumeDevices[0].Name, restoreOutputVolumeName)
	}

	var foundWorkVolume, foundOutputVolume bool
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim == nil {
			continue
		}
		switch v.PersistentVolumeClaim.ClaimName {
		case "work-pvc-1":
			foundWorkVolume = true
		case "output-pvc-1":
			foundOutputVolume = true
		}
	}
	if !foundWorkVolume {
		t.Error("expected pod to have a volume sourced from the work PVC")
	}
	if !foundOutputVolume {
		t.Error("expected pod to have a volume sourced from the output PVC")
	}

	var isBlockDeviceEnv string
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == downloader.EnvTargetIsBlockDevice {
			isBlockDeviceEnv = env.Value
			break
		}
	}
	if isBlockDeviceEnv != "true" {
		t.Errorf("env %s = %q, want %q", downloader.EnvTargetIsBlockDevice, isBlockDeviceEnv, "true")
	}
}

func TestHandleInProgressDataDownload(t *testing.T) {
	t.Run("already provisioned in a prior reconcile completes idempotently without a pod", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()

		// Simulates: a prior reconcile's rebindPVToNamespace succeeded (scratch PVC
		// deleted, PV's claimRef patched to point at the target PVC, PV labeled
		// with our UID) but the subsequent updatePhase(Completed) failed to
		// persist, so this reconcile starts from InProgress again with no pod and
		// no scratch PVC left. The target PVC's own Status.Phase is deliberately
		// left Pending (not Bound) to prove isRestoreAlreadyProvisioned detects
		// the committed rebind via the PV's claimRef, not the PVC's Bound status
		// (which the Kubernetes PV controller only sets asynchronously). Both
		// UIDs are set to the same real, non-empty value so this exercises the
		// actual claimRef-UID-vs-live-PVC-UID comparison, not the empty-UID
		// legacy fallback (see the mismatch test below for that branch).
		f.targetPVC.UID = types.UID("target-pvc-uid")
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-already-rebound",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Kind:      "PersistentVolumeClaim",
					Name:      f.targetPVC.Name,
					Namespace: f.targetPVC.Namespace,
					UID:       f.targetPVC.UID,
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, reboundPV).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Errorf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
		}
	})

	t.Run("claimRef UID mismatch does not count as already provisioned", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()

		// The claimRef names the right PVC/namespace, but a different UID -- as if
		// the target PVC had been deleted and recreated after this PV's claimRef was
		// set. isRestoreAlreadyProvisioned must not treat this as the same restore.
		reboundPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "pv-already-rebound",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Kind:      "PersistentVolumeClaim",
					Name:      f.targetPVC.Name,
					Namespace: f.targetPVC.Namespace,
					UID:       types.UID("some-other-uid"),
				},
			},
		}
		f.targetPVC.UID = types.UID("actual-target-pvc-uid")

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, reboundPV).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		done, err := r.isRestoreAlreadyProvisioned(context.Background(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if done {
			t.Error("expected isRestoreAlreadyProvisioned = false for a claimRef UID mismatch")
		}
	})

	t.Run("pod failed transitions to Failed with message", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
				ContainerStatuses: []corev1.ContainerStatus{
					{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: "qemu-img failed"}}},
				},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var updated velerov2alpha1.DataDownload
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseFailed {
			t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseFailed)
		}
		if updated.Status.Message == "" {
			t.Error("expected non-empty failure message")
		}
	})

	t.Run("pod running requeues", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod).Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		result, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.RequeueAfter == 0 {
			t.Error("expected requeue while pod is running")
		}
	})

	t.Run("pod succeeded rebinds scratch volume to target PVC and completes", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()

		// Target PVC starts Pending/unbound (the realistic Velero-created placeholder
		// state) so this test exercises the real validateExistingPVCForBind mutation path
		// (retain, delete scratch PVC, patch claimRef, wait for bound) rather than
		// the already-bound idempotent short-circuit.
		origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
		pvRebindPollInterval = 10 * time.Millisecond
		pvRebindTimeout = 2 * time.Second
		defer func() {
			pvRebindPollInterval = origInterval
			pvRebindTimeout = origTimeout
		}()

		scratchPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				StorageClassName:              "standard",
				VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
			},
		}
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-scratch",
				StorageClassName: new("standard"),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "downloader-pod", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC, pod).
			Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		// Fake clients don't run the real Kubernetes PV binder, so simulate it.
		defer startFakeBinder(t, fakeClient, scratchPV, f.targetPVC, pvRebindTimeout)()

		if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
		}
		if updated.Annotations[AnnotationDownloaderPodSucceeded] != "true" {
			t.Errorf("expected %s annotation to be persisted before the pod delete", AnnotationDownloaderPodSucceeded)
		}

		var pv corev1.PersistentVolume
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pv-scratch"}, &pv); err != nil {
			t.Fatalf("failed to get PV: %v", err)
		}
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != f.targetPVC.Name || pv.Spec.ClaimRef.Namespace != f.targetPVC.Namespace {
			t.Errorf("PV claimRef = %+v, want %s/%s", pv.Spec.ClaimRef, f.targetPVC.Namespace, f.targetPVC.Name)
		}

		var pods corev1.PodList
		_ = fakeClient.List(context.Background(), &pods)
		if len(pods.Items) != 0 {
			t.Errorf("expected downloader pod to be cleaned up after completion, found %d", len(pods.Items))
		}
	})

	t.Run("pod absent with success marker resumes rebind and completes", func(t *testing.T) {
		f := newDDTestFixture(t)
		scheme := ddScheme()

		// Same rebind fixtures as the pod-succeeded case above, but the pod is
		// already gone and the DataDownload carries the success marker -- the state
		// a retry reconcile sees after a prior pass persisted the marker, deleted
		// the pod, then errored on the strict still-terminating cleanup check. The
		// pod's absence must resume the rebind, not fail as "Downloader pod not found".
		origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
		pvRebindPollInterval = 10 * time.Millisecond
		pvRebindTimeout = 2 * time.Second
		defer func() {
			pvRebindPollInterval = origInterval
			pvRebindTimeout = origTimeout
		}()

		if f.dd.Annotations == nil {
			f.dd.Annotations = map[string]string{}
		}
		f.dd.Annotations[AnnotationDownloaderPodSucceeded] = "true"

		scratchPV := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				StorageClassName:              "standard",
				VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
			},
		}
		scratchPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "scratch-pvc-1", Namespace: "openshift-adp",
				Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				VolumeName:       "pv-scratch",
				StorageClassName: new("standard"),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC).
			Build()
		r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

		defer startFakeBinder(t, fakeClient, scratchPV, f.targetPVC, pvRebindTimeout)()

		if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var updated velerov2alpha1.DataDownload
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
			t.Fatalf("failed to get DataDownload: %v", err)
		}
		if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
			t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
		}

		var pv corev1.PersistentVolume
		if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "pv-scratch"}, &pv); err != nil {
			t.Fatalf("failed to get PV: %v", err)
		}
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != f.targetPVC.Name || pv.Spec.ClaimRef.Namespace != f.targetPVC.Namespace {
			t.Errorf("PV claimRef = %+v, want %s/%s", pv.Spec.ClaimRef, f.targetPVC.Namespace, f.targetPVC.Name)
		}
	})
}

// TestCompleteSuccessfulDownload_EmitsRetainedPVEvent covers #152: since the
// rebound PV is intentionally left in the Retain policy forever (restored user
// data must survive deletion of the target PVC), there's no other operational
// signal pointing an operator at it -- completeSuccessfulDownload must emit an
// Event so it's discoverable without already knowing to look.
func TestCompleteSuccessfulDownload_EmitsRetainedPVEvent(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	scratchPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC).Build()
	recorder := record.NewFakeRecorder(10)
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, EventRecorder: recorder, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	defer startFakeBinder(t, fakeClient, scratchPV, f.targetPVC, pvRebindTimeout)()

	if _, err := r.completeSuccessfulDownload(context.Background(), logr.Discard(), f.dd, "openshift-adp"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "PVLeftInRetainPolicy") {
			t.Errorf("event = %q, want it to reference PVLeftInRetainPolicy", event)
		}
	default:
		t.Error("expected an Event to be recorded for the retained PV, got none")
	}
}

// TestCompleteSuccessfulDownload_BlockMode_RebindsOutputPVCAndDeletesWorkPVC
// covers the Block-mode restore path: only the output PVC/PV (which holds the
// final flattened raw image) gets rebound onto the target -- the work PVC
// (which only ever staged the qcow2 chain) must be deleted afterward rather
// than left behind or rebound itself.
func TestCompleteSuccessfulDownload_BlockMode_RebindsOutputPVCAndDeletesWorkPVC(t *testing.T) {
	f := newDDTestFixture(t)
	f.dd.Annotations[AnnotationRestoreBlockMode] = "true"
	f.targetPVC.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
	scheme := ddScheme()

	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	outputPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-output"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeBlock),
		},
	}
	outputPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "output-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleOutput,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-output",
			StorageClassName: new("standard"),
			VolumeMode:       new(corev1.PersistentVolumeBlock),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	workPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "work-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.targetPVC, outputPV, outputPVC, workPVC).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	defer startFakeBinder(t, fakeClient, outputPV, f.targetPVC, pvRebindTimeout)()

	if _, err := r.completeSuccessfulDownload(context.Background(), logr.Discard(), f.dd, "openshift-adp"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCompleted {
		t.Fatalf("phase = %q, want %q (message: %s)", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCompleted, updated.Status.Message)
	}

	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: workPVC.Name, Namespace: workPVC.Namespace}, &corev1.PersistentVolumeClaim{}); !errors.IsNotFound(err) {
		t.Errorf("expected work PVC to be deleted after a successful Block-mode restore, get returned: %v", err)
	}
}

// TestEmitPodLogsDataDownload_TruncatesLongOutput covers #154: a downloader
// pod that produced a very large log (e.g. a crash loop with verbose output)
// must not flood the controller's own logs unbounded -- only the last
// maxEmittedPodLogLines lines are kept, with a truncation notice recording how
// many were dropped.
func TestEmitPodLogsDataDownload_TruncatesLongOutput(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "downloader-test-pod", Namespace: "openshift-adp"}}

	const totalLines = maxEmittedPodLogLines + 50
	var sb strings.Builder
	for i := 1; i <= totalLines; i++ {
		fmt.Fprintf(&sb, "line-%d\n", i)
	}

	var logBuf bytes.Buffer
	logger := funcr.New(func(prefix, args string) {
		logBuf.WriteString(args + "\n")
	}, funcr.Options{})

	r := &KubeVirtDataDownloadReconciler{
		PodLogCollector: func(ctx context.Context, podName, podNamespace string) (string, error) {
			return sb.String(), nil
		},
	}

	r.emitPodLogs(context.Background(), logger, pod)

	output := logBuf.String()
	if got := strings.Count(output, "\"message\"=\"line-"); got != maxEmittedPodLogLines {
		t.Errorf("expected %d emitted lines, got %d", maxEmittedPodLogLines, got)
	}
	if !strings.Contains(output, "Downloader pod log truncated") {
		t.Error("expected a truncation notice, found none")
	}
	if !strings.Contains(output, fmt.Sprintf("\"skippedLeadingLines\"=%d", totalLines-maxEmittedPodLogLines)) {
		t.Errorf("expected skippedLeadingLines=%d in the truncation notice, output: %s", totalLines-maxEmittedPodLogLines, output)
	}
	if strings.Contains(output, "\"message\"=\"line-1\"") {
		t.Error("expected the earliest lines to be dropped, but line-1 was emitted")
	}
	if !strings.Contains(output, fmt.Sprintf("\"message\"=\"line-%d\"", totalLines)) {
		t.Errorf("expected the final line (line-%d) to be kept", totalLines)
	}
}

// TestHandleInProgressDataDownload_PodSucceededDeleteFailureDoesNotPersistCompleted
// covers the same terminal-phase contract as the Canceling-path tests: once the
// downloader pod succeeds, cleanupPodsByUID's failure must block the rebind and
// the Completed transition (returning an error so the reconcile retries) rather
// than proceeding into rebindPVToNamespace's PVC-delete-and-wait with the pod
// object still present, which would just deadlock behind the pvc-protection
// finalizer. Split out from TestHandleInProgressDataDownload to keep that
// function's cyclomatic complexity down (gocyclo).
func TestHandleInProgressDataDownload_PodSucceededDeleteFailureDoesNotPersistCompleted(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()

	scratchPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(f.dd, f.targetPVC, scratchPV, scratchPVC, pod).
		Build()
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				return fmt.Errorf("simulated delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r := &KubeVirtDataDownloadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd); err == nil {
		t.Fatal("expected an error when the downloader pod can't be deleted, got nil")
	}

	var updated velerov2alpha1.DataDownload
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseCompleted {
		t.Error("phase must not be Completed until the downloader pod is actually gone")
	}
	if updated.Annotations[AnnotationDownloaderPodSucceeded] != downloaderPodSucceededValue {
		t.Errorf("expected %s annotation to be persisted before the failed delete attempt", AnnotationDownloaderPodSucceeded)
	}

	var pv corev1.PersistentVolume
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: "pv-scratch"}, &pv); err != nil {
		t.Fatalf("failed to get PV: %v", err)
	}
	if pv.Spec.ClaimRef != nil {
		t.Errorf("expected scratch PV claimRef to remain unset (rebind must not run before pod cleanup succeeds), got %+v", pv.Spec.ClaimRef)
	}
}

// TestHandleInProgressDataDownload_PodNotFoundRequeuesInsteadOfFailing covers
// the pod-absent branch when no success marker is present: a pod handlePrepared
// just created may not yet be visible to this reconcile's cached client
// (informer propagation isn't synchronous with the create call), so this must
// requeue and let Spec.OperationTimeout bound how long a genuinely vanished pod
// goes undetected, rather than failing on what's often just a transient cache
// miss. Split out from TestHandleInProgressDataDownload to keep that function's
// cyclomatic complexity down (gocyclo).
func TestHandleInProgressDataDownload_PodNotFoundRequeuesInsteadOfFailing(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), f.dd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected a requeue when the downloader pod isn't found yet")
	}
	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseFailed {
		t.Errorf("phase = %q, want it to stay non-terminal (a missing pod alone isn't proof it's gone for good)", updated.Status.Phase)
	}
}

func TestHandleCancelingDataDownload(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod, scratchPVC).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCanceled {
		t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCanceled)
	}

	var pods corev1.PodList
	_ = fakeClient.List(context.Background(), &pods)
	if len(pods.Items) != 0 {
		t.Errorf("expected pod to be cleaned up, found %d", len(pods.Items))
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = fakeClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 0 {
		t.Errorf("expected scratch PVC to be cleaned up, found %d", len(pvcs.Items))
	}
}

// TestHandleCancelingDataDownload_BlockMode_CleansUpBothWorkAndOutputPVCs
// covers cancellation of a Block-mode restore, which provisioned two scratch
// PVCs instead of one -- both the work and output PVCs must be cleaned up,
// not just whichever one an UID-label-only lookup happens to find first.
func TestHandleCancelingDataDownload_BlockMode_CleansUpBothWorkAndOutputPVCs(t *testing.T) {
	f := newDDTestFixture(t)
	f.dd.Annotations[AnnotationRestoreBlockMode] = "true"
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	workPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "work-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleWork,
			},
		},
	}
	outputPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "output-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataDownloadUID:   string(f.dd.UID),
				common.LabelScratchVolumeRole: common.ScratchVolumeRoleOutput,
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod, workPVC, outputPVC).Build()
	r := &KubeVirtDataDownloadReconciler{Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated velerov2alpha1.DataDownload
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated)
	if updated.Status.Phase != velerov2alpha1.DataDownloadPhaseCanceled {
		t.Errorf("phase = %q, want %q", updated.Status.Phase, velerov2alpha1.DataDownloadPhaseCanceled)
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = fakeClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 0 {
		t.Errorf("expected both work and output PVCs to be cleaned up, found %d", len(pvcs.Items))
	}
}

// TestHandleCancelingDataDownload_PodCleanupFailureDoesNotPersistCanceled
// covers a case Canceled being terminal makes important: if the pod can't be
// stopped, handleCanceling must return an error (so it retries) rather than
// deleting the scratch PVC and persisting Canceled anyway -- once Canceled
// persists, no further reconciliation ever runs for this object, so a still-
// running pod (and, if the scratch PVC delete raced ahead of it, a PVC wedged
// in Terminating behind the pvc-protection finalizer) would be abandoned
// forever with no chance to retry.
func TestHandleCancelingDataDownload_PodCleanupFailureDoesNotPersistCanceled(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "downloader-pod", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, pod, scratchPVC).Build()
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.Pod); ok {
				return fmt.Errorf("simulated delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r := &KubeVirtDataDownloadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err == nil {
		t.Fatal("expected an error when the downloader pod can't be deleted, got nil")
	}

	var updated velerov2alpha1.DataDownload
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseCanceled {
		t.Error("phase must not be Canceled until pod cleanup actually succeeds")
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = baseClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 1 {
		t.Errorf("expected scratch PVC to be left alone (pod cleanup failed first), found %d", len(pvcs.Items))
	}
}

// TestHandleCancelingDataDownload_ScratchPVCDeleteFailureDoesNotPersistCanceled
// covers the same terminal-phase contract as the pod-cleanup-failure test
// above, but for the scratch PVC delete step: Canceled is terminal (no further
// reconciliation ever runs for this object once it persists), so a swallowed
// scratch PVC delete failure would leak it forever with nothing left to retry
// the delete -- deleteAllScratchPVCs must propagate the failure instead of the
// best-effort cleanupScratchPVCIfPresent used by other, non-terminal paths.
func TestHandleCancelingDataDownload_ScratchPVCDeleteFailureDoesNotPersistCanceled(t *testing.T) {
	f := newDDTestFixture(t)
	scheme := ddScheme()
	scratchPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "scratch-pvc-1", Namespace: "openshift-adp",
			Labels: map[string]string{common.LabelDataDownloadUID: string(f.dd.UID)},
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, scratchPVC).Build()
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				return fmt.Errorf("simulated delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	r := &KubeVirtDataDownloadReconciler{Client: interceptedClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp"}

	if _, err := r.handleCanceling(context.Background(), logr.Discard(), f.dd); err == nil {
		t.Fatal("expected an error when the scratch PVC can't be deleted, got nil")
	}

	var updated velerov2alpha1.DataDownload
	if err := baseClient.Get(context.Background(), types.NamespacedName{Name: f.dd.Name, Namespace: f.dd.Namespace}, &updated); err != nil {
		t.Fatalf("failed to get DataDownload: %v", err)
	}
	if updated.Status.Phase == velerov2alpha1.DataDownloadPhaseCanceled {
		t.Error("phase must not be Canceled until scratch PVC cleanup actually succeeds")
	}

	var pvcs corev1.PersistentVolumeClaimList
	_ = baseClient.List(context.Background(), &pvcs)
	if len(pvcs.Items) != 1 {
		t.Errorf("expected scratch PVC to still exist (delete failed), found %d", len(pvcs.Items))
	}
}

func TestBuildDownloaderPodConfig(t *testing.T) {
	f := newDDTestFixture(t)
	f.bsl.Spec.Provider = "azure"
	f.bsl.Spec.Config = map[string]string{
		"resourceGroup":               "my-rg",
		"storageAccount":              "my-account",
		"storageAccountKeyEnvVar":     "AZURE_STORAGE_KEY",
		"storageAccountURI":           "https://my-account.blob.core.windows.net",
		"subscriptionId":              "sub-id",
		"useAAD":                      "true",
		"activeDirectoryAuthorityURI": "https://login.microsoftonline.com",
	}
	scheme := ddScheme()
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.dd, f.bsl, f.credSec).Build()
	r := &KubeVirtDataDownloadReconciler{
		Client: fakeClient, Scheme: scheme, Log: logr.Discard(), OADPNamespace: "openshift-adp",
		DatamoverImage: "quay.io/test/datamover:latest",
	}
	vmRef := &common.VMReference{Name: "test-vm", Namespace: "vm-ns"}

	cfg, err := r.buildDownloaderPodConfig(f.dd, f.bsl, vmRef, "scratch-pvc-1", "", "disk1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.ScratchPVCName != "scratch-pvc-1" {
		t.Errorf("ScratchPVCName = %q, want %q", cfg.ScratchPVCName, "scratch-pvc-1")
	}
	if cfg.TargetVolume != "disk1" {
		t.Errorf("TargetVolume = %q, want %q", cfg.TargetVolume, "disk1")
	}
	if cfg.BSLResourceGroup != "my-rg" {
		t.Errorf("BSLResourceGroup = %q, want %q", cfg.BSLResourceGroup, "my-rg")
	}
	if cfg.BSLStorageAccount != "my-account" {
		t.Errorf("BSLStorageAccount = %q, want %q", cfg.BSLStorageAccount, "my-account")
	}
	if cfg.BSLStorageAccountKeyEnvVar != "AZURE_STORAGE_KEY" {
		t.Errorf("BSLStorageAccountKeyEnvVar = %q, want %q", cfg.BSLStorageAccountKeyEnvVar, "AZURE_STORAGE_KEY")
	}
	if cfg.BSLStorageAccountURI != "https://my-account.blob.core.windows.net" {
		t.Errorf("BSLStorageAccountURI = %q, want %q", cfg.BSLStorageAccountURI, "https://my-account.blob.core.windows.net")
	}
	if cfg.BSLSubscriptionID != "sub-id" {
		t.Errorf("BSLSubscriptionID = %q, want %q", cfg.BSLSubscriptionID, "sub-id")
	}
	if cfg.BSLUseAAD != "true" {
		t.Errorf("BSLUseAAD = %q, want %q", cfg.BSLUseAAD, "true")
	}
	if cfg.BSLActiveDirectoryAuthorityURI != "https://login.microsoftonline.com" {
		t.Errorf("BSLActiveDirectoryAuthorityURI = %q, want %q", cfg.BSLActiveDirectoryAuthorityURI, "https://login.microsoftonline.com")
	}
}
