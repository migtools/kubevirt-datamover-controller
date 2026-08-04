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
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestOriginalReclaimPolicyOf(t *testing.T) {
	tests := []struct {
		name string
		pv   *corev1.PersistentVolume
		want corev1.PersistentVolumeReclaimPolicy
	}{
		{
			name: "not yet Retain -- spec value is authoritative regardless of annotation",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete},
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					pvOriginalReclaimPolicyAnnotation: string(corev1.PersistentVolumeReclaimRecycle),
				}},
			},
			want: corev1.PersistentVolumeReclaimDelete,
		},
		{
			name: "Retain with recorded annotation returns the recorded original",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain},
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					pvOriginalReclaimPolicyAnnotation: string(corev1.PersistentVolumeReclaimDelete),
				}},
			},
			want: corev1.PersistentVolumeReclaimDelete,
		},
		{
			name: "Retain with no annotation falls back to the (already-Retain) spec value",
			pv: &corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain},
			},
			want: corev1.PersistentVolumeReclaimRetain,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := originalReclaimPolicyOf(tt.pv); got != tt.want {
				t.Errorf("originalReclaimPolicyOf() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateExistingPVCForBind(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	basePV := func(mutate func(*corev1.PersistentVolume)) *corev1.PersistentVolume {
		pv := &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pv-1"},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: "standard",
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			},
		}
		if mutate != nil {
			mutate(pv)
		}
		return pv
	}

	basePVC := func(mutate func(*corev1.PersistentVolumeClaim)) *corev1.PersistentVolumeClaim {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "target-pvc", Namespace: "restore-ns"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				StorageClassName: new("standard"),
				VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimPending,
			},
		}
		if mutate != nil {
			mutate(pvc)
		}
		return pvc
	}

	tests := []struct {
		name          string
		pv            *corev1.PersistentVolume
		pvc           *corev1.PersistentVolumeClaim
		skipCreatePVC bool
		expectError   bool
		errorContains string
	}{
		{
			name:          "target PVC not found",
			pv:            basePV(nil),
			skipCreatePVC: true,
			expectError:   true,
			errorContains: "not found",
		},
		{
			name: "already bound to this PV short-circuits without spec validation",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Status.Phase = corev1.ClaimBound
				p.Spec.VolumeName = "pv-1"
				// Deliberately incompatible spec -- should not matter since already bound to pv-1.
				p.Spec.StorageClassName = new("mismatched")
			}),
			expectError: false,
		},
		{
			name: "already bound to a different PV fails",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Status.Phase = corev1.ClaimBound
				p.Spec.VolumeName = "some-other-pv"
			}),
			expectError:   true,
			errorContains: "already bound to PV",
		},
		{
			name: "pending PVC already requests a conflicting volume name",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Status.Phase = corev1.ClaimPending
				p.Spec.VolumeName = "some-other-pv"
			}),
			expectError:   true,
			errorContains: "already requests volume",
		},
		{
			name: "storageClassName mismatch",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.StorageClassName = new("other-class")
			}),
			expectError:   true,
			errorContains: "storageClassName",
		},
		{
			name: "requested capacity exceeds PV capacity",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
			}),
			expectError:   true,
			errorContains: "exceeds",
		},
		{
			name: "volumeMode mismatch",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.VolumeMode = new(corev1.PersistentVolumeBlock)
			}),
			expectError:   true,
			errorContains: "volumeMode",
		},
		{
			name: "access modes disjoint",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
			}),
			expectError:   true,
			errorContains: "access mode",
		},
		{
			name: "selector does not match PV labels",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				p.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"some-label": "some-value"}}
			}),
			expectError:   true,
			errorContains: "selector",
		},
		{
			name: "target PVC being deleted fails",
			pv:   basePV(nil),
			pvc: basePVC(func(p *corev1.PersistentVolumeClaim) {
				now := metav1.Now()
				p.DeletionTimestamp = &now
				p.Finalizers = []string{"kubernetes.io/pvc-protection"}
			}),
			expectError:   true,
			errorContains: "being deleted",
		},
		{
			name:        "compatible PVC and PV",
			pv:          basePV(nil),
			pvc:         basePVC(nil),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if !tt.skipCreatePVC && tt.pvc != nil {
				builder = builder.WithObjects(tt.pvc)
			}
			fakeClient := builder.Build()

			result, err := validateExistingPVCForBind(context.Background(), fakeClient, logr.Discard(), tt.pv, "restore-ns", "target-pvc")

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("error = %q, want to contain %q", err.Error(), tt.errorContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result == nil {
				t.Fatal("expected non-nil PVC result")
			}
			if result.Name != "target-pvc" {
				t.Errorf("result.Name = %q, want %q", result.Name, "target-pvc")
			}
		})
	}
}

// TestCreateNewBoundPVC_AlreadyExistsReturnsRealUID covers the BindTargetCreate
// race where two invocations (e.g. two reconciles) generate the same auto-named
// PVC and the second one's Create call fails with AlreadyExists. Create leaves
// the local object's ObjectMeta untouched on failure, so without re-fetching,
// the caller would patch the PV's claimRef.UID from an empty string instead of
// the real, already-existing PVC's UID -- silently dropping the UID-based
// binding safety check for whatever created it first.
func TestCreateNewBoundPVC_AlreadyExistsReturnsRealUID(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "source-pvc", Namespace: "vm-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: new("standard"),
			VolumeMode:       new(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}
	existingName := common.SafeResourceName(common.ReboundPVCNamePrefix, "test-du")
	existing := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: existingName, Namespace: "oadp-ns", UID: types.UID("real-server-assigned-uid"),
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	result, err := createNewBoundPVC(
		context.Background(), fakeClient, logr.Discard(),
		sourcePVC, "oadp-ns", "test-du", "du-uid-123",
		"velero.io/dataupload-uid", "velero.io/dataupload-name", "pv-scratch",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.UID != existing.UID {
		t.Errorf("UID = %q, want the already-existing PVC's real UID %q (patchPVBinding would otherwise set claimRef.UID to empty)",
			result.UID, existing.UID)
	}
}

// TestRebindPVToNamespace_AlreadyBoundIdempotentShortCircuit covers
// rebindPVToNamespace's own Step 1.5 idempotency check for BindTargetExisting:
// if the PV's claimRef already names the target PVC (a prior invocation
// already completed the rebind), it must short-circuit without repeating
// Steps 2-6, and clean up the now-leftover source PVC rather than leaving it
// dangling. This is distinct from TestValidateExistingPVCForBind's own
// "already bound" case, which only covers validateExistingPVCForBind in
// isolation, not this full-rebind-level short-circuit.
func TestRebindPVToNamespace_AlreadyBoundIdempotentShortCircuit(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	sourcePV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
			// claimRef already names the target -- a prior invocation's Step 5
			// (patch claimRef) already ran.
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Name:      "restored-disk",
				Namespace: "restore-ns",
			},
		},
	}
	// Still present -- as if a prior invocation's Step 3 (delete source PVC)
	// never ran or didn't finish. rebindPVToNamespace must clean this up.
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "scratch-pvc", Namespace: "oadp-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	targetPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-disk", Namespace: "restore-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
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
		WithObjects(sourcePV, sourcePVC, targetPVC).
		Build()

	result, err := rebindPVToNamespace(
		context.Background(), fakeClient, logr.Discard(),
		sourcePVC.Name, sourcePVC.Namespace, targetPVC.Namespace,
		"test-dd", "uid-123",
		"velero.io/datadownload-uid", "velero.io/datadownload-name",
		BindTargetExisting, targetPVC.Name,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NewPVCName != targetPVC.Name || result.NewPVCNamespace != targetPVC.Namespace || result.PVName != sourcePV.Name {
		t.Errorf("result = %+v, want NewPVCName=%q NewPVCNamespace=%q PVName=%q",
			result, targetPVC.Name, targetPVC.Namespace, sourcePV.Name)
	}

	// The leftover source PVC must have been cleaned up.
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: sourcePVC.Name, Namespace: sourcePVC.Namespace}, &corev1.PersistentVolumeClaim{})
	if !errors.IsNotFound(err) {
		t.Errorf("expected leftover source PVC to be deleted, get returned: %v", err)
	}

	// sourcePV started with no UID label (simulating a prior invocation whose
	// patch somehow didn't carry it) -- the short-circuit must self-heal it,
	// since isRestoreAlreadyProvisioned and the Cancel/timeout-vs-provisioned
	// guards all find this PV by that label.
	var pv corev1.PersistentVolume
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: sourcePV.Name}, &pv); err != nil {
		t.Fatalf("failed to get PV: %v", err)
	}
	if pv.Labels["velero.io/datadownload-uid"] != "uid-123" {
		t.Errorf("expected PV to carry the UID label after idempotent short-circuit, labels = %v", pv.Labels)
	}
}

// TestRebindPVToNamespace_EmptyExistingPVCName tests validation of an empty
// existingPVCName.
func TestRebindPVToNamespace_EmptyExistingPVCName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	_, err := rebindPVToNamespace(
		context.Background(), fakeClient, logr.Discard(),
		"source-pvc", "oadp-ns", "restore-ns",
		"test-dd", "uid-123",
		"velero.io/datadownload-uid", "velero.io/datadownload-name",
		BindTargetExisting, "",
	)
	if err == nil {
		t.Fatal("expected error for empty existingPVCName, got nil")
	}
	if !strings.Contains(err.Error(), "existingPVCName") {
		t.Errorf("error = %q, want to mention existingPVCName", err.Error())
	}
}

// TestRebindPVToNamespace_BindTargetExisting exercises the full rebindPVToNamespace
// flow in BindTargetExisting mode starting from an unbound target PVC, including
// the real waitForPVCBound poll loop (the already-bound idempotent short-circuit
// at this same rebindPVToNamespace level is covered separately by
// TestRebindPVToNamespace_AlreadyBoundIdempotentShortCircuit above; this test
// exercises the non-short-circuit path). A background goroutine flips the
// target PVC to Bound shortly after the patch, simulating what a real
// Kubernetes PV controller would do once claimRef is set -- fake clients don't run
// that controller, so the test drives it manually.
func TestRebindPVToNamespace_BindTargetExisting(t *testing.T) {
	origInterval, origTimeout := pvRebindPollInterval, pvRebindTimeout
	pvRebindPollInterval = 10 * time.Millisecond
	pvRebindTimeout = 2 * time.Second
	defer func() {
		pvRebindPollInterval = origInterval
		pvRebindTimeout = origTimeout
	}()

	// Snapshotted before the goroutine starts so the goroutine's deadline can't be
	// affected by the deferred restore above running concurrently with it.
	pvRebindTimeout := pvRebindTimeout
	binderDone := make(chan struct{})
	var statusUpdateErr error
	defer func() {
		// Wait for the fake-binder goroutine to finish before the vars-restore
		// defer above runs, so it never observes restored (non-test) values and
		// never outlives this test function.
		select {
		case <-binderDone:
			if statusUpdateErr != nil {
				t.Errorf("fake-binder goroutine failed to update PVC status: %v", statusUpdateErr)
			}
		case <-time.After(pvRebindTimeout + time.Second):
			t.Log("timed out waiting for fake-binder goroutine to finish")
		}
	}()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	sourcePV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("10Gi"),
			},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
			// A real bound PV always has ClaimRef pointing back at its PVC --
			// modeled here for fixture fidelity, even though the only production
			// code that reads ClaimRef (the Step 1.5 idempotency check) looks for
			// it naming the *target* PVC, not this source one, so this value is
			// inert for this test's own assertions.
			ClaimRef: &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Name:      "scratch-pvc",
				Namespace: "oadp-ns",
				UID:       types.UID("scratch-pvc-uid"),
			},
		},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "scratch-pvc", Namespace: "oadp-ns", UID: types.UID("scratch-pvc-uid")},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	targetPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-disk", Namespace: "restore-ns", UID: types.UID("restored-disk-uid")},
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

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sourcePV, sourcePVC, targetPVC).
		Build()

	go func() {
		// rebindPVToNamespace's patchPVBinding only sets the PV's claimRef -- a
		// real Kubernetes PV controller then completes the bind by setting the
		// PVC's Spec.VolumeName and Status.Phase, which the fake client won't do
		// on its own. Poll for the claimRef (what the code under test actually
		// writes) before simulating the rest of that binder's job, rather than a
		// fixed sleep that could race ahead of the patch.
		defer close(binderDone)
		deadline := time.Now().Add(pvRebindTimeout)
		for time.Now().Before(deadline) {
			pv := &corev1.PersistentVolume{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: sourcePV.Name}, pv); err != nil {
				statusUpdateErr = fmt.Errorf("get PV: %w", err)
				return
			}
			if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == targetPVC.Name && pv.Spec.ClaimRef.Namespace == targetPVC.Namespace {
				pvc := &corev1.PersistentVolumeClaim{}
				if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: targetPVC.Name, Namespace: targetPVC.Namespace}, pvc); err != nil {
					statusUpdateErr = fmt.Errorf("get target PVC: %w", err)
					return
				}
				pvc.Spec.VolumeName = sourcePV.Name
				if err := fakeClient.Update(context.Background(), pvc); err != nil {
					statusUpdateErr = fmt.Errorf("update target PVC: %w", err)
					return
				}
				pvc.Status.Phase = corev1.ClaimBound
				statusUpdateErr = fakeClient.Status().Update(context.Background(), pvc)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		statusUpdateErr = fmt.Errorf("timed out waiting for PV %s claimRef to name target PVC %s/%s", sourcePV.Name, targetPVC.Namespace, targetPVC.Name)
	}()

	result, err := rebindPVToNamespace(
		context.Background(), fakeClient, logr.Discard(),
		sourcePVC.Name, sourcePVC.Namespace, targetPVC.Namespace,
		"test-dd", "uid-123",
		"velero.io/datadownload-uid", "velero.io/datadownload-name",
		BindTargetExisting, targetPVC.Name,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NewPVCName != targetPVC.Name {
		t.Errorf("NewPVCName = %q, want %q", result.NewPVCName, targetPVC.Name)
	}
	if result.NewPVCNamespace != targetPVC.Namespace {
		t.Errorf("NewPVCNamespace = %q, want %q", result.NewPVCNamespace, targetPVC.Namespace)
	}
	if result.PVName != sourcePV.Name {
		t.Errorf("PVName = %q, want %q", result.PVName, sourcePV.Name)
	}
	if result.OriginalReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
		t.Errorf("OriginalReclaimPolicy = %q, want %q", result.OriginalReclaimPolicy, corev1.PersistentVolumeReclaimDelete)
	}

	var pv corev1.PersistentVolume
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: sourcePV.Name}, &pv); err != nil {
		t.Fatalf("failed to get PV: %v", err)
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("PV reclaim policy = %q, want %q", pv.Spec.PersistentVolumeReclaimPolicy, corev1.PersistentVolumeReclaimRetain)
	}
	if got := pv.Annotations[pvOriginalReclaimPolicyAnnotation]; got != string(corev1.PersistentVolumeReclaimDelete) {
		t.Errorf("original-reclaim-policy annotation = %q, want %q", got, corev1.PersistentVolumeReclaimDelete)
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != targetPVC.Name || pv.Spec.ClaimRef.Namespace != targetPVC.Namespace {
		t.Errorf("PV claimRef = %+v, want %s/%s", pv.Spec.ClaimRef, targetPVC.Namespace, targetPVC.Name)
	}
	if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != targetPVC.UID {
		t.Errorf("PV claimRef UID = %v, want %q", pv.Spec.ClaimRef, targetPVC.UID)
	}

	var destPVC corev1.PersistentVolumeClaim
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: targetPVC.Name, Namespace: targetPVC.Namespace}, &destPVC); err != nil {
		t.Fatalf("failed to get destination PVC: %v", err)
	}
	if destPVC.Spec.VolumeName != sourcePV.Name {
		t.Errorf("destination PVC Spec.VolumeName = %q, want %q", destPVC.Spec.VolumeName, sourcePV.Name)
	}

	sourceGetErr := fakeClient.Get(
		context.Background(),
		types.NamespacedName{Name: sourcePVC.Name, Namespace: sourcePVC.Namespace},
		&corev1.PersistentVolumeClaim{},
	)
	if !errors.IsNotFound(sourceGetErr) {
		t.Errorf("expected source PVC to be deleted, get returned: %v", sourceGetErr)
	}
}

// TestRebindPVToNamespace_DestinationIneligibleAfterSourceDeleted covers Step
// 5's re-validation: the destination PVC can become ineligible for binding
// between Step 1.5's initial check and Step 5's re-check (Steps 2-3's Retain
// patch + source-PVC delete-and-wait take real wall-clock time). At that
// point the source PVC is already gone and the PV is left Retain'd with a
// stale claimRef -- unrecoverable automatically, so this must surface as a
// clear "needs manual recovery" error rather than silently binding to a PVC
// that no longer qualifies.
func TestRebindPVToNamespace_DestinationIneligibleAfterSourceDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	sourcePV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-scratch"},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              "standard",
			VolumeMode:                    new(corev1.PersistentVolumeFilesystem),
		},
	}
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "scratch-pvc", Namespace: "oadp-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:       "pv-scratch",
			StorageClassName: new("standard"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	targetPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-disk", Namespace: "restore-ns"},
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

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sourcePV, sourcePVC, targetPVC).
		Build()

	// Step 1.5's validateExistingPVCForBind Get sees the real, compatible
	// object; every subsequent Get for the same target PVC (Step 5's
	// re-validation) is handed a mutated copy with a mismatched
	// StorageClassName instead -- simulating the destination changing
	// underneath the rebind during Steps 2-3's real wall-clock work.
	targetPVCGets := 0
	interceptedClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Get: func(ctx context.Context, c crclient.WithWatch, key crclient.ObjectKey, obj crclient.Object, opts ...crclient.GetOption) error {
			pvc, ok := obj.(*corev1.PersistentVolumeClaim)
			if !ok || key.Name != targetPVC.Name || key.Namespace != targetPVC.Namespace {
				return c.Get(ctx, key, obj, opts...)
			}
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			targetPVCGets++
			if targetPVCGets >= 2 {
				pvc.Spec.StorageClassName = new("mismatched-class")
			}
			return nil
		},
	})

	_, err := rebindPVToNamespace(
		context.Background(), interceptedClient, logr.Discard(),
		sourcePVC.Name, sourcePVC.Namespace, targetPVC.Namespace,
		"test-dd", "uid-123",
		"velero.io/datadownload-uid", "velero.io/datadownload-name",
		BindTargetExisting, targetPVC.Name,
	)
	if err == nil {
		t.Fatal("expected an error when the destination PVC becomes ineligible after the source PVC is deleted")
	}
	if !strings.Contains(err.Error(), "needs manual recovery") {
		t.Errorf("error = %q, want it to mention manual recovery", err.Error())
	}

	// The source PVC must actually be gone -- this is exactly why the failure
	// is unrecoverable automatically rather than just retriable.
	sourceGetErr := interceptedClient.Get(
		context.Background(),
		types.NamespacedName{Name: sourcePVC.Name, Namespace: sourcePVC.Namespace},
		&corev1.PersistentVolumeClaim{},
	)
	if !errors.IsNotFound(sourceGetErr) {
		t.Errorf("expected source PVC to be deleted, get returned: %v", sourceGetErr)
	}

	// And the PV must be left Retain'd, not cleaned up automatically.
	var pv corev1.PersistentVolume
	if err := interceptedClient.Get(context.Background(), types.NamespacedName{Name: sourcePV.Name}, &pv); err != nil {
		t.Fatalf("failed to get PV: %v", err)
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		t.Errorf("PV reclaim policy = %q, want %q", pv.Spec.PersistentVolumeReclaimPolicy, corev1.PersistentVolumeReclaimRetain)
	}
}

// TestWaitForPVCBound_ForeignVolumeFailsFast covers the racing-provisioner
// guard: a target PVC whose Spec.VolumeName names a different PV can never
// bind to the expected one (VolumeName is immutable once set), so the wait
// must fail immediately with an attributable error rather than misreporting
// the foreign bind as success (Phase == Bound) or polling to timeout.
func TestWaitForPVCBound_ForeignVolumeFailsFast(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-disk", Namespace: "restore-ns"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "provisioner-won-pv"},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pvc).Build()

	err := waitForPVCBound(context.Background(), fakeClient, "restored-disk", "restore-ns", "pv-scratch")
	if err == nil {
		t.Fatal("expected error for PVC bound to a foreign volume, got nil")
	}
	if !strings.Contains(err.Error(), "provisioner-won-pv") || !strings.Contains(err.Error(), "pv-scratch") {
		t.Errorf("error = %q, want to mention both the foreign and expected volume names", err.Error())
	}
}
