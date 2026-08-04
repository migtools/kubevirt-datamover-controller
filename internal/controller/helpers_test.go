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
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestOperationTimeoutExceeded(t *testing.T) {
	tests := []struct {
		name          string
		acceptedAt    *metav1.Time
		specTimeout   time.Duration
		wantExceeded  bool
		wantEffective time.Duration
	}{
		{
			name:          "nil acceptedAt never exceeds",
			acceptedAt:    nil,
			specTimeout:   time.Second,
			wantExceeded:  false,
			wantEffective: 0,
		},
		{
			name:          "unset spec timeout falls back to default and has not exceeded",
			acceptedAt:    ptrTime(time.Now().Add(-time.Minute)),
			specTimeout:   0,
			wantExceeded:  false,
			wantEffective: DefaultOperationTimeout,
		},
		{
			name:          "unset spec timeout falls back to default and has exceeded",
			acceptedAt:    ptrTime(time.Now().Add(-(DefaultOperationTimeout + time.Minute))),
			specTimeout:   0,
			wantExceeded:  true,
			wantEffective: DefaultOperationTimeout,
		},
		{
			name:          "custom spec timeout not yet exceeded",
			acceptedAt:    ptrTime(time.Now().Add(-time.Second)),
			specTimeout:   time.Hour,
			wantExceeded:  false,
			wantEffective: time.Hour,
		},
		{
			name:          "custom spec timeout exceeded",
			acceptedAt:    ptrTime(time.Now().Add(-2 * time.Hour)),
			specTimeout:   time.Hour,
			wantExceeded:  true,
			wantEffective: time.Hour,
		},
		{
			name:          "negative spec timeout falls back to default",
			acceptedAt:    ptrTime(time.Now().Add(-time.Minute)),
			specTimeout:   -1,
			wantExceeded:  false,
			wantEffective: DefaultOperationTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exceeded, _, effective := operationTimeoutExceeded(tt.acceptedAt, tt.specTimeout)
			if exceeded != tt.wantExceeded {
				t.Errorf("exceeded = %v, want %v", exceeded, tt.wantExceeded)
			}
			if effective != tt.wantEffective {
				t.Errorf("effective = %v, want %v", effective, tt.wantEffective)
			}
		})
	}
}

func ptrTime(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t)
	return &mt
}

func TestCapRequeueToOperationDeadline(t *testing.T) {
	tests := []struct {
		name           string
		result         ctrl.Result
		acceptedAt     *metav1.Time
		specTimeout    time.Duration
		wantRequeueMin time.Duration
		wantRequeueMax time.Duration
	}{
		{
			name:           "zero RequeueAfter is left alone",
			result:         ctrl.Result{},
			acceptedAt:     ptrTime(time.Now()),
			specTimeout:    time.Hour,
			wantRequeueMin: 0,
			wantRequeueMax: 0,
		},
		{
			name:           "nil acceptedAt is left alone",
			result:         ctrl.Result{RequeueAfter: 30 * time.Second},
			acceptedAt:     nil,
			specTimeout:    time.Hour,
			wantRequeueMin: 30 * time.Second,
			wantRequeueMax: 30 * time.Second,
		},
		{
			name:           "plenty of time left, delay unchanged",
			result:         ctrl.Result{RequeueAfter: 5 * time.Second},
			acceptedAt:     ptrTime(time.Now().Add(-time.Second)),
			specTimeout:    time.Hour,
			wantRequeueMin: 5 * time.Second,
			wantRequeueMax: 5 * time.Second,
		},
		{
			name:           "delay capped to the remaining deadline",
			result:         ctrl.Result{RequeueAfter: 5 * time.Minute},
			acceptedAt:     ptrTime(time.Now().Add(-time.Second)),
			specTimeout:    time.Minute,
			wantRequeueMin: 55 * time.Second,
			wantRequeueMax: time.Minute,
		},
		{
			name:           "deadline already elapsed requeues almost immediately, not with the stale long delay",
			result:         ctrl.Result{RequeueAfter: 30 * time.Second},
			acceptedAt:     ptrTime(time.Now().Add(-2 * time.Hour)),
			specTimeout:    time.Hour,
			wantRequeueMin: immediateRequeueDelay,
			wantRequeueMax: immediateRequeueDelay,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := capRequeueToOperationDeadline(tt.result, tt.acceptedAt, tt.specTimeout)
			if got.RequeueAfter < tt.wantRequeueMin || got.RequeueAfter > tt.wantRequeueMax {
				t.Errorf("RequeueAfter = %v, want in range [%v, %v]", got.RequeueAfter, tt.wantRequeueMin, tt.wantRequeueMax)
			}
		})
	}
}

func TestCheckOperationTimeoutCore(t *testing.T) {
	newTarget := func(acceptedAt *metav1.Time, specTimeout time.Duration, persistErr, failErr error) (*operationTimeoutTarget, *int, *int) {
		persistCalls, failCalls := 0, 0
		target := &operationTimeoutTarget{
			acceptedTimestamp:    func() *metav1.Time { return acceptedAt },
			setAcceptedTimestamp: func(t *metav1.Time) { acceptedAt = t },
			operationTimeout:     specTimeout,
			phase:                func() string { return "Accepted" },
			persist: func(ctx context.Context) error {
				persistCalls++
				return persistErr
			},
			fail: func(ctx context.Context, message string) error {
				failCalls++
				return failErr
			},
		}
		return target, &persistCalls, &failCalls
	}

	t.Run("nil acceptedAt backfills and succeeds", func(t *testing.T) {
		target, persistCalls, failCalls := newTarget(nil, time.Hour, nil, nil)
		failed, err := checkOperationTimeoutCore(context.Background(), logr.Discard(), "TestResource", *target)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if failed {
			t.Error("failed = true, want false (backfilling must not itself fail the resource)")
		}
		if *persistCalls != 1 {
			t.Errorf("persist calls = %d, want 1", *persistCalls)
		}
		if *failCalls != 0 {
			t.Errorf("fail calls = %d, want 0", *failCalls)
		}
		if target.acceptedTimestamp() == nil {
			t.Error("acceptedTimestamp still nil after backfill")
		}
	})

	t.Run("nil acceptedAt backfill persist failure is propagated", func(t *testing.T) {
		persistErr := errors.New("boom")
		target, persistCalls, failCalls := newTarget(nil, time.Hour, persistErr, nil)
		failed, err := checkOperationTimeoutCore(context.Background(), logr.Discard(), "TestResource", *target)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, persistErr) {
			t.Errorf("error = %v, want it to wrap %v", err, persistErr)
		}
		if failed {
			t.Error("failed = true, want false")
		}
		if *persistCalls != 1 {
			t.Errorf("persist calls = %d, want 1", *persistCalls)
		}
		if *failCalls != 0 {
			t.Errorf("fail calls = %d, want 0", *failCalls)
		}
	})

	t.Run("not yet exceeded is a no-op", func(t *testing.T) {
		target, persistCalls, failCalls := newTarget(ptrTime(time.Now().Add(-time.Second)), time.Hour, nil, nil)
		failed, err := checkOperationTimeoutCore(context.Background(), logr.Discard(), "TestResource", *target)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if failed {
			t.Error("failed = true, want false")
		}
		if *persistCalls != 0 {
			t.Errorf("persist calls = %d, want 0", *persistCalls)
		}
		if *failCalls != 0 {
			t.Errorf("fail calls = %d, want 0", *failCalls)
		}
	})

	t.Run("exceeded transitions to failed", func(t *testing.T) {
		target, persistCalls, failCalls := newTarget(ptrTime(time.Now().Add(-2*time.Hour)), time.Hour, nil, nil)
		failed, err := checkOperationTimeoutCore(context.Background(), logr.Discard(), "TestResource", *target)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !failed {
			t.Error("failed = false, want true")
		}
		if *failCalls != 1 {
			t.Errorf("fail calls = %d, want 1", *failCalls)
		}
		if *persistCalls != 0 {
			t.Errorf("persist calls = %d, want 0", *persistCalls)
		}
	})

	t.Run("exceeded but fail callback errors is propagated", func(t *testing.T) {
		failErr := errors.New("update conflict")
		target, _, failCalls := newTarget(ptrTime(time.Now().Add(-2*time.Hour)), time.Hour, nil, failErr)
		failed, err := checkOperationTimeoutCore(context.Background(), logr.Discard(), "TestResource", *target)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, failErr) {
			t.Errorf("error = %v, want it to wrap %v", err, failErr)
		}
		if failed {
			t.Error("failed = true, want false (the phase transition itself did not persist)")
		}
		if *failCalls != 1 {
			t.Errorf("fail calls = %d, want 1", *failCalls)
		}
	})
}

func TestCleanupPodsByUID(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "victim-pod", Namespace: "ns",
			Labels: map[string]string{"uid-label": "abc"},
		},
	}

	t.Run("deletes matching pods", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).Build()
		if cleanupNotReady := cleanupPodsByUID(context.Background(), fakeClient, "uid-label", "abc", "ns", logr.Discard()); cleanupNotReady {
			t.Fatal("expected cleanup to be ready (pod deleted with no finalizers), got not-ready")
		}
		var got corev1.Pod
		err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &got)
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected pod to be deleted, got err=%v", err)
		}
	})

	t.Run("delete failure is reported, not swallowed", func(t *testing.T) {
		deleteErr := errors.New("injected delete failure")
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).
			WithInterceptorFuncs(interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					return deleteErr
				},
			}).Build()

		if cleanupNotReady := cleanupPodsByUID(context.Background(), fakeClient, "uid-label", "abc", "ns", logr.Discard()); !cleanupNotReady {
			t.Fatal("expected not-ready to be reported, got ready")
		}
		// The pod must still exist -- the caller learns cleanup didn't actually
		// succeed instead of assuming it did.
		var got corev1.Pod
		if err := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &got); err != nil {
			t.Errorf("expected pod to still exist after a failed delete, got err=%v", err)
		}
	})

	t.Run("a pod still terminating after Delete is reported, not silently accepted", func(t *testing.T) {
		// Delete() only requests termination -- a pod held by a finalizer gets a
		// DeletionTimestamp but is not actually removed until the finalizer clears
		// (the fake client models this the same way a real API server does). A
		// caller relying on "the pod is stopped" must see this as still-incomplete
		// cleanup, not a successful one.
		terminatingPod := pod.DeepCopy()
		terminatingPod.Finalizers = []string{"example.com/still-cleaning-up"}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(terminatingPod).Build()

		if cleanupNotReady := cleanupPodsByUID(context.Background(), fakeClient, "uid-label", "abc", "ns", logr.Discard()); !cleanupNotReady {
			t.Fatal("expected not-ready to be reported for incomplete cleanup, got ready")
		}

		var got corev1.Pod
		if getErr := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &got); getErr != nil {
			t.Fatalf("expected pod to still exist (blocked on its finalizer), got err=%v", getErr)
		}
		if got.DeletionTimestamp == nil {
			t.Error("expected DeletionTimestamp to be set (delete was requested), but pod looks untouched")
		}
	})
}
