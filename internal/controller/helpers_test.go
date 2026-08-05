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
		if cleanupNotReady, _ := cleanupPodsByUID(context.Background(), fakeClient, nil, "uid-label", "abc", "ns", logr.Discard()); cleanupNotReady {
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

		if cleanupNotReady, _ := cleanupPodsByUID(context.Background(), fakeClient, nil, "uid-label", "abc", "ns", logr.Discard()); !cleanupNotReady {
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

		cleanupNotReady, terminating := cleanupPodsByUID(context.Background(), fakeClient, nil, "uid-label", "abc", "ns", logr.Discard())
		if !cleanupNotReady {
			t.Fatal("expected not-ready to be reported for incomplete cleanup, got ready")
		}
		if !terminating {
			t.Error("expected terminating=true (pod has a DeletionTimestamp), got false")
		}

		var got corev1.Pod
		if getErr := fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &got); getErr != nil {
			t.Fatalf("expected pod to still exist (blocked on its finalizer), got err=%v", getErr)
		}
		if got.DeletionTimestamp == nil {
			t.Error("expected DeletionTimestamp to be set (delete was requested), but pod looks untouched")
		}
	})

	t.Run("a pod remaining with no DeletionTimestamp is a hard not-ready, not terminating", func(t *testing.T) {
		// Models a genuine anomaly: Delete was called and returned no error, but
		// the pod survived with no DeletionTimestamp at all (e.g. an interceptor
		// or webhook silently no-oping it) -- unlike the finalizer-blocked case
		// above, this isn't the expected self-resolving state, so terminating
		// must be false even though notReady is true.
		survivingPod := pod.DeepCopy()
		baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(survivingPod).Build()
		fakeClient := interceptor.NewClient(baseClient, interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				return nil // silently do nothing
			},
		})

		cleanupNotReady, terminating := cleanupPodsByUID(context.Background(), fakeClient, nil, "uid-label", "abc", "ns", logr.Discard())
		if !cleanupNotReady {
			t.Fatal("expected not-ready to be reported for incomplete cleanup, got ready")
		}
		if terminating {
			t.Error("expected terminating=false for a pod with no DeletionTimestamp, got true")
		}
	})

	t.Run("APIReader fallback overrides a stale cached confirmation re-list", func(t *testing.T) {
		// The pod is genuinely already gone (nothing seeded into either client),
		// but the cached k8sClient's List is rigged to still report it present on
		// the confirmation re-list only -- modeling informer lag right after a
		// delete elsewhere reflects on this client. The confirmation step must
		// use apiReader instead when one is given, bypassing that stale read.
		listCallCount := 0
		cachedBase := fake.NewClientBuilder().WithScheme(scheme).Build()
		cached := interceptor.NewClient(cachedBase, interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				listCallCount++
				if listCallCount == 1 {
					// Initial pre-delete list: genuinely nothing to delete.
					return c.List(ctx, list, opts...)
				}
				// Confirmation re-list: simulate a stale cache still showing the pod.
				podList, ok := list.(*corev1.PodList)
				if !ok {
					t.Fatalf("expected *corev1.PodList, got %T", list)
				}
				podList.Items = []corev1.Pod{*pod.DeepCopy()}
				return nil
			},
		})
		apiReader := fake.NewClientBuilder().WithScheme(scheme).Build() // authoritative: pod is gone

		if cleanupNotReady, _ := cleanupPodsByUID(context.Background(), cached, apiReader, "uid-label", "abc", "ns", logr.Discard()); cleanupNotReady {
			t.Error("expected APIReader fallback to override the stale cached confirmation, got not-ready")
		}
	})

	t.Run("APIReader fallback finds a pod missed by the initial cached list", func(t *testing.T) {
		// The pod genuinely exists in the one real backing store (base), but the
		// cached client's List is rigged to always report it missing -- modeling
		// informer cache lag right after this controller created it. k8sClient's
		// Delete still targets the same real store (matching a real cluster,
		// where a cached client's writes go straight to the API server) --
		// without the initial-list fallback, the delete loop would issue no
		// Delete calls at all, and nothing would ever get cleaned up until some
		// later reconcile's cache happened to catch up.
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).Build()
		cached := interceptor.NewClient(base, interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return nil // always report empty, regardless of real state
			},
		})

		if cleanupNotReady, _ := cleanupPodsByUID(context.Background(), cached, base, "uid-label", "abc", "ns", logr.Discard()); cleanupNotReady {
			t.Fatal("expected cleanup to be ready (pod found via APIReader and deleted), got not-ready")
		}

		var got corev1.Pod
		err := base.Get(context.Background(), client.ObjectKeyFromObject(pod), &got)
		if !apierrors.IsNotFound(err) {
			t.Errorf("expected pod found via APIReader to be deleted, got err=%v", err)
		}
	})
}

// TestFindPodByUID_APIReaderFallback covers the informer-cache-lag case: a pod
// that genuinely exists but hasn't yet propagated to the cached client's
// informer (e.g. created by this same controller moments ago in an earlier
// reconcile) must still be found via the uncached apiReader fallback, not
// misreported as absent.
func TestFindPodByUID_APIReaderFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lagging-pod", Namespace: "ns",
			Labels: map[string]string{"uid-label": "abc"},
		},
	}

	t.Run("cached client has the pod, apiReader is never needed", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).Build()
		apiReader := fake.NewClientBuilder().WithScheme(scheme).Build() // deliberately empty

		got, err := findPodByUID(context.Background(), cached, apiReader, "uid-label", "abc", "ns")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.Name != pod.Name {
			t.Errorf("expected to find pod via cached client, got %+v", got)
		}
	})

	t.Run("cached client is empty (cache lag), apiReader fallback finds it", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).Build() // simulates a stale/lagging cache
		apiReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).Build()

		got, err := findPodByUID(context.Background(), cached, apiReader, "uid-label", "abc", "ns")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.Name != pod.Name {
			t.Errorf("expected apiReader fallback to find pod, got %+v", got)
		}
	})

	t.Run("both empty and apiReader nil: genuinely not found, no panic", func(t *testing.T) {
		cached := fake.NewClientBuilder().WithScheme(scheme).Build()

		got, err := findPodByUID(context.Background(), cached, nil, "uid-label", "abc", "ns")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil pod, got %+v", got)
		}
	})
}
