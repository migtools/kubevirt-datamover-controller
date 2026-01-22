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
	"testing"

	"github.com/go-logr/logr"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	tests := []struct {
		name            string
		dataUpload      *velerov2alpha1.DataUpload
		expectedRequeue bool
		expectedPhase   velerov2alpha1.DataUploadPhase
		expectError     bool
	}{
		{
			name: "skip non-kubevirt datamover",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: "velero",
				},
			},
			expectedRequeue: false,
			expectedPhase:   "",
			expectError:     false,
		},
		{
			name: "new phase transitions to accepted",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseNew,
				},
			},
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataUploadPhaseAccepted,
			expectError:     false,
		},
		{
			name: "empty phase transitions to accepted",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: "",
				},
			},
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataUploadPhaseAccepted,
			expectError:     false,
		},
		{
			name: "completed phase is terminal - no action",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseCompleted,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseCompleted,
			expectError:     false,
		},
		{
			name: "failed phase is terminal - no action",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseFailed,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseFailed,
			expectError:     false,
		},
		{
			name: "canceled phase is terminal - no action",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseCanceled,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseCanceled,
			expectError:     false,
		},
		{
			name: "canceling phase transitions to canceled",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseCanceling,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseCanceled,
			expectError:     false,
		},
		{
			name: "prepared phase transitions to inprogress",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataUploadPhaseInProgress,
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.dataUpload).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: "openshift-adp",
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      tt.dataUpload.Name,
					Namespace: tt.dataUpload.Namespace,
				},
			}

			result, err := r.Reconcile(context.Background(), req)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result.Requeue != tt.expectedRequeue {
				t.Errorf("expected requeue=%v, got requeue=%v", tt.expectedRequeue, result.Requeue)
			}

			// Verify phase if we expect a transition
			if tt.expectedPhase != "" && tt.dataUpload.Spec.DataMover == DataMoverKubeVirt {
				updatedDU := &velerov2alpha1.DataUpload{}
				err := fakeClient.Get(context.Background(), req.NamespacedName, updatedDU)
				if err != nil {
					t.Errorf("failed to get updated DataUpload: %v", err)
				}
				if updatedDU.Status.Phase != tt.expectedPhase {
					t.Errorf("expected phase=%s, got phase=%s", tt.expectedPhase, updatedDU.Status.Phase)
				}
			}
		})
	}
}

func TestReconcile_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "non-existent",
			Namespace: "openshift-adp",
		},
	}

	result, err := r.Reconcile(context.Background(), req)

	if err != nil {
		t.Errorf("expected no error for not-found, got: %v", err)
	}
	if result.Requeue {
		t.Errorf("expected no requeue for not-found")
	}
}

func TestFilterKubeVirtDataMover(t *testing.T) {
	tests := []struct {
		name      string
		dataMover string
		expected  bool
	}{
		{
			name:      "kubevirt datamover matches",
			dataMover: DataMoverKubeVirt,
			expected:  true,
		},
		{
			name:      "velero datamover does not match",
			dataMover: "velero",
			expected:  false,
		},
		{
			name:      "empty datamover does not match",
			dataMover: "",
			expected:  false,
		},
		{
			name:      "kopia datamover does not match",
			dataMover: "kopia",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the filter logic directly - this is what the predicate checks
			matches := tt.dataMover == DataMoverKubeVirt
			if matches != tt.expected {
				t.Errorf("expected match=%v, got match=%v for datamover=%s",
					tt.expected, matches, tt.dataMover)
			}
		})
	}
}

func TestUpdatePhase(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	tests := []struct {
		name         string
		initialPhase velerov2alpha1.DataUploadPhase
		targetPhase  velerov2alpha1.DataUploadPhase
		message      string
		expectError  bool
	}{
		{
			name:         "update from new to accepted",
			initialPhase: velerov2alpha1.DataUploadPhaseNew,
			targetPhase:  velerov2alpha1.DataUploadPhaseAccepted,
			message:      "DataUpload accepted by kubevirt datamover",
			expectError:  false,
		},
		{
			name:         "update from accepted to prepared",
			initialPhase: velerov2alpha1.DataUploadPhaseAccepted,
			targetPhase:  velerov2alpha1.DataUploadPhasePrepared,
			message:      "VMBT/VMB created",
			expectError:  false,
		},
		{
			name:         "update from prepared to inprogress",
			initialPhase: velerov2alpha1.DataUploadPhasePrepared,
			targetPhase:  velerov2alpha1.DataUploadPhaseInProgress,
			message:      "Datamover pod launched",
			expectError:  false,
		},
		{
			name:         "update from inprogress to completed",
			initialPhase: velerov2alpha1.DataUploadPhaseInProgress,
			targetPhase:  velerov2alpha1.DataUploadPhaseCompleted,
			message:      "Backup completed successfully",
			expectError:  false,
		},
		{
			name:         "update from canceling to canceled",
			initialPhase: velerov2alpha1.DataUploadPhaseCanceling,
			targetPhase:  velerov2alpha1.DataUploadPhaseCanceled,
			message:      "DataUpload canceled",
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: tt.initialPhase,
				},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(du).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: "openshift-adp",
			}

			err := r.updatePhase(context.Background(), du, tt.targetPhase, tt.message)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Verify the phase was updated
			updatedDU := &velerov2alpha1.DataUpload{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      du.Name,
				Namespace: du.Namespace,
			}, updatedDU)
			if err != nil {
				t.Errorf("failed to get updated DataUpload: %v", err)
			}
			if updatedDU.Status.Phase != tt.targetPhase {
				t.Errorf("expected phase=%s, got phase=%s", tt.targetPhase, updatedDU.Status.Phase)
			}
			if updatedDU.Status.Message != tt.message {
				t.Errorf("expected message=%s, got message=%s", tt.message, updatedDU.Status.Message)
			}
		})
	}
}

func TestHandleAccepted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// Currently handleAccepted just logs and returns - no requeue
	if result.Requeue {
		t.Errorf("expected no requeue from handleAccepted (not yet implemented)")
	}
}

func TestHandleInProgress(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	result, err := r.handleInProgress(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// Currently handleInProgress just logs and returns - no requeue
	if result.Requeue {
		t.Errorf("expected no requeue from handleInProgress (not yet implemented)")
	}
}

func TestDefaultMaxConcurrentReconciles(t *testing.T) {
	if DefaultMaxConcurrentReconciles != 3 {
		t.Errorf("expected DefaultMaxConcurrentReconciles=3, got %d", DefaultMaxConcurrentReconciles)
	}
}

func TestDataMoverKubeVirtConstant(t *testing.T) {
	if DataMoverKubeVirt != "kubevirt" {
		t.Errorf("expected DataMoverKubeVirt='kubevirt', got '%s'", DataMoverKubeVirt)
	}
}
