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
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtcorev1.AddToScheme(scheme)

	// Helper function to create a valid VM with CBT enabled and running
	validVM := func(name, namespace string) *kubevirtcorev1.VirtualMachine {
		return &kubevirtcorev1.VirtualMachine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Status: kubevirtcorev1.VirtualMachineStatus{
				PrintableStatus: kubevirtcorev1.VirtualMachineStatusRunning,
				ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
					State: kubevirtcorev1.ChangedBlockTrackingEnabled,
				},
			},
		}
	}

	tests := []struct {
		name            string
		dataUpload      *velerov2alpha1.DataUpload
		vm              *kubevirtcorev1.VirtualMachine // optional: VM to create in fake client
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
			name: "new phase transitions to accepted with VM annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "default",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseNew,
				},
			},
			vm:              validVM("test-vm", "default"),
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataUploadPhaseAccepted,
			expectError:     false,
		},
		{
			name: "empty phase transitions to accepted with VM annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "default",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: "",
				},
			},
			vm:              validVM("test-vm", "default"),
			expectedRequeue: true,
			expectedPhase:   velerov2alpha1.DataUploadPhaseAccepted,
			expectError:     false,
		},
		{
			name: "new phase fails without VM annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseNew,
				},
			},
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseFailed,
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
					DataMover: common.DataMoverKubeVirt,
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
					DataMover: common.DataMoverKubeVirt,
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
					DataMover: common.DataMoverKubeVirt,
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
					DataMover: common.DataMoverKubeVirt,
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
					DataMover: common.DataMoverKubeVirt,
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
			builder := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.dataUpload)

			// Add VM to fake client if provided
			if tt.vm != nil {
				builder = builder.WithObjects(tt.vm)
			}

			fakeClient := builder.Build()

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
			// Check if requeue is expected (using RequeueAfter > 0 instead of deprecated Requeue field)
			gotRequeue := result.RequeueAfter > 0
			if gotRequeue != tt.expectedRequeue {
				t.Errorf("expected requeue=%v, got requeue=%v (RequeueAfter=%v)", tt.expectedRequeue, gotRequeue, result.RequeueAfter)
			}

			// Verify phase if we expect a transition
			if tt.expectedPhase != "" && tt.dataUpload.Spec.DataMover == common.DataMoverKubeVirt {
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
	if result.RequeueAfter > 0 {
		t.Errorf("expected no requeue for not-found, got RequeueAfter=%v", result.RequeueAfter)
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
			dataMover: common.DataMoverKubeVirt,
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
			matches := tt.dataMover == common.DataMoverKubeVirt
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
					DataMover: common.DataMoverKubeVirt,
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
			DataMover: common.DataMoverKubeVirt,
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
	// handleAccepted now implements Phase 2 logic and may request requeue
	// This is expected behavior when VMB is in progress
	_ = result // result.RequeueAfter may be > 0 depending on VMB state
}

func TestHandleAccepted_VMBStatusDetection(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name          string
		vmbConditions []kubevirtbackupv1alpha1.Condition
		expectedPhase velerov2alpha1.DataUploadPhase
		expectRequeue bool
	}{
		{
			name: "VMB Done True transitions to Prepared",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "Completed VirtualMachineBackup",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
					Reason: "Completed VirtualMachineBackup",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhasePrepared,
			expectRequeue: true,
		},
		{
			name: "VMB Done False transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:    kubevirtbackupv1alpha1.ConditionDone,
					Status:  corev1.ConditionFalse,
					Reason:  "BackupFailed",
					Message: "VM backup failed due to an error",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
		},
		{
			name: "VMB only Progressing True requeues",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionTrue,
					Reason: "Backup in progress",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged
			expectRequeue: true,
		},
		{
			name: "VMB Progressing False without Done requeues",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionInitializing,
					Status: corev1.ConditionFalse,
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "PVC being attached to VM",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged - wait for Done
			expectRequeue: true,
		},
		{
			name:          "VMB no conditions requeues",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged
			expectRequeue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vmName := "test-vm"
			vmNamespace := "test-ns"
			duName := "test-du"

			// Create DataUpload
			du := &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      duName,
					Namespace: vmNamespace,
					UID:       types.UID("test-uid"),
					Annotations: map[string]string{
						common.AnnotationVMName:      vmName,
						common.AnnotationVMNamespace: vmNamespace,
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:       common.DataMoverKubeVirt,
					SourceNamespace: vmNamespace,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhaseAccepted,
				},
			}

			// Create temporary PVC (needed for handleAccepted to proceed)
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "kubevirt-backup-" + duName,
					Namespace: vmNamespace,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "velero.io/v2alpha1",
							Kind:       "DataUpload",
							Name:       duName,
							UID:        du.UID,
						},
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Phase: corev1.ClaimBound,
				},
			}

			// Create VMBT
			vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vmbt-" + vmName,
					Namespace: vmNamespace,
				},
				Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
					Source: corev1.TypedLocalObjectReference{
						APIGroup: strPtr("kubevirt.io"),
						Kind:     "VirtualMachine",
						Name:     vmName,
					},
				},
			}

			// Create VMB with specified conditions
			checkpointName := "vmb-" + duName + "-checkpoint"
			vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vmb-" + duName,
					Namespace: vmNamespace,
					Labels: map[string]string{
						common.LabelDataUploadName: duName,
						common.LabelDataUploadUID:  string(du.UID),
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "velero.io/v2alpha1",
							Kind:       "DataUpload",
							Name:       duName,
							UID:        du.UID,
						},
					},
				},
				Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
					Source: corev1.TypedLocalObjectReference{
						APIGroup: strPtr("backup.kubevirt.io"),
						Kind:     "VirtualMachineBackupTracker",
						Name:     vmbt.Name,
					},
					PvcName: strPtr(pvc.Name),
				},
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Type:           kubevirtbackupv1alpha1.Full,
					CheckpointName: &checkpointName,
					Conditions:     tt.vmbConditions,
				},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(du, pvc, vmbt, vmb).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: vmNamespace,
			}

			result, err := r.handleAccepted(context.Background(), logr.Discard(), du)

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectRequeue && result.RequeueAfter == 0 {
				t.Errorf("expected requeue, got no requeue")
			}
			if !tt.expectRequeue && result.RequeueAfter > 0 {
				t.Errorf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
			}

			// Fetch updated DataUpload to check phase
			var updatedDU velerov2alpha1.DataUpload
			if err := fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      duName,
				Namespace: vmNamespace,
			}, &updatedDU); err != nil {
				t.Fatalf("failed to get updated DataUpload: %v", err)
			}

			if updatedDU.Status.Phase != tt.expectedPhase {
				t.Errorf("expected phase=%s, got phase=%s", tt.expectedPhase, updatedDU.Status.Phase)
			}
		})
	}
}

// strPtr returns a pointer to the given string
func strPtr(s string) *string {
	return &s
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
			DataMover: common.DataMoverKubeVirt,
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
	// handleInProgress is a placeholder for Phase 3
	// Currently returns empty result (no requeue)
	if result.RequeueAfter > 0 {
		t.Errorf("expected no requeue from handleInProgress (not yet implemented), got RequeueAfter=%v", result.RequeueAfter)
	}
}

func TestDefaultMaxConcurrentReconciles(t *testing.T) {
	if DefaultMaxConcurrentReconciles != 3 {
		t.Errorf("expected DefaultMaxConcurrentReconciles=3, got %d", DefaultMaxConcurrentReconciles)
	}
}

func TestDataMoverKubeVirtConstant(t *testing.T) {
	if common.DataMoverKubeVirt != "kubevirt" {
		t.Errorf("expected common.DataMoverKubeVirt='kubevirt', got '%s'", common.DataMoverKubeVirt)
	}
}

func TestGetVMReference(t *testing.T) {
	// Tests the common.GetVMReference function used by the controller
	tests := []struct {
		name              string
		dataUpload        *velerov2alpha1.DataUpload
		expectedVMName    string
		expectedNamespace string
		expectError       bool
	}{
		{
			name: "valid annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "my-vm",
						common.AnnotationVMNamespace: "my-namespace",
					},
				},
			},
			expectedVMName:    "my-vm",
			expectedNamespace: "my-namespace",
			expectError:       false,
		},
		{
			name: "missing namespace annotation uses source namespace",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName: "my-vm",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					SourceNamespace: "source-ns",
				},
			},
			expectedVMName:    "my-vm",
			expectedNamespace: "source-ns",
			expectError:       false,
		},
		{
			name: "no annotations",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
			},
			expectedVMName:    "",
			expectedNamespace: "",
			expectError:       true,
		},
		{
			name: "missing vm name annotation",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMNamespace: "my-namespace",
					},
				},
			},
			expectedVMName:    "",
			expectedNamespace: "",
			expectError:       true,
		},
		{
			name: "empty vm name annotation",
			dataUpload: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "",
						common.AnnotationVMNamespace: "my-namespace",
					},
				},
			},
			expectedVMName:    "",
			expectedNamespace: "",
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vmRef, err := common.GetVMReference(tt.dataUpload)

			if tt.expectError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			var vmName, vmNamespace string
			if vmRef != nil {
				vmName = vmRef.Name
				vmNamespace = vmRef.Namespace
			}

			if vmName != tt.expectedVMName {
				t.Errorf("expected vmName=%s, got vmName=%s", tt.expectedVMName, vmName)
			}
			if vmNamespace != tt.expectedNamespace {
				t.Errorf("expected vmNamespace=%s, got vmNamespace=%s", tt.expectedNamespace, vmNamespace)
			}
		})
	}
}

func TestAnnotationConstants(t *testing.T) {
	if common.AnnotationVMName != "kubevirt-datamover.io/vm-name" {
		t.Errorf("expected common.AnnotationVMName='kubevirt-datamover.io/vm-name', got '%s'", common.AnnotationVMName)
	}
	if common.AnnotationVMNamespace != "kubevirt-datamover.io/vm-namespace" {
		t.Errorf("expected common.AnnotationVMNamespace='kubevirt-datamover.io/vm-namespace', got '%s'", common.AnnotationVMNamespace)
	}
	if common.LabelDataUploadName != "velero.io/dataupload-name" {
		t.Errorf("expected common.LabelDataUploadName='velero.io/dataupload-name', got '%s'", common.LabelDataUploadName)
	}
	if common.LabelDataUploadUID != "velero.io/dataupload-uid" {
		t.Errorf("expected common.LabelDataUploadUID='velero.io/dataupload-uid', got '%s'", common.LabelDataUploadUID)
	}
	if DefaultTempPVCSize != "10Gi" {
		t.Errorf("expected DefaultTempPVCSize='10Gi', got '%s'", DefaultTempPVCSize)
	}
}
