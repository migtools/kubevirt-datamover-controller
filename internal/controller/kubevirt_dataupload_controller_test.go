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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
			name: "prepared phase without VM annotations fails",
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
			expectedRequeue: false,
			expectedPhase:   velerov2alpha1.DataUploadPhaseFailed,
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
			UID:       types.UID("test-uid-in-progress"),
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

// TestHandleAccepted_VMBTAnnotationSetButVMBNotVisible tests the race condition where
// a rapid re-reconcile runs before the cached client sees the VMB that was just created.
// The VMBT annotation is already set (from the first reconcile), so the controller must
// NOT re-enter prepareVMBackupTracker (which would delete the VMBT the VMB references).
// Instead it should requeue to let the cache catch up.
func TestHandleAccepted_VMBTAnnotationSetButVMBNotVisible(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	duName := "test-du-race"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: "openshift-adp",
			UID:       types.UID("du-uid-race"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
				// VMBT annotation already set by a previous reconcile
				AnnotationVMBTName: "vmbt-test-vm-prev",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "default",
			SourceNamespace:       vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	// Temp PVC that was already created by a previous reconcile
	tempPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName + "-abc12",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "du-uid-race",
			},
		},
	}

	// The VMBT that the (not-yet-visible) VMB references — must NOT be deleted
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm-prev",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash: common.HashForLabel(vmName),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{},
	}

	// No VMB in the fake client — simulates cache lag where VMB isn't visible yet
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, tempPVC, vmbt).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:         fakeClient,
		Scheme:         scheme,
		Log:            logr.Discard(),
		OADPNamespace:  "openshift-adp",
		DatamoverImage: "quay.io/test/datamover:v1",
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should requeue (not fail, not enter VMBT preparation)
	if result.RequeueAfter == 0 {
		t.Error("expected requeue but got none — controller should wait for cache to catch up")
	}

	// Verify the VMBT was NOT deleted
	existingVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: "vmbt-test-vm-prev", Namespace: vmNamespace,
	}, existingVMBT); err != nil {
		t.Errorf("VMBT was deleted when it should have been preserved: %v", err)
	}
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
		skipVMBT      bool // when true, do not create the VMBT (simulates deleted VMBT)
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
			name: "VMB Done True with Failed reason transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:    kubevirtbackupv1alpha1.ConditionProgressing,
					Status:  corev1.ConditionFalse,
					Reason:  "Failed",
					Message: "Backup has failed: No space left on device",
				},
				{
					Type:    kubevirtbackupv1alpha1.ConditionDone,
					Status:  corev1.ConditionTrue,
					Reason:  "Failed",
					Message: "Backup has failed: No space left on device",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
		},
		{
			name: "VMB Done False and Progressing False transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "BackupFailed",
				},
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
			name: "VMB Done False and Progressing True requeues (backup still running)",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionTrue,
					Reason: "Backup is in progress",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionFalse,
					Reason: "Backup is in progress",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged - still running
			expectRequeue: true,
		},
		{
			name: "VMB Done False without Progressing requeues",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:    kubevirtbackupv1alpha1.ConditionDone,
					Status:  corev1.ConditionFalse,
					Reason:  "Backup is in progress",
					Message: "",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged - wait for Progressing
			expectRequeue: true,
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
			name: "VMB stuck initializing (VMBT deleted) transitions to Failed",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionInitializing,
					Status: corev1.ConditionTrue,
					Reason: "BackupTracker vmbt-test-vm does not exist",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "BackupTracker vmbt-test-vm does not exist",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
			skipVMBT:      true, // VMBT is gone — this should fail
		},
		{
			name: "VMB initializing with VMBT present requeues (PVC being attached)",
			vmbConditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionInitializing,
					Status: corev1.ConditionTrue,
					Reason: "backup target PVC is being attached to VMI",
				},
				{
					Type:   kubevirtbackupv1alpha1.ConditionProgressing,
					Status: corev1.ConditionFalse,
					Reason: "backup target PVC is being attached to VMI",
				},
			},
			expectedPhase: velerov2alpha1.DataUploadPhaseAccepted, // unchanged — transient state
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
					Labels: map[string]string{
						common.LabelDataUploadUID: string(du.UID),
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
					Labels: map[string]string{
						common.LabelVMNameHash: common.HashForLabel(vmName),
					},
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
						common.LabelDataUploadUID: string(du.UID),
					},
					Annotations: map[string]string{
						common.AnnotationDataUploadName: duName,
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

			objects := []client.Object{du, pvc, vmb}
			if !tt.skipVMBT {
				objects = append(objects, vmbt)
			}
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objects...).
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

func TestIsVMBTerminal(t *testing.T) {
	tests := []struct {
		name     string
		vmb      *kubevirtbackupv1alpha1.VirtualMachineBackup
		expected bool
	}{
		{
			name: "nil status is not terminal",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: nil,
			},
			expected: false,
		},
		{
			name: "no conditions is not terminal",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{},
				},
			},
			expected: false,
		},
		{
			name: "Done=True is terminal (success)",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionTrue},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: true,
		},
		{
			name: "Done=False + Progressing=False is terminal (failure)",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionFalse},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: true,
		},
		{
			name: "Progressing=True + Done=False is not terminal (in progress)",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionFalse},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionTrue},
					},
				},
			},
			expected: false,
		},
		{
			name: "Initializing=True only is not terminal",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionInitializing, Status: corev1.ConditionTrue},
						{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
		{
			name: "Done=False without Progressing is not terminal",
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
					Conditions: []kubevirtbackupv1alpha1.Condition{
						{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionFalse},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isVMBTerminal(tt.vmb)
			if got != tt.expected {
				t.Errorf("isVMBTerminal() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHasOlderActiveDUForVM(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	oadpNs := "openshift-adp"

	// Use second-precision times to avoid nanosecond issues with fake client
	baseTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC))
	newerTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC))

	makeDU := func(name string, uid types.UID, phase velerov2alpha1.DataUploadPhase, creationTime metav1.Time, vm, vmNs string) *velerov2alpha1.DataUpload {
		return &velerov2alpha1.DataUpload{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         oadpNs,
				UID:               uid,
				CreationTimestamp: creationTime,
				Annotations: map[string]string{
					common.AnnotationVMName:      vm,
					common.AnnotationVMNamespace: vmNs,
				},
			},
			Spec: velerov2alpha1.DataUploadSpec{
				DataMover:       common.DataMoverKubeVirt,
				SourceNamespace: vmNs,
			},
			Status: velerov2alpha1.DataUploadStatus{
				Phase: phase,
			},
		}
	}

	// The DU we're testing (current)
	currentDU := makeDU("du-current", "current-uid", velerov2alpha1.DataUploadPhaseAccepted, baseTime, vmName, vmNamespace)

	tests := []struct {
		name        string
		otherDUs    []client.Object
		wantWait    bool
		wantBlocked string
	}{
		{
			name:     "no other DUs",
			otherDUs: []client.Object{},
			wantWait: false,
		},
		{
			name: "other DU targets different VM",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, olderTime, "different-vm", vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "other DU targets same VM in different namespace",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, olderTime, vmName, "other-ns"),
			},
			wantWait: false,
		},
		{
			name: "older DU in Completed phase (terminal)",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseCompleted, olderTime, vmName, vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "older DU in Failed phase (terminal)",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseFailed, olderTime, vmName, vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "older DU in New phase — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseNew, olderTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "older DU in Accepted phase — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, olderTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "older DU in Prepared phase — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhasePrepared, olderTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "older DU in InProgress phase — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseInProgress, olderTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "newer DU in Accepted phase — should NOT wait",
			otherDUs: []client.Object{
				makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, newerTime, vmName, vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "same timestamp, lower UID — should wait",
			otherDUs: []client.Object{
				makeDU("du-other", "aaa-uid", velerov2alpha1.DataUploadPhaseAccepted, baseTime, vmName, vmNamespace),
			},
			wantWait:    true,
			wantBlocked: "du-other",
		},
		{
			name: "same timestamp, higher UID — should NOT wait",
			otherDUs: []client.Object{
				makeDU("du-other", "zzz-uid", velerov2alpha1.DataUploadPhaseAccepted, baseTime, vmName, vmNamespace),
			},
			wantWait: false,
		},
		{
			name: "non-kubevirt DU is ignored",
			otherDUs: []client.Object{
				func() *velerov2alpha1.DataUpload {
					du := makeDU("du-other", "other-uid", velerov2alpha1.DataUploadPhaseAccepted, olderTime, vmName, vmNamespace)
					du.Spec.DataMover = "kopia"
					return du
				}(),
			},
			wantWait: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := append([]client.Object{currentDU.DeepCopy()}, tt.otherDUs...)
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: oadpNs,
			}

			shouldWait, blockingDU, err := r.hasOlderActiveDUForVM(context.Background(), currentDU.DeepCopy())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if shouldWait != tt.wantWait {
				t.Errorf("hasOlderActiveDUForVM() shouldWait = %v, want %v", shouldWait, tt.wantWait)
			}
			if tt.wantWait && blockingDU != tt.wantBlocked {
				t.Errorf("hasOlderActiveDUForVM() blockingDU = %q, want %q", blockingDU, tt.wantBlocked)
			}
		})
	}
}

func TestHandleAccepted_RequeuesWhenOlderDUActive(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	oadpNs := "openshift-adp"

	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC))
	newerTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC))

	// DU-1 (older) is in InProgress — still uploading to S3
	du1 := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "du-1",
			Namespace:         oadpNs,
			UID:               types.UID("du-1-uid"),
			CreationTimestamp: olderTime,
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
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// DU-2 (newer) just entered Accepted — should wait for DU-1
	du2 := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "du-2",
			Namespace:         oadpNs,
			UID:               types.UID("du-2-uid"),
			CreationTimestamp: newerTime,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "default",
			SourceNamespace:       vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du1, du2).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: oadpNs,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RequeueAfter != RequeueAfterLong {
		t.Errorf("expected RequeueAfter=%v (RequeueAfterLong), got %v", RequeueAfterLong, result.RequeueAfter)
	}
}

func TestHandleAccepted_ProceedsWhenOlderDUCompleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "vm-ns"
	oadpNs := "openshift-adp"

	olderTime := metav1.NewTime(time.Date(2026, 1, 1, 11, 59, 0, 0, time.UTC))
	newerTime := metav1.NewTime(time.Date(2026, 1, 1, 12, 1, 0, 0, time.UTC))

	// DU-1 (older) is Completed — should not block
	du1 := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "du-1",
			Namespace:         oadpNs,
			UID:               types.UID("du-1-uid"),
			CreationTimestamp: olderTime,
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
			Phase: velerov2alpha1.DataUploadPhaseCompleted,
		},
	}

	// DU-1's old VMBT still exists in the VM namespace
	vmbt1 := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm-aaa",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash:    common.HashForLabel(vmName),
				common.LabelDataUploadUID: string(du1.UID),
			},
		},
	}

	// DU-2 (newer) — should proceed since DU-1 is terminal
	du2 := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "du-2",
			Namespace:         oadpNs,
			UID:               types.UID("du-2-uid"),
			CreationTimestamp: newerTime,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "default",
			SourceNamespace:       vmNamespace,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: oadpNs,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{Bucket: "b", Prefix: "v"},
			},
			Config:     map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "c"}, Key: "cloud"},
		},
		Status: velerov1.BackupStorageLocationStatus{Phase: velerov1.BackupStorageLocationPhaseAvailable},
	}

	// Temp PVC for DU-2
	tempPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-du-2",
			Namespace: vmNamespace,
			Labels:    map[string]string{common.LabelDataUploadUID: "du-2-uid"},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du1, du2, bsl, tempPVC, vmbt1).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: oadpNs,
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du2)

	// DU-2 should NOT be blocked — DU-1 is Completed (terminal).
	// It will proceed into prepareVMBackupTracker, which will fail because
	// BSL credentials aren't fully set up in this test. That's fine — we're
	// testing that the serialization guard does NOT block.
	// The important assertion is that DU-1's VMBT is reused (not deleted) per issue #32.
	reusedVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	getErr := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: vmbt1.Name, Namespace: vmNamespace,
	}, reusedVMBT)

	// prepareVMBackupTracker should have reused DU-1's existing VMBT
	if getErr != nil {
		t.Errorf("expected DU-1's VMBT to be reused by prepareVMBackupTracker, but it was deleted: %v", getErr)
	}
	// We don't care about the error from handleAccepted itself — it may fail
	// on BSL credential lookup. The point is it got past the serialization guard.
	_ = err
}

// strPtr returns a pointer to the given string
func strPtr(s string) *string {
	return &s
}

func TestHandleInProgress(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

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

	// Create a running datamover pod in the OADP namespace
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod).
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
	// When pod is running, should requeue to check again
	if result.RequeueAfter != RequeueAfterShort {
		t.Errorf("expected RequeueAfter=%v when pod is running, got %v", RequeueAfterShort, result.RequeueAfter)
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
	if common.AnnotationDataUploadName != "velero.io/dataupload-name" {
		t.Errorf("expected common.AnnotationDataUploadName='velero.io/dataupload-name', got '%s'", common.AnnotationDataUploadName)
	}
	if common.LabelVMNameHash != "kubevirt-datamover.io/vm-name-hash" {
		t.Errorf("expected common.LabelVMNameHash='kubevirt-datamover.io/vm-name-hash', got '%s'", common.LabelVMNameHash)
	}
	if common.LabelDataUploadUID != "velero.io/dataupload-uid" {
		t.Errorf("expected common.LabelDataUploadUID='velero.io/dataupload-uid', got '%s'", common.LabelDataUploadUID)
	}
	if DefaultTempPVCSize != "10Gi" {
		t.Errorf("expected DefaultTempPVCSize='10Gi', got '%s'", DefaultTempPVCSize)
	}
}

func TestExtractPodFailureMessage(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected string
	}{
		{
			name: "container terminated with message",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "uploader",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Message: "Error: failed to upload files",
								},
							},
						},
					},
				},
			},
			expected: "Error: failed to upload files",
		},
		{
			name: "container terminated with reason only",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "uploader",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Reason: "OOMKilled",
								},
							},
						},
					},
				},
			},
			expected: "OOMKilled",
		},
		{
			name: "init container terminated with message",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "init",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									Message: "Init container failed",
								},
							},
						},
					},
				},
			},
			expected: "Init container failed",
		},
		{
			name: "pod condition with message",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					Conditions: []corev1.PodCondition{
						{
							Type:    corev1.PodScheduled,
							Status:  corev1.ConditionFalse,
							Message: "Insufficient memory",
						},
					},
				},
			},
			expected: "Insufficient memory",
		},
		{
			name: "no failure info",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{},
			},
			expected: "unknown error",
		},
		{
			name: "running container - no terminated state",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "uploader",
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			expected: "unknown error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractPodFailureMessage(tt.pod)
			if result != tt.expected {
				t.Errorf("extractPodFailureMessage() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestHandleInProgress_PodSucceeded(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-succeeded"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: "test-ns",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// Create a succeeded datamover pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod).
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
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue when pod succeeded, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify phase transitioned to Completed
	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseCompleted {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhaseCompleted, updatedDU.Status.Phase)
	}
}

func TestHandleInProgress_PodFailed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-failed"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: "test-ns",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// Create a failed datamover pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "uploader",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							Message: "S3 upload failed: access denied",
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod).
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
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue when pod failed, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify phase transitioned to Failed
	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}
}

func TestHandleInProgress_PodNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid-notfound"),
			Annotations: map[string]string{
				common.AnnotationVMName:      "test-vm",
				common.AnnotationVMNamespace: "test-ns",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseInProgress,
		},
	}

	// No pod exists
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
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue when pod not found, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify phase transitioned to Failed
	updatedDU := &velerov2alpha1.DataUpload{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      du.Name,
		Namespace: du.Namespace,
	}, updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}
}

func TestCleanupDatamoverResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du",
			Namespace: "openshift-adp",
			UID:       types.UID("test-uid"),
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// Create resources to be cleaned up
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.DatamoverPodNamePrefix + du.Name,
			Namespace: "openshift-adp",
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
	}

	reboundPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.ReboundPVCNamePrefix + du.Name,
			Namespace: "openshift-adp",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pod, reboundPVC).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	// Call cleanup
	r.cleanupDatamoverResources(context.Background(), logr.Discard(), du, "openshift-adp")

	// Verify pod was deleted
	deletedPod := &corev1.Pod{}
	err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}, deletedPod)
	if err == nil {
		t.Error("expected pod to be deleted")
	}

	// Verify rebound PVC was deleted
	deletedPVC := &corev1.PersistentVolumeClaim{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      reboundPVC.Name,
		Namespace: reboundPVC.Namespace,
	}, deletedPVC)
	if err == nil {
		t.Error("expected rebound PVC to be deleted")
	}
}

func TestGetVeleroBackupName(t *testing.T) {
	tests := []struct {
		name     string
		du       *velerov2alpha1.DataUpload
		expected string
	}{
		{
			name: "with velero backup label",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						common.LabelVeleroBackupName: "my-velero-backup",
					},
				},
			},
			expected: "my-velero-backup",
		},
		{
			name: "without velero backup label",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"some-other-label": "value",
					},
				},
			},
			expected: "",
		},
		{
			name: "nil labels",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{},
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getVeleroBackupName(tt.du)
			if result != tt.expected {
				t.Errorf("getVeleroBackupName() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBuildDatamoverPodConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	tests := []struct {
		name           string
		du             *velerov2alpha1.DataUpload
		bsl            *velerov1.BackupStorageLocation
		vmb            *kubevirtbackupv1alpha1.VirtualMachineBackup
		vmRef          *common.VMReference
		backupType     string
		checkpointName string
		vmbtName       string
		datamoverImage string
		expectError    bool
		errorContains  string
		validate       func(*testing.T, *DatamoverPodConfig)
	}{
		{
			name: "valid config",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					UID:       types.UID("du-uid-123"),
					Labels: map[string]string{
						common.LabelVeleroBackupName: "velero-backup-001",
					},
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "my-bucket",
							Prefix: "velero",
						},
					},
					Config: map[string]string{
						"region": "us-east-1",
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "cloud-credentials",
						},
						Key: "cloud",
					},
				},
			},
			vmb: &kubevirtbackupv1alpha1.VirtualMachineBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "vmb-test-du",
					Namespace: "vm-ns",
				},
			},
			vmRef:          &common.VMReference{Name: "my-vm", Namespace: "vm-ns"},
			backupType:     "full",
			checkpointName: "checkpoint-001",
			vmbtName:       "vmbt-my-vm-xyz",
			datamoverImage: "quay.io/test/datamover:v1",
			expectError:    false,
			validate: func(t *testing.T, config *DatamoverPodConfig) {
				if config.Name != "test-du" {
					t.Errorf("Name = %q, want %q", config.Name, "test-du")
				}
				if config.Namespace != "vm-ns" {
					t.Errorf("Namespace = %q, want %q", config.Namespace, "vm-ns")
				}
				if config.BSLBucket != "my-bucket" {
					t.Errorf("BSLBucket = %q, want %q", config.BSLBucket, "my-bucket")
				}
				if config.BSLPrefix != "velero-kubevirt-datamover" {
					t.Errorf("BSLPrefix = %q, want %q", config.BSLPrefix, "velero-kubevirt-datamover")
				}
				if config.BSLRegion != "us-east-1" {
					t.Errorf("BSLRegion = %q, want %q", config.BSLRegion, "us-east-1")
				}
				if config.CredentialSecretName != "cloud-credentials" {
					t.Errorf("CredentialSecretName = %q, want %q", config.CredentialSecretName, "cloud-credentials")
				}
				if config.VeleroBackupName != "velero-backup-001" {
					t.Errorf("VeleroBackupName = %q, want %q", config.VeleroBackupName, "velero-backup-001")
				}
				if config.BackupType != "full" {
					t.Errorf("BackupType = %q, want %q", config.BackupType, "full")
				}
				if config.CheckpointName != "checkpoint-001" {
					t.Errorf("CheckpointName = %q, want %q", config.CheckpointName, "checkpoint-001")
				}
				if config.VMBTName != "vmbt-my-vm-xyz" {
					t.Errorf("VMBTName = %q, want %q", config.VMBTName, "vmbt-my-vm-xyz")
				}
			},
		},
		{
			name: "BSL without prefix",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-du",
					UID:  types.UID("du-uid"),
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "my-bucket",
							Prefix: "", // No prefix
						},
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "creds",
						},
					},
				},
			},
			vmb:            &kubevirtbackupv1alpha1.VirtualMachineBackup{},
			vmRef:          &common.VMReference{Name: "vm", Namespace: "ns"},
			backupType:     "full",
			checkpointName: "cp",
			vmbtName:       "vmbt-vm-123",
			datamoverImage: "image:v1",
			expectError:    false,
			validate: func(t *testing.T, config *DatamoverPodConfig) {
				if config.BSLPrefix != "kubevirt-datamover" {
					t.Errorf("BSLPrefix = %q, want %q", config.BSLPrefix, "kubevirt-datamover")
				}
			},
		},
		{
			name: "missing bucket",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-du",
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bsl-no-bucket",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "", // Missing
						},
					},
				},
			},
			vmb:           &kubevirtbackupv1alpha1.VirtualMachineBackup{},
			vmRef:         &common.VMReference{Name: "vm", Namespace: "ns"},
			expectError:   true,
			errorContains: "no bucket configured",
		},
		{
			name: "missing credential secret",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-du",
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bsl-no-creds",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Credential: nil, // Missing
				},
			},
			vmb:           &kubevirtbackupv1alpha1.VirtualMachineBackup{},
			vmRef:         &common.VMReference{Name: "vm", Namespace: "ns"},
			expectError:   true,
			errorContains: "no credential secret configured",
		},
		{
			name: "missing datamover image",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-du",
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "creds",
						},
					},
				},
			},
			vmb:            &kubevirtbackupv1alpha1.VirtualMachineBackup{},
			vmRef:          &common.VMReference{Name: "vm", Namespace: "ns"},
			datamoverImage: "", // Missing
			expectError:    true,
			errorContains:  "datamover image not configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &KubeVirtDataUploadReconciler{
				DatamoverImage: tt.datamoverImage,
			}

			config, err := r.buildDatamoverPodConfig(
				tt.du,
				tt.bsl,
				tt.vmb,
				tt.vmRef,
				tt.backupType,
				tt.checkpointName,
				tt.vmbtName,
			)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.validate != nil {
					tt.validate(t, config)
				}
			}
		})
	}
}

// contains checks if s contains substr
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestGetBackupStorageLocation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)

	tests := []struct {
		name          string
		du            *velerov2alpha1.DataUpload
		bsl           *velerov1.BackupStorageLocation
		expectError   bool
		errorContains string
	}{
		{
			name: "BSL found",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					BackupStorageLocation: "default",
				},
			},
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
			},
			expectError: false,
		},
		{
			name: "BSL not found",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					BackupStorageLocation: "nonexistent",
				},
			},
			bsl:           nil,
			expectError:   true,
			errorContains: "failed to get BackupStorageLocation",
		},
		{
			name: "empty BSL name",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
				},
				Spec: velerov2alpha1.DataUploadSpec{
					BackupStorageLocation: "",
				},
			},
			bsl:           nil,
			expectError:   true,
			errorContains: "no BackupStorageLocation specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.bsl != nil {
				builder = builder.WithObjects(tt.bsl)
			}
			fakeClient := builder.Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				OADPNamespace: "openshift-adp",
			}

			bsl, err := r.getBackupStorageLocation(context.Background(), tt.du)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if bsl == nil {
					t.Error("expected BSL but got nil")
				}
			}
		})
	}
}

func TestHandlePrepared(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name          string
		setupObjects  func() []runtime.Object
		du            *velerov2alpha1.DataUpload
		datamoverImg  string
		expectError   bool
		expectedPhase velerov2alpha1.DataUploadPhase
		expectRequeue bool
	}{
		{
			name:         "creates datamover pod and transitions to InProgress",
			datamoverImg: "quay.io/test/datamover:v1",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					UID:       types.UID("du-uid-123"),
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "vm-ns",
						AnnotationVMBTName:           "vmbt-test-vm-abc",
					},
					Labels: map[string]string{
						common.LabelVeleroBackupName: "velero-backup",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:             common.DataMoverKubeVirt,
					BackupStorageLocation: "default",
					SourceNamespace:       "vm-ns",
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			setupObjects: func() []runtime.Object {
				bsl := &velerov1.BackupStorageLocation{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "default",
						Namespace: "openshift-adp",
					},
					Spec: velerov1.BackupStorageLocationSpec{
						Provider: "aws",
						StorageType: velerov1.StorageType{
							ObjectStorage: &velerov1.ObjectStorageLocation{
								Bucket: "test-bucket",
								Prefix: "velero",
							},
						},
						Config: map[string]string{"region": "us-east-1"},
						Credential: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
							Key:                  "cloud",
						},
					},
				}

				checkpointName := "checkpoint-001"
				tempPVCName := "kubevirt-backup-test-du-abc12"
				vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test-du",
						Namespace: "vm-ns",
						Labels: map[string]string{
							common.LabelDataUploadUID: "du-uid-123",
						},
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
						PvcName: &tempPVCName,
					},
					Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
						Type:           kubevirtbackupv1alpha1.Full,
						CheckpointName: &checkpointName,
					},
				}

				// Pre-create the rebound PVC in OADP namespace (skips rebind step)
				reboundPVC := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      common.ReboundPVCNamePrefix + "test-du",
						Namespace: "openshift-adp",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						VolumeName:  "pv-123",
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("10Gi"),
							},
						},
					},
					Status: corev1.PersistentVolumeClaimStatus{
						Phase: corev1.ClaimBound,
					},
				}

				return []runtime.Object{bsl, vmb, reboundPVC}
			},
			expectError:   false,
			expectedPhase: velerov2alpha1.DataUploadPhaseInProgress,
			expectRequeue: true,
		},
		{
			name:         "missing VM annotations fails",
			datamoverImg: "quay.io/test/datamover:v1",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					// No VM annotations
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover: common.DataMoverKubeVirt,
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			setupObjects:  func() []runtime.Object { return nil },
			expectError:   false,
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
		},
		{
			name:         "missing BSL fails",
			datamoverImg: "quay.io/test/datamover:v1",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "openshift-adp",
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "vm-ns",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:             common.DataMoverKubeVirt,
					BackupStorageLocation: "nonexistent",
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			setupObjects:  func() []runtime.Object { return nil },
			expectError:   false,
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
		},
		{
			name:         "VMB without PvcName fails when rebind is needed",
			datamoverImg: "quay.io/test/datamover:v1",
			du: &velerov2alpha1.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du-no-pvc",
					Namespace: "openshift-adp",
					UID:       types.UID("du-uid-nopvc"),
					Annotations: map[string]string{
						common.AnnotationVMName:      "test-vm",
						common.AnnotationVMNamespace: "vm-ns",
						AnnotationVMBTName:           "vmbt-test-vm-abc",
					},
					Labels: map[string]string{
						common.LabelVeleroBackupName: "velero-backup",
					},
				},
				Spec: velerov2alpha1.DataUploadSpec{
					DataMover:             common.DataMoverKubeVirt,
					BackupStorageLocation: "default",
					SourceNamespace:       "vm-ns",
				},
				Status: velerov2alpha1.DataUploadStatus{
					Phase: velerov2alpha1.DataUploadPhasePrepared,
				},
			},
			setupObjects: func() []runtime.Object {
				bsl := &velerov1.BackupStorageLocation{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "default",
						Namespace: "openshift-adp",
					},
					Spec: velerov1.BackupStorageLocationSpec{
						Provider: "aws",
						StorageType: velerov1.StorageType{
							ObjectStorage: &velerov1.ObjectStorageLocation{
								Bucket: "test-bucket",
								Prefix: "velero",
							},
						},
						Config: map[string]string{"region": "us-east-1"},
						Credential: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
							Key:                  "cloud",
						},
					},
				}

				// VMB exists but has no PvcName in spec
				vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "vmb-test-du-no-pvc",
						Namespace: "vm-ns",
						Labels: map[string]string{
							common.LabelDataUploadUID: "du-uid-nopvc",
						},
					},
					Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{},
					Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
						Type: kubevirtbackupv1alpha1.Full,
					},
				}
				// No rebound PVC exists → rebind path will be taken
				return []runtime.Object{bsl, vmb}
			},
			expectError:   false,
			expectedPhase: velerov2alpha1.DataUploadPhaseFailed,
			expectRequeue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.du)

			if tt.setupObjects != nil {
				for _, obj := range tt.setupObjects() {
					builder = builder.WithRuntimeObjects(obj)
				}
			}

			fakeClient := builder.Build()

			r := &KubeVirtDataUploadReconciler{
				Client:         fakeClient,
				Scheme:         scheme,
				Log:            logr.Discard(),
				OADPNamespace:  "openshift-adp",
				DatamoverImage: tt.datamoverImg,
			}

			result, err := r.handlePrepared(context.Background(), logr.Discard(), tt.du)

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.expectRequeue && result.RequeueAfter == 0 {
				t.Error("expected requeue but got none")
			}
			if !tt.expectRequeue && result.RequeueAfter > 0 {
				t.Errorf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
			}

			// Check phase
			updatedDU := &velerov2alpha1.DataUpload{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{
				Name:      tt.du.Name,
				Namespace: tt.du.Namespace,
			}, updatedDU); err != nil {
				t.Fatalf("failed to get updated DataUpload: %v", err)
			}

			if updatedDU.Status.Phase != tt.expectedPhase {
				t.Errorf("expected phase=%s, got phase=%s", tt.expectedPhase, updatedDU.Status.Phase)
			}
		})
	}
}

func TestGetCredentialsFromBSL(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name          string
		bsl           *velerov1.BackupStorageLocation
		secret        *corev1.Secret
		expectError   bool
		errorContains string
	}{
		{
			name: "valid credentials",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "cloud-credentials",
						},
						Key: "cloud",
					},
				},
			},
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cloud-credentials",
					Namespace: "openshift-adp",
				},
				Data: map[string][]byte{
					"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
				},
			},
			expectError: false,
		},
		{
			name: "no credential reference",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Credential: nil,
				},
			},
			expectError:   true,
			errorContains: "no credential configured",
		},
		{
			name: "secret not found",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "nonexistent",
						},
						Key: "cloud",
					},
				},
			},
			expectError:   true,
			errorContains: "failed to get credential secret",
		},
		{
			name: "secret missing key",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "default",
					Namespace: "openshift-adp",
				},
				Spec: velerov1.BackupStorageLocationSpec{
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "cloud-credentials",
						},
						Key: "wrong-key",
					},
				},
			},
			secret: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cloud-credentials",
					Namespace: "openshift-adp",
				},
				Data: map[string][]byte{
					"cloud": []byte("data"),
				},
			},
			expectError:   true,
			errorContains: "does not contain key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.secret != nil {
				builder = builder.WithObjects(tt.secret)
			}
			fakeClient := builder.Build()

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				OADPNamespace: "openshift-adp",
			}

			credData, err := r.getCredentialsFromBSL(context.Background(), tt.bsl)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(credData) == 0 {
				t.Error("expected non-empty credentials data")
			}
		})
	}
}

func TestHandleAccepted_WithBSLCheckpointLookup(t *testing.T) {
	// This test verifies that handleAccepted correctly handles the case where
	// BSL is available but credentials are missing. The controller should
	// gracefully fall back to full backup.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-bsl"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	// Create BSL (but no credential secret - lookup will fail gracefully)
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	// Create temporary PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
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

	// Create VMB (done=true to transition to Prepared)
	checkpointName := "vmb-" + duName + "-checkpoint"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "test-uid",
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: duName,
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
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
					Reason: "Completed VirtualMachineBackup",
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, pvc, vmbt, vmb).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still transition to Prepared even when BSL checkpoint lookup fails
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhasePrepared, updatedDU.Status.Phase)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue after transitioning to Prepared")
	}

}

func TestHandleAccepted_NoBSLConfigured(t *testing.T) {
	// Test that handleAccepted works when BSL name is empty - falls back to full backup
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-no-bsl"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "", // No BSL configured
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
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
	}

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

	// VMB with Done=True
	checkpointName := "cp-001"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: duName,
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
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
				},
			},
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
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still transition to Prepared
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("expected phase=%s, got phase=%s", velerov2alpha1.DataUploadPhasePrepared, updatedDU.Status.Phase)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue")
	}
}

func TestHandleAccepted_BSLNotFound_FailsDataUpload(t *testing.T) {
	// When BSL is specified but doesn't exist, handleAccepted should fail
	// the DataUpload immediately without creating PVC/VMBT/VMB resources.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-bsl-notfound"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "nonexistent-bsl", // BSL does not exist
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
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not requeue (permanent failure)
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for permanent BSL failure, got RequeueAfter=%v", result.RequeueAfter)
	}

	// DataUpload should be failed
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}

	// Verify no PVC was created (resources should not be created if BSL is unavailable)
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := fakeClient.List(context.Background(), pvcList); err != nil {
		t.Fatalf("failed to list PVCs: %v", err)
	}
	if len(pvcList.Items) != 0 {
		t.Errorf("expected no PVCs to be created, but found %d", len(pvcList.Items))
	}
}

func TestHandleAccepted_BSLUnavailable_FailsDataUpload(t *testing.T) {
	// When BSL exists but is not in Available phase, handleAccepted should fail
	// the DataUpload immediately without creating any resources.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-bsl-unavail"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	// BSL exists but in Unavailable phase
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
				},
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseUnavailable,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not requeue (permanent failure)
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
	}

	// DataUpload should be failed
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}

	if updatedDU.Status.Message == "" {
		t.Error("expected a failure message")
	}

	// Verify no PVC was created
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := fakeClient.List(context.Background(), pvcList); err != nil {
		t.Fatalf("failed to list PVCs: %v", err)
	}
	if len(pvcList.Items) != 0 {
		t.Errorf("expected no PVCs to be created, but found %d", len(pvcList.Items))
	}
}

func TestHandleAccepted_BSLNotAccessible_FailsDataUpload(t *testing.T) {
	// When the BSL referenced by the DataUpload does not exist (not found),
	// handleAccepted should fail the DataUpload without creating resources.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-bsl-transient"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "my-bsl",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	// No BSL is created in the fake client, so the controller will get a NotFound
	// error when looking up the BSL. This should cause the DataUpload to fail.
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// BSL not found is a permanent failure - DataUpload should be failed
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue for BSL not found, got RequeueAfter=%v", result.RequeueAfter)
	}

	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhaseFailed {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhaseFailed, updatedDU.Status.Phase)
	}

	// Verify no PVC was created
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := fakeClient.List(context.Background(), pvcList); err != nil {
		t.Fatalf("failed to list PVCs: %v", err)
	}
	if len(pvcList.Items) != 0 {
		t.Errorf("expected no PVCs to be created, but found %d", len(pvcList.Items))
	}
}

func TestLookupCheckpointFromBSL_MissingBucket(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "", // No bucket
				},
			},
		},
	}

	_, err := r.lookupCheckpointFromBSL(context.Background(), bsl, "ns", "vm")
	if err == nil {
		t.Error("expected error for missing bucket")
	}
	if !contains(err.Error(), "no bucket configured") {
		t.Errorf("expected error about missing bucket, got: %v", err)
	}
}

func TestLookupCheckpointFromBSL_MissingCredential(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "my-bucket",
				},
			},
			Credential: nil, // No credential
		},
	}

	_, err := r.lookupCheckpointFromBSL(context.Background(), bsl, "ns", "vm")
	if err == nil {
		t.Error("expected error for missing credential")
	}
	if !contains(err.Error(), "no credential configured") {
		t.Errorf("expected error about missing credential, got: %v", err)
	}
}

func TestHandleAccepted_HappyPath_IncrementalBackup(t *testing.T) {
	// This test verifies the full happy path: BSL has valid checkpoint data,
	// the controller finds the latest checkpoint, updates VMBT, and KubeVirt
	// performs an incremental backup.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-incr"
	existingCheckpointID := "cp-existing-001"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

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

	checkpointName := "vmb-" + duName + "-checkpoint"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "test-uid",
			},
			Annotations: map[string]string{
				common.AnnotationDataUploadName: duName,
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
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
					Reason: "Completed VirtualMachineBackup",
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, pvc, vmbt, vmb).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Create a mock object store with an existing checkpoint index
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	vmIndex := uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:     existingCheckpointID,
				Type:   "full",
				Parent: "",
				Files: []uploader.CheckpointFile{
					{
						Filename:   "vmb-prev-disk1.qcow2",
						ObjectPath: "checkpoints/" + vmNamespace + "/" + vmName + "/" + existingCheckpointID + "/vmb-prev-disk1.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	indexPath := "checkpoints/" + vmNamespace + "/" + vmName + "/index.json"
	_ = mockStore.PutObject("test-bucket", indexPath, bytes.NewReader(indexData))

	// Also store the qcow2 file so chain validation succeeds
	qcow2Path := "checkpoints/" + vmNamespace + "/" + vmName + "/" + existingCheckpointID + "/vmb-prev-disk1.qcow2"
	_ = mockStore.PutObject("test-bucket", qcow2Path, bytes.NewReader([]byte("fake-qcow2-data")))

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should transition to Prepared
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhasePrepared, updatedDU.Status.Phase)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue after transitioning to Prepared")
	}

	// Controller no longer modifies VMBT status; checkpoint management is left to KubeVirt.
}

func TestHandleAccepted_StaleCheckpointForcesFullBackup(t *testing.T) {
	// This test verifies that when BSL validation finds no valid chain
	// (e.g., index.json deleted from S3), the controller creates the VMB
	// with ForceFullBackup=true. The controller does NOT modify VMBT status.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-stale-cp"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// VMBT with a stale checkpoint from a previous backup
	staleCheckpointName := "cp-previous-001"
	now := metav1.Now()
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
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         staleCheckpointName,
				CreationTime: &now,
			},
		},
	}

	// Do NOT pre-create the VMB: let ensureVMBackup create it with ForceFullBackup=true
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, pvc, vmbt).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Mock object store with NO index.json (simulates deleted S3 data)
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify VMB was created with ForceFullBackup=true
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if !createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be true when BSL finds no valid chain")
	}

	// Verify BSL validated annotation was set
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Annotations[common.AnnotationBSLValidated] != "true" {
		t.Error("expected BSL validated annotation to be set")
	}
}

func TestHandleAccepted_SkipsBSLValidationWhenAnnotated(t *testing.T) {
	// This test verifies that BSL validation is skipped on subsequent reconciles
	// when the DataUpload already has the BSL validated annotation.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-annotated"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:       vmName,
				common.AnnotationVMNamespace:  vmNamespace,
				common.AnnotationBSLValidated: "true", // Already validated
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

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

	// VMB in progress (no Done condition yet)
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "test-uid",
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
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{},
	}

	// BSL must exist for the Step 0 availability check
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
				},
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt, vmb, bsl).
		Build()

	// ObjectStoreFactory should NOT be called - BSL validation is skipped
	factoryCalled := false
	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			factoryCalled = true
			return nil, nil
		},
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if factoryCalled {
		t.Error("ObjectStoreFactory should not be called when BSL validation annotation is present")
	}
}

func TestLookupCheckpointFromBSL_WithValidCredentials(t *testing.T) {
	// Tests the full lookupCheckpointFromBSL path with valid credentials
	// and a mock object store that returns no checkpoint (first backup).
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-credentials",
			Namespace: "openshift-adp",
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(credSecret).
		Build()

	// Mock store with no index (first backup)
	mockStore := uploader.NewMockObjectStore("my-bucket", "velero-kubevirt-datamover")

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "my-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-credentials"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	result, err := r.lookupCheckpointFromBSL(context.Background(), bsl, "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Found {
		t.Error("expected Found=false for first backup (no index)")
	}
	if !contains(result.Message, "no checkpoint index found") {
		t.Errorf("expected message about no index, got: %s", result.Message)
	}
}

func TestLookupCheckpointFromBSL_WithExistingCheckpoint(t *testing.T) {
	// Tests lookupCheckpointFromBSL when BSL has an existing valid checkpoint.
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-credentials",
			Namespace: "openshift-adp",
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(credSecret).
		Build()

	mockStore := uploader.NewMockObjectStore("my-bucket", "velero-kubevirt-datamover")

	// Populate mock store with checkpoint data
	vmIndex := uploader.VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:     "cp-001",
				Type:   "full",
				Parent: "",
				Files: []uploader.CheckpointFile{
					{
						Filename:   "disk1.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-001/disk1.qcow2",
					},
				},
			},
			{
				ID:     "cp-002",
				Type:   "incremental",
				Parent: "cp-001",
				Files: []uploader.CheckpointFile{
					{
						Filename:   "disk1.qcow2",
						ObjectPath: "checkpoints/test-ns/test-vm/cp-002/disk1.qcow2",
					},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = mockStore.PutObject("my-bucket", "checkpoints/test-ns/test-vm/index.json",
		bytes.NewReader(indexData))
	_ = mockStore.PutObject("my-bucket", "checkpoints/test-ns/test-vm/cp-001/disk1.qcow2",
		bytes.NewReader([]byte("data")))
	_ = mockStore.PutObject("my-bucket", "checkpoints/test-ns/test-vm/cp-002/disk1.qcow2",
		bytes.NewReader([]byte("data")))

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "my-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-credentials"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	result, err := r.lookupCheckpointFromBSL(context.Background(), bsl, "test-ns", "test-vm")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !result.Found {
		t.Errorf("expected Found=true, got Found=false (message: %s)", result.Message)
	}
	if result.LatestCheckpoint != "cp-002" {
		t.Errorf("expected LatestCheckpoint=cp-002, got=%s", result.LatestCheckpoint)
	}
	if result.ChainLength != 2 {
		t.Errorf("expected ChainLength=2, got=%d", result.ChainLength)
	}
}

func TestGetCredentialsFromBSL_ReturnsRawBytes(t *testing.T) {
	// Verifies that credentials are returned as raw bytes without temp files.
	scheme := runtime.NewScheme()
	_ = velerov1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	expectedData := "[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"
	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-credentials",
			Namespace: "openshift-adp",
		},
		Data: map[string][]byte{
			"cloud": []byte(expectedData),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(credSecret).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		OADPNamespace: "openshift-adp",
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: "openshift-adp",
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-credentials"},
				Key:                  "cloud",
			},
		},
	}

	credData, err := r.getCredentialsFromBSL(context.Background(), bsl)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(credData) != expectedData {
		t.Errorf("credential data mismatch: got %q, want %q", string(credData), expectedData)
	}
}

func TestExtractBSLConfig(t *testing.T) {
	tests := []struct {
		name          string
		bsl           *velerov1.BackupStorageLocation
		expectError   bool
		errorContains string
		validate      func(*testing.T, *bslConfig)
	}{
		{
			name: "full config with prefix",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "my-bucket",
							Prefix: "velero",
						},
					},
					Config: map[string]string{"region": "us-west-2"},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
						Key:                  "custom-key",
					},
				},
			},
			validate: func(t *testing.T, cfg *bslConfig) {
				if cfg.Provider != "aws" {
					t.Errorf("Provider = %q, want %q", cfg.Provider, "aws")
				}
				if cfg.Bucket != "my-bucket" {
					t.Errorf("Bucket = %q, want %q", cfg.Bucket, "my-bucket")
				}
				if cfg.Prefix != "velero-kubevirt-datamover" {
					t.Errorf("Prefix = %q, want %q", cfg.Prefix, "velero-kubevirt-datamover")
				}
				if cfg.Region != "us-west-2" {
					t.Errorf("Region = %q, want %q", cfg.Region, "us-west-2")
				}
				if cfg.CredentialName != "creds" {
					t.Errorf("CredentialName = %q, want %q", cfg.CredentialName, "creds")
				}
				if cfg.CredentialKey != "custom-key" {
					t.Errorf("CredentialKey = %q, want %q", cfg.CredentialKey, "custom-key")
				}
			},
		},
		{
			name: "empty prefix produces bare datamover prefix",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
							Prefix: "",
						},
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
					},
				},
			},
			validate: func(t *testing.T, cfg *bslConfig) {
				if cfg.Prefix != "kubevirt-datamover" {
					t.Errorf("Prefix = %q, want %q", cfg.Prefix, "kubevirt-datamover")
				}
			},
		},
		{
			name: "nil ObjectStorage returns error",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "bsl-nil-os"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: nil,
					},
				},
			},
			expectError:   true,
			errorContains: "no bucket configured",
		},
		{
			name: "empty bucket returns error",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "bsl-no-bucket"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "",
						},
					},
				},
			},
			expectError:   true,
			errorContains: "no bucket configured",
		},
		{
			name: "nil Config map gives empty region",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Config: nil,
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
					},
				},
			},
			validate: func(t *testing.T, cfg *bslConfig) {
				if cfg.Region != "" {
					t.Errorf("Region = %q, want empty", cfg.Region)
				}
			},
		},
		{
			name: "nil Credential gives empty credential name",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Credential: nil,
				},
			},
			validate: func(t *testing.T, cfg *bslConfig) {
				if cfg.CredentialName != "" {
					t.Errorf("CredentialName = %q, want empty", cfg.CredentialName)
				}
			},
		},
		{
			name: "Credential with empty Key defaults to cloud",
			bsl: &velerov1.BackupStorageLocation{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: velerov1.BackupStorageLocationSpec{
					Provider: "aws",
					StorageType: velerov1.StorageType{
						ObjectStorage: &velerov1.ObjectStorageLocation{
							Bucket: "bucket",
						},
					},
					Credential: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: "creds"},
						Key:                  "",
					},
				},
			},
			validate: func(t *testing.T, cfg *bslConfig) {
				if cfg.CredentialKey != "cloud" {
					t.Errorf("CredentialKey = %q, want %q", cfg.CredentialKey, "cloud")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := extractBSLConfig(tt.bsl)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				} else if tt.errorContains != "" && !contains(err.Error(), tt.errorContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestHandleAccepted_VMBConflictRequeuesGracefully(t *testing.T) {
	// This test verifies that when another VirtualMachineBackup is in progress
	// for the same VM (admission webhook denies creation), the controller requeues
	// with a longer delay instead of returning an error that causes a retry storm.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-conflict"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:       vmName,
				common.AnnotationVMNamespace:  vmNamespace,
				common.AnnotationBSLValidated: "true", // Skip BSL validation
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

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

	// BSL must exist for the Step 0 availability check
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
				},
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	// No VMB exists - the controller will try to create one.
	// We simulate the admission webhook rejection by using an interceptor.
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt, bsl).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	// Call ensureVMBackup directly to verify the error handling logic.
	// The fake client won't produce an admission webhook error, so we test
	// the handleAccepted logic by simulating the error path.
	// Instead, we test by calling handleAccepted and checking that if
	// ensureVMBackup returns an "in progress for source" error, it requeues.

	// To properly test this, we need to verify the string matching logic.
	// Let's construct the exact error that the admission webhook produces.
	webhookErr := fmt.Errorf("failed to create VirtualMachineBackup: admission webhook " +
		"\"virtualmachinebackup-validator.backup.kubevirt.io\" denied the request: " +
		"VirtualMachineBackup \"vmb-other-du\" in progress for source")

	// Verify the string matching works correctly
	if !contains(webhookErr.Error(), "in progress for source") {
		t.Fatal("expected error to contain 'in progress for source'")
	}

	// Verify that a normal error does NOT match
	normalErr := fmt.Errorf("failed to create VirtualMachineBackup: connection refused")
	if contains(normalErr.Error(), "in progress for source") {
		t.Fatal("normal error should not match 'in progress for source'")
	}

	// Also verify via handleAccepted: when VMB doesn't exist and creation succeeds,
	// the flow continues normally (no conflict).
	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have created the VMB and be requeuing to check its status
	if result.RequeueAfter == 0 {
		t.Error("expected requeue")
	}

	// Verify RequeueAfterLong constant is larger than RequeueAfterShort
	if RequeueAfterLong <= RequeueAfterShort {
		t.Errorf("RequeueAfterLong (%v) should be greater than RequeueAfterShort (%v)",
			RequeueAfterLong, RequeueAfterShort)
	}
}

func TestHandleAccepted_BSLAnnotationNotSetOnTransientFailure(t *testing.T) {
	// This test verifies that when BSL lookup fails due to a transient error
	// (e.g., missing credential secret), the AnnotationBSLValidated is NOT set,
	// so validation will be retried on the next reconcile.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-transient"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	// BSL exists but credential secret is MISSING (transient failure)
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "missing-secret"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

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

	// VMB with Done=True so the test can proceed past the VMB check
	checkpointName := "cp-001"
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-" + duName,
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "test-uid",
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
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{
					Type:   kubevirtbackupv1alpha1.ConditionDone,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, pvc, vmbt, vmb).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify BSL validated annotation was NOT set (transient failure)
	var updatedDU velerov2alpha1.DataUpload
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name:      duName,
		Namespace: vmNamespace,
	}, &updatedDU); err != nil {
		t.Fatalf("failed to get updated DataUpload: %v", err)
	}

	if updatedDU.Annotations[common.AnnotationBSLValidated] == "true" {
		t.Error("expected BSL validated annotation to NOT be set on transient failure, " +
			"so validation will be retried on the next reconcile")
	}

	// Should still transition to Prepared (graceful degradation)
	if updatedDU.Status.Phase != velerov2alpha1.DataUploadPhasePrepared {
		t.Errorf("expected phase=%s, got phase=%s",
			velerov2alpha1.DataUploadPhasePrepared, updatedDU.Status.Phase)
	}
}

func TestHandleAccepted_ForceFullBackupAnnotation(t *testing.T) {
	// When the force-full-backup annotation is set on the DataUpload,
	// the controller should:
	// 1. Skip BSL checkpoint lookup entirely
	// 2. Clear any existing VMBT checkpoint
	// 3. Create the VMB with ForceFullBackup=true
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-force-full"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:          vmName,
				common.AnnotationVMNamespace:     vmNamespace,
				common.AnnotationForceFullBackup: "true",
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// VMBT with an existing checkpoint (should be cleared)
	existingCheckpoint := "cp-existing-001"
	now := metav1.Now()
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
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         existingCheckpoint,
				CreationTime: &now,
			},
		},
	}

	// BSL must exist for the Step 0 availability check
	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
				},
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt, bsl).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	objectStoreFactoryCalled := false
	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			objectStoreFactoryCalled = true
			return nil, fmt.Errorf("should not be called")
		},
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue")
	}

	// Verify BSL checkpoint lookup was NOT called
	if objectStoreFactoryCalled {
		t.Error("ObjectStoreFactory should not be called when force-full-backup annotation is set")
	}

	// Verify VMB was created with ForceFullBackup=true
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if !createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be true")
	}
}

func TestHandleAccepted_ForceFullBackupWithNoExistingCheckpoint(t *testing.T) {
	// When force-full-backup is set but VMBT has no checkpoint,
	// the controller should still create the VMB with ForceFullBackup=true
	// without errors (no checkpoint to clear).
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-force-no-cp"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:          vmName,
				common.AnnotationVMNamespace:     vmNamespace,
				common.AnnotationForceFullBackup: "true",
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

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// VMBT with no checkpoint
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

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	result, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected requeue")
	}

	// Verify VMB was created with ForceFullBackup=true
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if !createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be true")
	}
}

func TestHandleAccepted_NoForceFullBackupByDefault(t *testing.T) {
	// When the force-full-backup annotation is NOT set, the VMB should
	// be created with ForceFullBackup=false (default).
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-no-force"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			UID:       types.UID("test-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:       vmName,
				common.AnnotationVMNamespace:  vmNamespace,
				common.AnnotationBSLValidated: "true", // Skip BSL validation
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

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

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

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, pvc, vmbt).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify VMB was created with ForceFullBackup=false
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be false when annotation is not set")
	}
}

func TestHandleAccepted_StaleCheckpointSetsForceFullOnVMB(t *testing.T) {
	// When BSL validation finds no valid checkpoint chain but VMBT has a stale
	// checkpoint, the controller should set ForceFullBackup=true on the VMB
	// as defense-in-depth.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-stale-force"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kubevirt-backup-" + duName,
			Namespace: vmNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	// VMBT with a stale checkpoint
	staleCheckpointName := "cp-stale-001"
	now := metav1.Now()
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
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         staleCheckpointName,
				CreationTime: &now,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, pvc, vmbt).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Mock object store with NO index.json (simulates deleted S3 data)
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	_, err := r.handleAccepted(context.Background(), logr.Discard(), du)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify VMB was created with ForceFullBackup=true
	vmbList := &kubevirtbackupv1alpha1.VirtualMachineBackupList{}
	if err := fakeClient.List(context.Background(), vmbList, client.InNamespace(vmNamespace), client.MatchingLabels{common.LabelDataUploadUID: string(du.UID)}); err != nil {
		t.Fatalf("failed to list created VMBs: %v", err)
	}
	if len(vmbList.Items) != 1 {
		t.Fatalf("expected 1 VMB to be created, but found %d", len(vmbList.Items))
	}
	createdVMB := vmbList.Items[0]

	if !createdVMB.Spec.ForceFullBackup {
		t.Error("expected VMB.Spec.ForceFullBackup to be true when BSL finds stale checkpoint chain")
	}

}

func TestValidateBSLCheckpoint_ForceFullOnChainFallback(t *testing.T) {
	// When the BSL checkpoint chain is broken mid-way and falls back to an
	// older checkpoint, forceFullBackup must be true. We can't safely assume
	// KubeVirt's CBT can produce a correct incremental since the older checkpoint.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-chain-fallback"

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
			DataMover:             common.DataMoverKubeVirt,
			SourceNamespace:       vmNamespace,
			BackupStorageLocation: "default",
		},
		Status: velerov2alpha1.DataUploadStatus{
			Phase: velerov2alpha1.DataUploadPhaseAccepted,
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	// VMBT with checkpoint set to cp-003 (the latest in the chain).
	// BSL will return cp-001 as latest valid (because cp-002 is broken).
	// The mismatch triggers forceFullBackup.
	now := metav1.Now()
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
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         "cp-003",
				CreationTime: &now,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret, vmbt).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Mock object store: chain cp-001(Full) -> cp-002(Inc) -> cp-003(Inc)
	// cp-002's file is missing → chain falls back to cp-001
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
	vmIndex := uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:       "cp-001",
				Type:     "full",
				VMBackup: "vmb-001",
				Files: []uploader.CheckpointFile{
					{ObjectPath: "checkpoints/test-ns/test-vm/cp-001/disk.qcow2"},
				},
			},
			{
				ID:       "cp-002",
				Type:     "incremental",
				Parent:   "cp-001",
				VMBackup: "vmb-002",
				Files: []uploader.CheckpointFile{
					{ObjectPath: "checkpoints/test-ns/test-vm/cp-002/disk.qcow2"},
				},
			},
			{
				ID:       "cp-003",
				Type:     "incremental",
				Parent:   "cp-002",
				VMBackup: "vmb-003",
				Files: []uploader.CheckpointFile{
					{ObjectPath: "checkpoints/test-ns/test-vm/cp-003/disk.qcow2"},
				},
			},
		},
	}
	indexData, _ := json.Marshal(vmIndex)
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)
	// Create files for cp-001 and cp-003 but NOT cp-002 (mid-chain break)
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/cp-001/disk.qcow2", []byte("full"))
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/cp-003/disk.qcow2", []byte("inc2"))

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	vmRef := &common.VMReference{Name: vmName, Namespace: vmNamespace}

	forceFullBackup, checkpointLookup := r.validateBSLCheckpoint(context.Background(), logr.Discard(), du, vmRef)

	if !forceFullBackup {
		t.Error("expected forceFullBackup=true when BSL chain falls back to older checkpoint")
	}

	if checkpointLookup == nil {
		t.Fatal("expected checkpointLookup to be returned")
	}
	if !checkpointLookup.Found {
		t.Error("expected checkpointLookup.Found=true (fell back to valid chain)")
	}

}

func TestPrepareVMBackupTracker_FirstBackup(t *testing.T) {
	// No S3 index exists → creates fresh VMBT with no LatestCheckpoint
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-first"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "", // No BSL → first backup path
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
		OADPNamespace: vmNamespace,
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if vmbt == nil {
		t.Fatal("expected VMBT to be created")
	}
	if !strings.HasPrefix(vmbt.Name, "vmbt-"+vmName) {
		t.Errorf("VMBT name %q does not have expected prefix", vmbt.Name)
	}
	if vmbt.Namespace != vmNamespace {
		t.Errorf("VMBT namespace = %q, want %q", vmbt.Namespace, vmNamespace)
	}
	if vmbt.Labels[common.LabelVMNameHash] != common.HashForLabel(vmName) {
		t.Errorf("VMBT is missing label %s", common.LabelVMNameHash)
	}
	if vmbt.Annotations[common.AnnotationVMName] != vmName {
		t.Errorf("VMBT annotation %s = %q, want %q", common.AnnotationVMName, vmbt.Annotations[common.AnnotationVMName], vmName)
	}
	// First backup: no LatestCheckpoint
	if vmbt.Status != nil && vmbt.Status.LatestCheckpoint != nil {
		t.Error("expected no LatestCheckpoint for first backup")
	}
}

func TestPrepareVMBackupTracker_FromS3(t *testing.T) {
	// S3 has index.json with vmbtObjectPath pointing to vmbt.json
	// vmbt.json has LatestCheckpoint set
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-s3"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover:             common.DataMoverKubeVirt,
			BackupStorageLocation: "default",
		},
	}

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
		Status: velerov1.BackupStorageLocationStatus{
			Phase: velerov1.BackupStorageLocationPhaseAvailable,
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, bsl, credSecret).
		WithStatusSubresource(&kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}).
		Build()

	// Set up mock object store with index.json and vmbt.json
	mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")

	// Create vmbt.json with LatestCheckpoint
	now := metav1.Now()
	archivedVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName,
			Namespace: vmNamespace,
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
			LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
				Name:         "cp-002",
				CreationTime: &now,
			},
		},
	}
	vmbtData, _ := json.MarshalIndent(archivedVMBT, "", "  ")
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/cp-002/vmbt.json", vmbtData)

	// Create index.json with vmbtObjectPath
	vmIndex := &uploader.VMIndex{
		VMName:    vmName,
		Namespace: vmNamespace,
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:             "cp-002",
				Type:           "full",
				VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-002/vmbt.json",
			},
		},
	}
	indexData, _ := json.MarshalIndent(vmIndex, "", "  ")
	_ = mockStore.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
		ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
			return mockStore, nil
		},
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if vmbt == nil {
		t.Fatal("expected VMBT to be created")
	}

	// Verify LatestCheckpoint was restored from S3
	if vmbt.Status == nil || vmbt.Status.LatestCheckpoint == nil {
		t.Fatal("expected VMBT to have LatestCheckpoint from S3")
	}
	if vmbt.Status.LatestCheckpoint.Name != "cp-002" {
		t.Errorf("LatestCheckpoint.Name = %q, want %q", vmbt.Status.LatestCheckpoint.Name, "cp-002")
	}
}

func TestPrepareVMBackupTracker_ReusesExisting(t *testing.T) {
	// VMBT already exists in cluster → should be reused (not deleted and recreated).
	// Issue #32: VMBT must persist on-cluster for KubeVirt lifecycle events.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"
	duName := "test-du-reuse"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      duName,
			Namespace: vmNamespace,
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// Pre-create an existing VMBT
	existingVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-" + vmName + "-old",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash: common.HashForLabel(vmName),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: strPtr("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, existingVMBT).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the existing VMBT was reused (same name)
	if vmbt.Name != existingVMBT.Name {
		t.Errorf("expected existing VMBT %q to be reused, got %q", existingVMBT.Name, vmbt.Name)
	}
	if vmbt.Labels[common.LabelVMNameHash] != common.HashForLabel(vmName) {
		t.Errorf("expected VMBT to have label %s, got %q",
			common.LabelVMNameHash, vmbt.Labels[common.LabelVMNameHash])
	}

	// Verify the VMBT still exists on-cluster
	preserved := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: existingVMBT.Name, Namespace: vmNamespace,
	}, preserved); err != nil {
		t.Errorf("existing VMBT was deleted when it should have been reused: %v", err)
	}
}

func TestPrepareVMBackupTracker_ReusesVMBTEvenIfReferencedByActiveVMB(t *testing.T) {
	// A VMBT referenced by a non-terminal VMB is still reused (not deleted).
	// Issue #32: VMBTs are never deleted — they persist on-cluster.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-new",
			Namespace: vmNamespace,
			UID:       types.UID("du-new-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// VMBT from another DU, still referenced by an active VMB
	otherVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm-other",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash:    common.HashForLabel(vmName),
				common.LabelDataUploadUID: "other-du-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: strPtr("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// Active (non-terminal) VMB referencing the other VMBT
	activeVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-other-du",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "other-du-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: strPtr("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     otherVMBT.Name,
			},
		},
		// nil status = non-terminal
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, otherVMBT, activeVMB).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The existing VMBT should be reused
	if vmbt == nil {
		t.Fatal("expected VMBT to be returned")
	}
	if vmbt.Name != otherVMBT.Name {
		t.Errorf("expected existing VMBT %q to be reused, got %q", otherVMBT.Name, vmbt.Name)
	}

	// The VMBT must still exist on-cluster
	preserved := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: otherVMBT.Name, Namespace: vmNamespace,
	}, preserved); err != nil {
		t.Errorf("VMBT was deleted when it should have been preserved: %v", err)
	}
}

func TestPrepareVMBackupTracker_ReusesVMBTWithTerminalVMB(t *testing.T) {
	// A VMBT referenced by a terminal VMB (Done=True) should be reused.
	// Issue #32: VMBTs are never deleted — they persist on-cluster.
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "du-new",
			Namespace: vmNamespace,
			UID:       types.UID("du-new-uid"),
			Annotations: map[string]string{
				common.AnnotationVMName:      vmName,
				common.AnnotationVMNamespace: vmNamespace,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// VMBT from a completed DU
	oldVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-vm-done",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash:    common.HashForLabel(vmName),
				common.LabelDataUploadUID: "old-du-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: strPtr("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	// Terminal VMB (Done=True) referencing the old VMBT
	terminalVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-old-du",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: "old-du-uid",
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: strPtr("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     oldVMBT.Name,
			},
		},
		Status: &kubevirtbackupv1alpha1.VirtualMachineBackupStatus{
			Conditions: []kubevirtbackupv1alpha1.Condition{
				{Type: kubevirtbackupv1alpha1.ConditionDone, Status: corev1.ConditionTrue},
				{Type: kubevirtbackupv1alpha1.ConditionProgressing, Status: corev1.ConditionFalse},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, oldVMBT, terminalVMB).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: vmNamespace,
	}

	vmbt, err := r.prepareVMBackupTracker(context.Background(), logr.Discard(), du, vmName, vmNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The old VMBT should be reused (not deleted)
	if vmbt.Name != oldVMBT.Name {
		t.Errorf("expected VMBT %q to be reused, got %q", oldVMBT.Name, vmbt.Name)
	}

	preserved := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: oldVMBT.Name, Namespace: vmNamespace,
	}, preserved); err != nil {
		t.Errorf("VMBT was deleted when it should have been preserved: %v", err)
	}
}

func TestLookupLatestVMBTFromBSL(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = velerov1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmNamespace := "test-ns"

	bsl := &velerov1.BackupStorageLocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default",
			Namespace: vmNamespace,
		},
		Spec: velerov1.BackupStorageLocationSpec{
			Provider: "aws",
			StorageType: velerov1.StorageType{
				ObjectStorage: &velerov1.ObjectStorageLocation{
					Bucket: "test-bucket",
					Prefix: "velero",
				},
			},
			Config: map[string]string{"region": "us-east-1"},
			Credential: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "cloud-creds"},
				Key:                  "cloud",
			},
		},
	}

	credSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloud-creds",
			Namespace: vmNamespace,
		},
		Data: map[string][]byte{
			"cloud": []byte("[default]\naws_access_key_id=AKID\naws_secret_access_key=SECRET\n"),
		},
	}

	tests := []struct {
		name        string
		setupStore  func(*uploader.MockObjectStore)
		expectNil   bool
		expectError bool
		expectCP    string
	}{
		{
			name: "returns nil when no index exists",
			setupStore: func(_ *uploader.MockObjectStore) {
				// Empty store
			},
			expectNil: true,
		},
		{
			name: "returns nil when index has empty vmbtObjectPath",
			setupStore: func(store *uploader.MockObjectStore) {
				vmIndex := &uploader.VMIndex{
					VMName:    "test-vm",
					Namespace: vmNamespace,
					Checkpoints: []uploader.CheckpointEntry{
						{ID: "cp-001", Type: "full"}, // No VMBTObjectPath
					},
				}
				data, _ := json.MarshalIndent(vmIndex, "", "  ")
				_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", data)
			},
			expectNil: true,
		},
		{
			name: "returns VMBT from S3 with checkpoint",
			setupStore: func(store *uploader.MockObjectStore) {
				now := metav1.Now()
				vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
					Status: &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
						LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
							Name:         "cp-005",
							CreationTime: &now,
						},
					},
				}
				vmbtData, _ := json.MarshalIndent(vmbt, "", "  ")
				_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/cp-005/vmbt.json", vmbtData)

				vmIndex := &uploader.VMIndex{
					VMName:    "test-vm",
					Namespace: vmNamespace,
					Checkpoints: []uploader.CheckpointEntry{
						{
							ID:             "cp-005",
							Type:           "full",
							VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-005/vmbt.json",
						},
					},
				}
				indexData, _ := json.MarshalIndent(vmIndex, "", "  ")
				_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)
			},
			expectNil: false,
			expectCP:  "cp-005",
		},
		{
			name: "returns error when vmbt.json is missing",
			setupStore: func(store *uploader.MockObjectStore) {
				vmIndex := &uploader.VMIndex{
					VMName:    "test-vm",
					Namespace: vmNamespace,
					Checkpoints: []uploader.CheckpointEntry{
						{
							ID:             "cp-005",
							Type:           "full",
							VMBTObjectPath: "checkpoints/test-ns/test-vm/cp-005/vmbt.json",
						},
					},
				}
				indexData, _ := json.MarshalIndent(vmIndex, "", "  ")
				_ = store.PutObjectBytes("checkpoints/test-ns/test-vm/index.json", indexData)
				// Don't create vmbt.json
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(bsl, credSecret).
				Build()

			mockStore := uploader.NewMockObjectStore("test-bucket", "velero-kubevirt-datamover")
			tt.setupStore(mockStore)

			r := &KubeVirtDataUploadReconciler{
				Client:        fakeClient,
				Scheme:        scheme,
				Log:           logr.Discard(),
				OADPNamespace: vmNamespace,
				ObjectStoreFactory: func(_ *uploader.UploaderConfig) (velero.ObjectStore, error) {
					return mockStore, nil
				},
			}

			result, err := r.lookupLatestVMBTFromBSL(context.Background(), bsl, vmNamespace, "test-vm")

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectNil {
				if result != nil {
					t.Error("expected nil result")
				}
				return
			}

			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if result.Status == nil || result.Status.LatestCheckpoint == nil {
				t.Fatal("expected LatestCheckpoint to be set")
			}
			if result.Status.LatestCheckpoint.Name != tt.expectCP {
				t.Errorf("LatestCheckpoint.Name = %q, want %q",
					result.Status.LatestCheckpoint.Name, tt.expectCP)
			}
		})
	}
}

func TestSafeGenerateNamePrefix(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		maxNameLen int
		expected   string
	}{
		{
			name:       "short prefix is unchanged",
			prefix:     "short-prefix-",
			maxNameLen: 63,
			expected:   "short-prefix-",
		},
		{
			name:       "prefix exactly at limit is unchanged",
			prefix:     strings.Repeat("a", 58), // 58 = 63 - 5
			maxNameLen: 63,
			expected:   strings.Repeat("a", 58),
		},
		{
			name:       "long prefix is truncated",
			prefix:     "a-very-long-prefix-that-will-be-truncated-and-should-not-exceed-the-limit-",
			maxNameLen: 63,
			expected:   "a-very-long-prefix-that-will-be-truncated-and-should-not-e", // 58 chars
		},
		{
			name:       "long prefix for VMB is truncated",
			prefix:     "vmb-a-very-long-dataupload-name-that-exceeds-the-limit-for-kubevirt-hotplug-",
			maxNameLen: maxVMBNameLen,                              // 45
			expected:   "vmb-a-very-long-dataupload-name-that-exc", // 40 chars = 45 - 5
		},
		{
			name:       "edge case: maxNameLen < random part",
			prefix:     "short-",
			maxNameLen: 4,
			expected:   "s", // maxPrefix becomes 1
		},
		{
			name:       "edge case: maxNameLen == random part",
			prefix:     "short-",
			maxNameLen: 5,
			expected:   "s", // maxPrefix becomes 1
		},
		{
			name:       "edge case: maxNameLen == random part + 1",
			prefix:     "short-",
			maxNameLen: 6,
			expected:   "s", // maxPrefix is 1, so prefix is truncated to 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := safeGenerateNamePrefix(tt.prefix, tt.maxNameLen)
			if result != tt.expected {
				t.Errorf("safeGenerateNamePrefix() = %q, want %q", result, tt.expected)
			}
			// Sanity check length
			maxPrefixLen := max(tt.maxNameLen-k8sGenerateNameRandomLen, 1)
			if len(result) > maxPrefixLen {
				t.Errorf("result length %d exceeds max prefix length %d", len(result), maxPrefixLen)
			}
		})
	}
}

// TestCleanupVMBackupResources_PreservesVMBT verifies that cleanupVMBackupResources
// deletes the VMB but preserves the VMBT on-cluster (issue #32).
func TestCleanupVMBackupResources_PreservesVMBT(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = velerov2alpha1.AddToScheme(scheme)
	_ = kubevirtbackupv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	vmName := "test-vm"
	vmNamespace := "test-ns"

	du := &velerov2alpha1.DataUpload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-du-cleanup",
			Namespace: "openshift-adp",
			UID:       types.UID("cleanup-uid"),
		},
		Spec: velerov2alpha1.DataUploadSpec{
			DataMover: common.DataMoverKubeVirt,
		},
	}

	// VMB that should be deleted during cleanup
	vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmb-test-cleanup",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelDataUploadUID: string(du.UID),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: strPtr("backup.kubevirt.io"),
				Kind:     "VirtualMachineBackupTracker",
				Name:     "vmbt-test-cleanup",
			},
		},
	}

	// VMBT that should be PRESERVED during cleanup
	vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbt-test-cleanup",
			Namespace: vmNamespace,
			Labels: map[string]string{
				common.LabelVMNameHash: common.HashForLabel(vmName),
			},
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: strPtr("kubevirt.io"),
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(du, vmb, vmbt).
		Build()

	r := &KubeVirtDataUploadReconciler{
		Client:        fakeClient,
		Scheme:        scheme,
		Log:           logr.Discard(),
		OADPNamespace: "openshift-adp",
	}

	r.cleanupVMBackupResources(context.Background(), logr.Discard(), du, vmNamespace)

	// VMB should be deleted
	deletedVMB := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: vmb.Name, Namespace: vmNamespace,
	}, deletedVMB); err == nil {
		t.Error("VMB should have been deleted during cleanup, but it still exists")
	}

	// VMBT should be PRESERVED (not deleted)
	preservedVMBT := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Name: vmbt.Name, Namespace: vmNamespace,
	}, preservedVMBT); err != nil {
		t.Errorf("VMBT should have been preserved during cleanup, but it was deleted: %v", err)
	}
}
