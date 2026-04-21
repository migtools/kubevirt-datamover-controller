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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	velerov2alpha1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
)

func TestGetVMReference(t *testing.T) {
	tests := []struct {
		name          string
		dataUpload    *velerov2alpha1api.DataUpload
		expectedRef   *VMReference
		expectError   bool
		errorContains string
	}{
		{
			name:          "nil DataUpload",
			dataUpload:    nil,
			expectError:   true,
			errorContains: "DataUpload is nil",
		},
		{
			name: "no annotations",
			dataUpload: &velerov2alpha1api.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "velero",
				},
			},
			expectError:   true,
			errorContains: "has no annotations",
		},
		{
			name: "missing vm-name annotation",
			dataUpload: &velerov2alpha1api.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "velero",
					Annotations: map[string]string{
						"some-other-annotation": "value",
					},
				},
			},
			expectError:   true,
			errorContains: "missing required annotation",
		},
		{
			name: "empty vm-name annotation",
			dataUpload: &velerov2alpha1api.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "velero",
					Annotations: map[string]string{
						AnnotationVMName: "",
					},
				},
			},
			expectError:   true,
			errorContains: "missing required annotation",
		},
		{
			name: "vm-name set, namespace defaults to source",
			dataUpload: &velerov2alpha1api.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "velero",
					Annotations: map[string]string{
						AnnotationVMName: "my-vm",
					},
				},
				Spec: velerov2alpha1api.DataUploadSpec{
					SourceNamespace: "vm-namespace",
				},
			},
			expectedRef: &VMReference{
				Name:      "my-vm",
				Namespace: "vm-namespace",
			},
			expectError: false,
		},
		{
			name: "both vm-name and vm-namespace set",
			dataUpload: &velerov2alpha1api.DataUpload{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-du",
					Namespace: "velero",
					Annotations: map[string]string{
						AnnotationVMName:      "my-vm",
						AnnotationVMNamespace: "explicit-namespace",
					},
				},
				Spec: velerov2alpha1api.DataUploadSpec{
					SourceNamespace: "source-namespace",
				},
			},
			expectedRef: &VMReference{
				Name:      "my-vm",
				Namespace: "explicit-namespace",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := GetVMReference(tt.dataUpload)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
				assert.Nil(t, ref)
			} else {
				require.NoError(t, err)
				require.NotNil(t, ref)
				assert.Equal(t, tt.expectedRef.Name, ref.Name)
				assert.Equal(t, tt.expectedRef.Namespace, ref.Namespace)
			}
		})
	}
}

func TestGetVolumeMapForVm(t *testing.T) {
	tests := []struct {
		name     string
		vm       *kubevirtcorev1.VirtualMachine
		expected map[string]string
	}{
		{
			name:     "nil VM",
			vm:       nil,
			expected: map[string]string{},
		},
		{
			name: "VM with nil Template",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: nil,
				},
			},
			expected: map[string]string{},
		},
		{
			name: "VM with nil Volumes",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: nil,
						},
					},
				},
			},
			expected: map[string]string{},
		},
		{
			name: "VM with empty Volumes",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{},
						},
					},
				},
			},
			expected: map[string]string{},
		},
		{
			name: "VM with PVC volume",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "disk0",
									VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-1",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: map[string]string{"disk0": "pvc-1"},
		},
		{
			name: "VM with DataVolume",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "disk0",
									VolumeSource: kubevirtcorev1.VolumeSource{
										DataVolume: &kubevirtcorev1.DataVolumeSource{
											Name: "dv-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: map[string]string{"disk0": "dv-1"},
		},
		{
			name: "VM with MemoryDump volume",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "memory-dump",
									VolumeSource: kubevirtcorev1.VolumeSource{
										MemoryDump: &kubevirtcorev1.MemoryDumpVolumeSource{
											PersistentVolumeClaimVolumeSource: kubevirtcorev1.PersistentVolumeClaimVolumeSource{
												PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
													ClaimName: "memory-dump-pvc",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: map[string]string{"memory-dump": "memory-dump-pvc"},
		},
		{
			name: "VM with multiple volumes",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "disk0",
									VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-1",
											},
										},
									},
								},
								{
									Name: "disk1",
									VolumeSource: kubevirtcorev1.VolumeSource{
										DataVolume: &kubevirtcorev1.DataVolumeSource{
											Name: "dv-1",
										},
									},
								},
								{
									Name: "disk2",
									VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-2",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: map[string]string{
				"disk0": "pvc-1",
				"disk1": "dv-1",
				"disk2": "pvc-2",
			},
		},
		{
			name: "VM with non-PVC volumes only",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "cloudinit",
									VolumeSource: kubevirtcorev1.VolumeSource{
										CloudInitNoCloud: &kubevirtcorev1.CloudInitNoCloudSource{
											UserData: "test",
										},
									},
								},
								{
									Name: "containerDisk",
									VolumeSource: kubevirtcorev1.VolumeSource{
										ContainerDisk: &kubevirtcorev1.ContainerDiskSource{
											Image: "test-image",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: map[string]string{},
		},
		{
			name: "VM with mixed PVC and non-PVC volumes",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "disk0",
									VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-1",
											},
										},
									},
								},
								{
									Name: "cloudinit",
									VolumeSource: kubevirtcorev1.VolumeSource{
										CloudInitNoCloud: &kubevirtcorev1.CloudInitNoCloudSource{
											UserData: "test",
										},
									},
								},
								{
									Name: "disk1",
									VolumeSource: kubevirtcorev1.VolumeSource{
										DataVolume: &kubevirtcorev1.DataVolumeSource{
											Name: "dv-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: map[string]string{
				"disk0": "pvc-1",
				"disk1": "dv-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetVolumeMapForVm(tt.vm)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateVMIsRunning(t *testing.T) {
	tests := []struct {
		name          string
		vm            *kubevirtcorev1.VirtualMachine
		expectError   bool
		errorContains string
	}{
		{
			name:          "nil VM",
			vm:            nil,
			expectError:   true,
			errorContains: "VirtualMachine is nil",
		},
		{
			name: "VM is running",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					PrintableStatus: kubevirtcorev1.VirtualMachineStatusRunning,
				},
			},
			expectError: false,
		},
		{
			name: "VM is stopped",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					PrintableStatus: kubevirtcorev1.VirtualMachineStatusStopped,
				},
			},
			expectError:   true,
			errorContains: "not running",
		},
		{
			name: "VM is starting",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					PrintableStatus: kubevirtcorev1.VirtualMachineStatusStarting,
				},
			},
			expectError:   true,
			errorContains: "not running",
		},
		{
			name: "VM is migrating",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					PrintableStatus: kubevirtcorev1.VirtualMachineStatusMigrating,
				},
			},
			expectError:   true,
			errorContains: "not running",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateVMIsRunning(tt.vm)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateCBTEnabled(t *testing.T) {
	tests := []struct {
		name          string
		vm            *kubevirtcorev1.VirtualMachine
		expectError   bool
		errorContains string
	}{
		{
			name:          "nil VM",
			vm:            nil,
			expectError:   true,
			errorContains: "VirtualMachine is nil",
		},
		{
			name: "CBT not configured (nil)",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					ChangedBlockTracking: nil,
				},
			},
			expectError:   true,
			errorContains: "does not have ChangedBlockTracking configured",
		},
		{
			name: "CBT enabled",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
						State: kubevirtcorev1.ChangedBlockTrackingEnabled,
					},
				},
			},
			expectError: false,
		},
		{
			name: "CBT pending restart",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
						State: kubevirtcorev1.ChangedBlockTrackingPendingRestart,
					},
				},
			},
			expectError:   true,
			errorContains: "not enabled",
		},
		{
			name: "CBT initializing",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
						State: kubevirtcorev1.ChangedBlockTrackingInitializing,
					},
				},
			},
			expectError:   true,
			errorContains: "not enabled",
		},
		{
			name: "CBT undefined (empty string)",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
						State: kubevirtcorev1.ChangedBlockTrackingUndefined,
					},
				},
			},
			expectError:   true,
			errorContains: "not enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCBTEnabled(tt.vm)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateVMForBackup(t *testing.T) {
	tests := []struct {
		name          string
		vm            *kubevirtcorev1.VirtualMachine
		expectError   bool
		errorContains string
	}{
		{
			name:          "nil VM",
			vm:            nil,
			expectError:   true,
			errorContains: "VirtualMachine is nil",
		},
		{
			name: "VM running with CBT enabled - all checks pass",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					PrintableStatus: kubevirtcorev1.VirtualMachineStatusRunning,
					ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
						State: kubevirtcorev1.ChangedBlockTrackingEnabled,
					},
				},
			},
			expectError: false,
		},
		{
			name: "VM stopped - fails running check first",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					PrintableStatus: kubevirtcorev1.VirtualMachineStatusStopped,
					ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
						State: kubevirtcorev1.ChangedBlockTrackingEnabled,
					},
				},
			},
			expectError:   true,
			errorContains: "not running",
		},
		{
			name: "VM running but CBT not enabled",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					PrintableStatus: kubevirtcorev1.VirtualMachineStatusRunning,
					ChangedBlockTracking: &kubevirtcorev1.ChangedBlockTrackingStatus{
						State: kubevirtcorev1.ChangedBlockTrackingPendingRestart,
					},
				},
			},
			expectError:   true,
			errorContains: "not enabled",
		},
		{
			name: "VM running but CBT nil",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Status: kubevirtcorev1.VirtualMachineStatus{
					PrintableStatus:      kubevirtcorev1.VirtualMachineStatusRunning,
					ChangedBlockTracking: nil,
				},
			},
			expectError:   true,
			errorContains: "does not have ChangedBlockTracking configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateVMForBackup(tt.vm)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetVolumesForVm(t *testing.T) {
	tests := []struct {
		name     string
		vm       *kubevirtcorev1.VirtualMachine
		expected []string
	}{
		{
			name:     "nil VM",
			vm:       nil,
			expected: []string{},
		},
		{
			name: "VM with nil Template",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: nil,
				},
			},
			expected: []string{},
		},
		{
			name: "VM with nil Volumes",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: nil,
						},
					},
				},
			},
			expected: []string{},
		},
		{
			name: "VM with empty Volumes",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{},
						},
					},
				},
			},
			expected: []string{},
		},
		{
			name: "VM with PVC volume",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "disk0",
									VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-1",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{"pvc-1"},
		},
		{
			name: "VM with DataVolume",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "disk0",
									VolumeSource: kubevirtcorev1.VolumeSource{
										DataVolume: &kubevirtcorev1.DataVolumeSource{
											Name: "dv-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{"dv-1"},
		},
		{
			name: "VM with MemoryDump volume",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "memory-dump",
									VolumeSource: kubevirtcorev1.VolumeSource{
										MemoryDump: &kubevirtcorev1.MemoryDumpVolumeSource{
											PersistentVolumeClaimVolumeSource: kubevirtcorev1.PersistentVolumeClaimVolumeSource{
												PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
													ClaimName: "memory-dump-pvc",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{"memory-dump-pvc"},
		},
		{
			name: "VM with multiple volumes",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "disk0",
									VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-1",
											},
										},
									},
								},
								{
									Name: "disk1",
									VolumeSource: kubevirtcorev1.VolumeSource{
										DataVolume: &kubevirtcorev1.DataVolumeSource{
											Name: "dv-1",
										},
									},
								},
								{
									Name: "disk2",
									VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-2",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{"pvc-1", "dv-1", "pvc-2"},
		},
		{
			name: "VM with non-PVC volumes only",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "cloudinit",
									VolumeSource: kubevirtcorev1.VolumeSource{
										CloudInitNoCloud: &kubevirtcorev1.CloudInitNoCloudSource{
											UserData: "test",
										},
									},
								},
								{
									Name: "containerDisk",
									VolumeSource: kubevirtcorev1.VolumeSource{
										ContainerDisk: &kubevirtcorev1.ContainerDiskSource{
											Image: "test-image",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{},
		},
		{
			name: "VM with mixed PVC and non-PVC volumes",
			vm: &kubevirtcorev1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vm",
					Namespace: "default",
				},
				Spec: kubevirtcorev1.VirtualMachineSpec{
					Template: &kubevirtcorev1.VirtualMachineInstanceTemplateSpec{
						Spec: kubevirtcorev1.VirtualMachineInstanceSpec{
							Volumes: []kubevirtcorev1.Volume{
								{
									Name: "disk0",
									VolumeSource: kubevirtcorev1.VolumeSource{
										PersistentVolumeClaim: &kubevirtcorev1.PersistentVolumeClaimVolumeSource{
											PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
												ClaimName: "pvc-1",
											},
										},
									},
								},
								{
									Name: "cloudinit",
									VolumeSource: kubevirtcorev1.VolumeSource{
										CloudInitNoCloud: &kubevirtcorev1.CloudInitNoCloudSource{
											UserData: "test",
										},
									},
								},
								{
									Name: "disk1",
									VolumeSource: kubevirtcorev1.VolumeSource{
										DataVolume: &kubevirtcorev1.DataVolumeSource{
											Name: "dv-1",
										},
									},
								},
							},
						},
					},
				},
			},
			expected: []string{"pvc-1", "dv-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetVolumesForVm(tt.vm)

			// Use ElementsMatch to handle nil vs empty slice and ordering differences
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}
