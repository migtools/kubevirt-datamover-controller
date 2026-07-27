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
package uploader

import (
	"encoding/json"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestCheckpointEntryJSONSerialization(t *testing.T) {
	tests := []struct {
		name       string
		entry      CheckpointEntry
		expectJSON map[string]any
	}{
		{
			name: "full backup checkpoint",
			entry: CheckpointEntry{
				ID:        "cp-001",
				Type:      "full",
				Timestamp: time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC),
				VMBackup:  "vmb-test",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-test-disk1.qcow2",
						DiskName:   "disk1",
						Size:       1073741824,
						ObjectPath: "checkpoints/ns/vm/cp-001/vmb-test-disk1.qcow2",
					},
				},
				PVCs:         []string{"disk1"},
				PVCSizes:     []resource.Quantity{resource.MustParse("10Gi")},
				ReferencedBy: []string{"backup-001"},
			},
			expectJSON: map[string]any{
				"id":           "cp-001",
				"type":         "full",
				"vmBackup":     "vmb-test",
				"pvcs":         []any{"disk1"},
				"pvcSizes":     []any{"10Gi"},
				"referencedBy": []any{"backup-001"},
			},
		},
		{
			name: "incremental backup checkpoint",
			entry: CheckpointEntry{
				ID:        "cp-002",
				Type:      "incremental",
				Parent:    "cp-001",
				Timestamp: time.Date(2026, 1, 16, 10, 30, 0, 0, time.UTC),
				VMBackup:  "vmb-test-2",
				Files: []CheckpointFile{
					{
						Filename:   "vmb-test-2-disk1.qcow2",
						DiskName:   "disk1",
						Size:       104857600,
						ObjectPath: "checkpoints/ns/vm/cp-002/vmb-test-2-disk1.qcow2",
					},
				},
				PVCs:         []string{"disk1"},
				PVCSizes:     []resource.Quantity{resource.MustParse("10Gi")},
				ReferencedBy: []string{"backup-002"},
			},
			expectJSON: map[string]any{
				"id":           "cp-002",
				"type":         "incremental",
				"parent":       "cp-001",
				"vmBackup":     "vmb-test-2",
				"pvcs":         []any{"disk1"},
				"pvcSizes":     []any{"10Gi"},
				"referencedBy": []any{"backup-002"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize to JSON
			data, err := json.Marshal(tt.entry)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}

			// Parse JSON back to map
			var result map[string]any
			if err := json.Unmarshal(data, &result); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}

			// Check expected fields
			for key, expected := range tt.expectJSON {
				actual, ok := result[key]
				if !ok {
					t.Errorf("missing key %q in JSON output", key)
					continue
				}

				// Compare based on type
				switch exp := expected.(type) {
				case string:
					if actual != exp {
						t.Errorf("field %q = %v, want %v", key, actual, exp)
					}
				case []any:
					actualSlice, ok := actual.([]any)
					if !ok {
						t.Errorf("field %q is not a slice", key)
						continue
					}
					if len(actualSlice) != len(exp) {
						t.Errorf("field %q length = %d, want %d", key, len(actualSlice), len(exp))
					}
				}
			}
		})
	}
}

func TestVMIndexJSONSerialization(t *testing.T) {
	vmIndex := VMIndex{
		VMName:    "test-vm",
		Namespace: "test-ns",
		Checkpoints: []CheckpointEntry{
			{
				ID:   "cp-001",
				Type: "full",
			},
			{
				ID:     "cp-002",
				Type:   "incremental",
				Parent: "cp-001",
			},
		},
		LastUpdated: time.Date(2026, 1, 16, 12, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(vmIndex)
	if err != nil {
		t.Fatalf("failed to marshal VMIndex: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify field names match design document
	if result["vmName"] != "test-vm" {
		t.Errorf("vmName = %v, want %v", result["vmName"], "test-vm")
	}
	if result["namespace"] != "test-ns" {
		t.Errorf("namespace = %v, want %v", result["namespace"], "test-ns")
	}

	checkpoints, ok := result["checkpoints"].([]any)
	if !ok {
		t.Fatal("checkpoints is not a slice")
	}
	if len(checkpoints) != 2 {
		t.Errorf("checkpoints length = %d, want 2", len(checkpoints))
	}
}

func TestBackupManifestJSONSerialization(t *testing.T) {
	manifest := BackupManifest{
		BackupName: "velero-backup-001",
		Timestamp:  time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		VMs: []VMBackupReference{
			{
				Name:         "vm1",
				Namespace:    "ns1",
				CheckpointID: "cp-001",
				ManifestPath: "manifests/velero-backup-001/ns1-vm1.json",
			},
		},
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal BackupManifest: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify field names match design document
	if result["backupName"] != "velero-backup-001" {
		t.Errorf("backupName = %v, want %v", result["backupName"], "velero-backup-001")
	}

	vms, ok := result["vms"].([]any)
	if !ok {
		t.Fatal("vms is not a slice")
	}
	if len(vms) != 1 {
		t.Errorf("vms length = %d, want 1", len(vms))
	}
}

func TestVMBackupManifestJSONSerialization(t *testing.T) {
	manifest := VMBackupManifest{
		Namespace:       "test-ns",
		Name:            "test-vm",
		CheckpointChain: []string{"cp-001", "cp-002"},
		BackupName:      "velero-backup-001",
		Timestamp:       time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("failed to marshal VMBackupManifest: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify field names match design document
	if result["namespace"] != "test-ns" {
		t.Errorf("namespace = %v, want %v", result["namespace"], "test-ns")
	}
	if result["name"] != "test-vm" {
		t.Errorf("name = %v, want %v", result["name"], "test-vm")
	}
	if result["backupName"] != "velero-backup-001" {
		t.Errorf("backupName = %v, want %v", result["backupName"], "velero-backup-001")
	}

	chain, ok := result["checkpointChain"].([]any)
	if !ok {
		t.Fatal("checkpointChain is not a slice")
	}
	if len(chain) != 2 {
		t.Errorf("checkpointChain length = %d, want 2", len(chain))
	}
}

func TestCheckpointFileJSONSerialization(t *testing.T) {
	file := CheckpointFile{
		Filename:   "vmb-test-disk1.qcow2",
		DiskName:   "disk1",
		Size:       1073741824,
		ObjectPath: "checkpoints/ns/vm/cp-001/vmb-test-disk1.qcow2",
	}

	data, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("failed to marshal CheckpointFile: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if result["filename"] != "vmb-test-disk1.qcow2" {
		t.Errorf("filename = %v, want %v", result["filename"], "vmb-test-disk1.qcow2")
	}
	if result["diskName"] != "disk1" {
		t.Errorf("diskName = %v, want %v", result["diskName"], "disk1")
	}
	if result["objectPath"] != "checkpoints/ns/vm/cp-001/vmb-test-disk1.qcow2" {
		t.Errorf("objectPath = %v, want %v", result["objectPath"], "checkpoints/ns/vm/cp-001/vmb-test-disk1.qcow2")
	}
}

func TestEnvironmentVariableConstants(t *testing.T) {
	// Verify environment variable names are correctly defined
	expectedEnvVars := map[string]string{
		"EnvBSLProvider":       "KUBEVIRT_DM_BSL_PROVIDER",
		"EnvBSLBucket":         "KUBEVIRT_DM_BSL_BUCKET",
		"EnvBSLPrefix":         "KUBEVIRT_DM_BSL_PREFIX",
		"EnvBSLRegion":         "KUBEVIRT_DM_BSL_REGION",
		"EnvCredentialsFile":   "KUBEVIRT_DM_CREDENTIALS_FILE",
		"EnvVMName":            "KUBEVIRT_DM_VM_NAME",
		"EnvVMNamespace":       "KUBEVIRT_DM_VM_NAMESPACE",
		"EnvCheckpointName":    "KUBEVIRT_DM_CHECKPOINT_NAME",
		"EnvBackupType":        "KUBEVIRT_DM_BACKUP_TYPE",
		"EnvVeleroBackupName":  "KUBEVIRT_DM_VELERO_BACKUP_NAME",
		"EnvSourcePVCPath":     "KUBEVIRT_DM_SOURCE_PVC_PATH",
		"EnvDataUploadName":    "KUBEVIRT_DM_DATAUPLOAD_NAME",
		"EnvDataUploadUID":     "KUBEVIRT_DM_DATAUPLOAD_UID",
		"EnvVMBName":           "KUBEVIRT_DM_VMB_NAME",
		"EnvBSLServiceAccount": "KUBEVIRT_DM_BSL_SERVICE_ACCOUNT",
		"EnvBSLKMSKeyName":     "KUBEVIRT_DM_BSL_KMS_KEY_NAME",
	}

	// Just checking they're defined and have expected prefix
	envVarsToCheck := []struct {
		name     string
		constant string
	}{
		{"EnvBSLProvider", EnvBSLProvider},
		{"EnvBSLBucket", EnvBSLBucket},
		{"EnvBSLPrefix", EnvBSLPrefix},
		{"EnvBSLRegion", EnvBSLRegion},
		{"EnvCredentialsFile", EnvCredentialsFile},
		{"EnvVMName", EnvVMName},
		{"EnvVMNamespace", EnvVMNamespace},
		{"EnvCheckpointName", EnvCheckpointName},
		{"EnvBackupType", EnvBackupType},
		{"EnvVeleroBackupName", EnvVeleroBackupName},
		{"EnvSourcePVCPath", EnvSourcePVCPath},
		{"EnvDataUploadName", EnvDataUploadName},
		{"EnvDataUploadUID", EnvDataUploadUID},
		{"EnvVMBName", EnvVMBName},
		{"EnvBSLServiceAccount", EnvBSLServiceAccount},
		{"EnvBSLKMSKeyName", EnvBSLKMSKeyName},
	}

	for _, ev := range envVarsToCheck {
		expected := expectedEnvVars[ev.name]
		if ev.constant != expected {
			t.Errorf("%s = %q, want %q", ev.name, ev.constant, expected)
		}
	}
}

func TestDefaultConstants(t *testing.T) {
	if DefaultSourcePVCPath != "/backup-data" {
		t.Errorf("DefaultSourcePVCPath = %q, want %q", DefaultSourcePVCPath, "/backup-data")
	}
	if DefaultCredentialsPath != "/credentials/cloud" {
		t.Errorf("DefaultCredentialsPath = %q, want %q", DefaultCredentialsPath, "/credentials/cloud")
	}
}
