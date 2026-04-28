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

// Package common contains shared constants and types used by both
// the kubevirt-datamover-controller and kubevirt-datamover-plugin.
package common

// Annotation keys for DataUpload resources
const (
	// AnnotationVMName is the annotation key for the source VirtualMachine name.
	// This annotation is set on the DataUpload by the plugin to identify
	// which VM should be backed up.
	AnnotationVMName = "kubevirt-datamover.io/vm-name"

	// AnnotationVMNamespace is the annotation key for the source VirtualMachine namespace.
	// If not set, the controller will use the DataUpload's source namespace.
	AnnotationVMNamespace = "kubevirt-datamover.io/vm-namespace"

	// AnnotationOperationID is the annotation for tracking async backup/restore operations.
	AnnotationOperationID = "kubevirt-datamover.io/operation-id"

	// AnnotationBSLValidated indicates that BSL checkpoint validation has been
	// performed for this DataUpload. This prevents redundant S3 queries on
	// every reconcile loop iteration while still ensuring validation runs
	// once per DataUpload.
	AnnotationBSLValidated = "kubevirt-datamover.io/bsl-validated"

	// AnnotationForceFullBackup, when set to "true" on a DataUpload, forces
	// a full backup even when a valid incremental checkpoint chain exists.
	// The VMBT checkpoint is cleared and the VMB is created with
	// ForceFullBackup=true. The new checkpoint replaces the old one in BSL.
	AnnotationForceFullBackup = "kubevirt-datamover.io/force-full-backup"

	// AnnotationBackupPVCSize, when set on a DataUpload, overrides the
	// calculated temp PVC size. Value must be a valid Kubernetes quantity
	// (e.g., "50Gi").
	AnnotationBackupPVCSize = "kubevirt-datamover.io/backup-pvc-size"

	// AnnotationDataUploadName is the annotation key for the DataUpload name.
	// Used on VMB, VMBT, and PVC resources to track ownership.
	AnnotationDataUploadName = "velero.io/dataupload-name"
)

// Annotation keys for VirtualMachine resources
const (
	// AnnotationMaxIncrementalBackups, when set on a VirtualMachine, overrides
	// the global --max-incremental-backups setting for that VM.
	// The value must be a non-negative integer string (e.g., "5"). "0" means unlimited.
	AnnotationMaxIncrementalBackups = "kubevirt-datamover.io/max-incremental-backups"
)

// Label keys for resources created by the controller.
// Note: Kubernetes label values are limited to 63 characters. Use UIDs or
// hashes (which have fixed length) as label values for lookups; store
// human-readable names in annotations instead.
const (
	// LabelDataUploadUID is the label key for the DataUpload UID.
	// UIDs are always 36 characters, safe for label values.
	// Used for precise ownership tracking and label-based lookups.
	LabelDataUploadUID = "velero.io/dataupload-uid"

	// LabelVMNameHash is the label key for a hashed VM name on VMBTs.
	// VM names can exceed the 63-char label limit, so we store a 16-char
	// hex hash for label-based lookups and the full name in an annotation.
	LabelVMNameHash = "kubevirt-datamover.io/vm-name-hash"

	// LabelDatamoverPod identifies the type of datamover pod.
	LabelDatamoverPod = "kubevirt-datamover.io/pod-type"

	// LabelVeleroBackupName is the label key for the Velero backup name.
	LabelVeleroBackupName = "velero.io/backup-name"
)

// Naming conventions for resources
const (
	// DatamoverPodNamePrefix is the prefix for datamover pod names.
	DatamoverPodNamePrefix = "kubevirt-dm-"

	// TempPVCNamePrefix is the prefix for temporary PVC names.
	TempPVCNamePrefix = "kubevirt-backup-"

	// ReboundPVCNamePrefix is the prefix for PVCs created in OADP namespace after PV rebinding.
	ReboundPVCNamePrefix = "kubevirt-dm-pvc-"

	// VMBackupNamePrefix is the prefix for VirtualMachineBackup names.
	VMBackupNamePrefix = "vmb-"
)

// DataMover identifier
const (
	// DataMoverKubeVirt is the datamover value that indicates the kubevirt
	// datamover controller should handle the DataUpload/DataDownload.
	DataMoverKubeVirt = "kubevirt"
)

// BSL prefix for datamover objects
const (
	// DatamoverBSLPrefix is the suffix appended to the BSL prefix to namespace
	// datamover objects within the object store bucket.
	DatamoverBSLPrefix = "kubevirt-datamover"
)

// Default images
const (
	// DefaultDatamoverImage is the default image for datamover pods.
	DefaultDatamoverImage = "quay.io/konveyor/kubevirt-datamover-controller:latest"
)

// SnapshotType constants for DataUpload
const (
	// SnapshotTypeCSI is the snapshot type for CSI-based backups.
	// The kubevirt datamover uses CSI snapshots as the underlying mechanism.
	SnapshotTypeCSI = "CSI"
)

// DataUpload Phase Transitions (for documentation):
//
// The kubevirt-datamover-controller handles the following phase transitions:
//
//   New -> Accepted:
//     - Controller validates VM annotations exist
//     - Transitions to Accepted if valid, Failed if missing
//
//   Accepted -> Prepared:
//     - Controller creates temporary PVC for backup output
//     - Controller creates/updates VirtualMachineBackupTracker (VMBT)
//     - Controller creates VirtualMachineBackup (VMB) referencing the VMBT
//     - Waits for VMB to complete (Done: True condition)
//     - Transitions to Prepared when VMB completes successfully
//     - Transitions to Failed if VMB fails (Done: False condition)
//
//   Prepared -> InProgress:
//     - Controller signals that backup data is ready for transfer
//     - Plugin launches datamover pod to transfer data to BSL
//
//   InProgress -> Completed:
//     - Plugin monitors datamover pod progress
//     - Transitions to Completed when data transfer finishes
//
//   Any -> Failed:
//     - Any phase can transition to Failed on errors
//
//   InProgress -> Canceling -> Canceled:
//     - Cancellation support (handles user-initiated cancel requests)
