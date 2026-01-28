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
)

// Label keys for resources created by the controller
const (
	// LabelDataUploadName is the label key for the DataUpload name.
	// Used on VMB, VMBT, and PVC resources to track ownership.
	LabelDataUploadName = "velero.io/dataupload-name"

	// LabelDataUploadUID is the label key for the DataUpload UID.
	// Used for precise ownership tracking.
	LabelDataUploadUID = "velero.io/dataupload-uid"
)

// DataMover identifier
const (
	// DataMoverKubeVirt is the datamover value that indicates the kubevirt
	// datamover controller should handle the DataUpload/DataDownload.
	DataMoverKubeVirt = "kubevirt"
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
