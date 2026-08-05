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

import "time"

// Annotation keys for DataUpload and DataDownload resources
const (
	// AnnotationVMName is the annotation key for the source VirtualMachine name.
	// This annotation is set on the DataUpload/DataDownload by the plugin to identify
	// which VM should be backed up or restored.
	AnnotationVMName = "kubevirt-datamover.io/vm-name"

	// AnnotationVMNamespace is the annotation key for the source VirtualMachine namespace.
	// If not set, the controller will use the DataUpload/DataDownload's source namespace.
	AnnotationVMNamespace = "kubevirt-datamover.io/vm-namespace"

	// AnnotationOperationID is the annotation for tracking async backup/restore operations.
	AnnotationOperationID = "kubevirt-datamover.io/operation-id"

	// AnnotationBSLValidated indicates that BSL checkpoint validation has been
	// performed for this DataUpload. This prevents redundant S3 queries on
	// every reconcile loop iteration while still ensuring validation runs
	// once per DataUpload.
	AnnotationBSLValidated = "kubevirt-datamover.io/bsl-validated"

	// AnnotationDatamoverPodSucceeded indicates the datamover pod has already
	// been observed in PodSucceeded phase, with cleanup pending or in
	// progress. Kubelet may take a reconcile or two to finish tearing the
	// pod down (unmounting its volumes) before it disappears entirely; this
	// annotation lets a later reconcile that finds no pod distinguish
	// "cleanup still finishing" from a pod that never existed, so it doesn't
	// mistakenly fail the DataUpload. See https://github.com/migtools/kubevirt-datamover-controller/issues/171.
	AnnotationDatamoverPodSucceeded = "kubevirt-datamover.io/datamover-pod-succeeded"

	// AnnotationForceFullBackup, when set to "true" on a DataUpload, forces
	// a full backup even when a valid incremental checkpoint chain exists.
	// The VMBT checkpoint is cleared and the VMB is created with
	// ForceFullBackup=true. The new checkpoint replaces the old one in BSL.
	AnnotationForceFullBackup = "kubevirt-datamover.io/force-full-backup"

	// AnnotationBackupPVCSize, when set on a DataUpload, overrides the
	// calculated temp PVC size. Value must be a valid Kubernetes quantity
	// (e.g., "50Gi").
	AnnotationBackupPVCSize = "kubevirt-datamover.io/backup-pvc-size"

	// AnnotationExpectedBackupType records the backup type the controller
	// expected based on BSL chain validation ("full" or "incremental").
	// Used to detect mismatches when virt-controller reports a different
	// type in VMB.Status.Type (e.g., VM lost its libvirt checkpoint).
	AnnotationExpectedBackupType = "kubevirt-datamover.io/expected-backup-type"

	// AnnotationDataUploadName is the annotation key for the DataUpload name.
	// Used on VMB, VMBT, and PVC resources to track ownership.
	AnnotationDataUploadName = "velero.io/dataupload-name"

	// AnnotationDataDownloadName is the annotation key for the DataDownload name.
	// Used on PVC and pod resources to track ownership during restore.
	AnnotationDataDownloadName = "velero.io/datadownload-name"
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

	// LabelDataDownloadUID is the label key for the DataDownload UID.
	// UIDs are always 36 characters, safe for label values.
	// Used for precise ownership tracking and label-based lookups during restore.
	LabelDataDownloadUID = "velero.io/datadownload-uid"

	// LabelVMNameHash is the label key for a hashed VM name on VMBTs.
	// VM names can exceed the 63-char label limit, so we store a 16-char
	// hex hash for label-based lookups and the full name in an annotation.
	LabelVMNameHash = "kubevirt-datamover.io/vm-name-hash"

	// LabelDatamoverPod identifies the type of datamover pod.
	LabelDatamoverPod = "kubevirt-datamover.io/pod-type"

	// LabelVeleroBackupName is the label key for the Velero backup name.
	LabelVeleroBackupName = "velero.io/backup-name"

	// LabelVeleroRestoreName is the label key for the Velero restore name.
	LabelVeleroRestoreName = "velero.io/restore-name"

	// LabelScratchVolumeRole distinguishes a DataDownload's scratch PVCs when
	// a Block-mode restore target needs two: a Filesystem "work" PVC staging
	// the downloaded qcow2 chain, and a Block "output" PVC receiving the
	// final flattened image (which gets rebound onto the restore target).
	// Unset on the single scratch PVC used by a Filesystem-mode restore
	// target, which needs only one and serves both roles.
	LabelScratchVolumeRole = "kubevirt-datamover.io/scratch-volume-role"
)

// Scratch volume role values used with LabelScratchVolumeRole. Only present
// on a Block-mode restore target's two scratch PVCs -- a Filesystem-mode
// target's single scratch PVC carries no role label.
const (
	// ScratchVolumeRoleWork identifies the Filesystem-mode PVC that stages
	// the downloaded qcow2 chain.
	ScratchVolumeRoleWork = "work"

	// ScratchVolumeRoleOutput identifies the Block-mode PVC that receives
	// the final flattened raw disk image and gets rebound onto the restore
	// target.
	ScratchVolumeRoleOutput = "output"
)

// Naming conventions for resources
const (
	// DatamoverPodNamePrefix is the prefix for datamover upload pod names.
	DatamoverPodNamePrefix = "kubevirt-dm-"

	// DownloaderPodNamePrefix is the prefix for datamover download pod names.
	DownloaderPodNamePrefix = "kubevirt-dm-dl-"

	// TempPVCNamePrefix is the prefix for temporary backup PVC names.
	TempPVCNamePrefix = "kubevirt-backup-"

	// ScratchPVCNamePrefix is the prefix for temporary scratch PVCs used during restore
	// to hold downloaded qcow2 files before chain reconstruction.
	ScratchPVCNamePrefix = "kubevirt-restore-scratch-"

	// TargetPVCNamePrefix is the prefix for restored disk PVCs.
	TargetPVCNamePrefix = "kubevirt-restore-"

	// ReboundPVCNamePrefix is the prefix for PVCs created in OADP namespace after PV rebinding.
	ReboundPVCNamePrefix = "kubevirt-dm-pvc-"

	// VMBackupNamePrefix is the prefix for VirtualMachineBackup names.
	VMBackupNamePrefix = "vmb-"
)

// Pod type values used with LabelDatamoverPod.
// These match OperationMode values in the pod builder so label selectors
// and pod builder stay consistent.
const (
	// PodTypeUpload identifies a datamover pod running the upload (backup) path.
	PodTypeUpload = "upload"

	// PodTypeDownload identifies a datamover pod running the download (restore) path.
	PodTypeDownload = "download"
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

// Default thresholds
const (
	// DefaultStaleDataUploadThreshold is the default duration after which a
	// DataUpload in an active phase is considered stale and will no longer
	// block younger DataUploads for the same VM.
	DefaultStaleDataUploadThreshold = 2 * time.Hour
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
//
// DataDownload Phase Transitions (for documentation):
//
// The kubevirt-datamover-controller handles the following phase transitions
// for DataDownload CRs:
//
//   New -> Accepted:
//     - Controller validates VM annotations and BSL accessibility
//     - Transitions to Accepted if valid, Failed if missing
//
//   Accepted -> Prepared:
//     - Controller reads backup manifest from BSL to get checkpoint chain
//     - Creates scratch PVC for downloaded qcow2 files
//     - Transitions to Prepared when ready
//
//   Prepared -> InProgress:
//     - Controller launches downloader pod to download and reconstruct disk
//     - Pod downloads checkpoint chain, runs qemu-img to flatten to raw
//
//   InProgress -> Completed:
//     - Controller monitors downloader pod progress
//     - Provisions target PVC with restored disk data
//     - Transitions to Completed when data transfer finishes
//
//   Any -> Failed:
//     - Any phase can transition to Failed on errors
//
//   InProgress -> Canceling -> Canceled:
//     - Cancellation support (handles user-initiated cancel requests)
