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

package uploader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	kubevirtcorev1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	restcfg "sigs.k8s.io/controller-runtime/pkg/client/config"
)

const apiGroupKubeVirt = "kubevirt.io"

// Helper functions for working with velero.ObjectStore interface

// putObjectBytes uploads bytes to the object store.
func putObjectBytes(store velero.ObjectStore, bucket, key string, data []byte) error {
	return store.PutObject(bucket, key, bytes.NewReader(data))
}

// getObjectBytes downloads an object as bytes from the object store.
func getObjectBytes(store velero.ObjectStore, bucket, key string) ([]byte, error) {
	reader, err := store.GetObject(bucket, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return io.ReadAll(reader)
}

// Run is the main entrypoint for the uploader.
func Run(ctx context.Context, logger logr.Logger) error {
	logger.Info("Starting kubevirt datamover uploader")

	// Load configuration from environment
	config, err := LoadConfigFromEnv()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	logger.Info("Config loaded",
		"vm", config.VMNamespace+"/"+config.VMName,
		"checkpoint", config.CheckpointName,
		"type", config.BackupType)

	// Initialize object store - returns velero.ObjectStore interface
	store, err := InitObjectStore(config)
	if err != nil {
		return fmt.Errorf("failed to initialize object store: %w", err)
	}

	logger.Info("Object store initialized", "bucket", config.BSLBucket, "prefix", config.BSLPrefix)

	// Upload qcow2 files
	files, err := uploadQcow2Files(ctx, store, config, logger)
	if err != nil {
		return fmt.Errorf("failed to upload qcow2 files: %w", err)
	}

	if len(files) == 0 {
		return fmt.Errorf("no qcow2 files found in %s", config.SourcePVCPath)
	}

	logger.Info("Uploaded qcow2 files", "count", len(files))

	// Create K8s client once — reused for archiving and cleanup
	k8sClient, err := newKubeClient()
	if err != nil {
		return fmt.Errorf("failed to create K8s client: %w", err)
	}

	// Archive VMB/VMBT CRs to S3 (FATAL if fails — index references must be valid)
	archived, err := archiveKubeResources(ctx, store, k8sClient, config, logger)
	if err != nil {
		return fmt.Errorf("failed to archive Kube resources: %w", err)
	}
	logger.Info("Kube resources archived to S3")

	// Update VM index (references the archived paths)
	if err := updateVMIndex(ctx, store, k8sClient, config, files, archived, logger); err != nil {
		return fmt.Errorf("failed to update VM index: %w", err)
	}

	logger.Info("VM index updated")

	// Update backup manifests
	if err := updateBackupManifests(ctx, store, config, logger); err != nil {
		return fmt.Errorf("failed to update backup manifests: %w", err)
	}

	logger.Info("Backup manifests updated")

	// Delete VMB/VMBT from cluster (non-fatal — S3 is now source of truth)
	cleanupKubeResources(ctx, k8sClient, config, logger)

	logger.Info("Upload completed successfully")

	return nil
}

// LoadConfigFromEnv parses environment variables into UploaderConfig.
func LoadConfigFromEnv() (*UploaderConfig, error) {
	config := &UploaderConfig{
		BSLProvider:      os.Getenv(EnvBSLProvider),
		BSLBucket:        os.Getenv(EnvBSLBucket),
		BSLPrefix:        os.Getenv(EnvBSLPrefix),
		BSLRegion:        os.Getenv(EnvBSLRegion),
		CredentialsFile:  os.Getenv(EnvCredentialsFile),
		VMName:           os.Getenv(EnvVMName),
		VMNamespace:      os.Getenv(EnvVMNamespace),
		CheckpointName:   os.Getenv(EnvCheckpointName),
		BackupType:       os.Getenv(EnvBackupType),
		VeleroBackupName: os.Getenv(EnvVeleroBackupName),
		DataUploadName:   os.Getenv(EnvDataUploadName),
		DataUploadUID:    os.Getenv(EnvDataUploadUID),
		VMBName:          os.Getenv(EnvVMBName),
		VMBTName:         os.Getenv(EnvVMBTName),
		SourcePVCPath:    os.Getenv(EnvSourcePVCPath),
	}

	// Apply defaults
	if config.SourcePVCPath == "" {
		config.SourcePVCPath = DefaultSourcePVCPath
	}
	if config.CredentialsFile == "" {
		config.CredentialsFile = DefaultCredentialsPath
	}
	if config.BackupType == "" {
		config.BackupType = "full"
	}
	if config.VMBTName == "" && config.VMName != "" {
		config.VMBTName = common.SafeResourceName("vmbt-", config.VMName)
	}

	// Validate required fields
	if config.BSLBucket == "" {
		return nil, fmt.Errorf("%s is required", EnvBSLBucket)
	}
	if config.VMName == "" {
		return nil, fmt.Errorf("%s is required", EnvVMName)
	}
	if config.VMNamespace == "" {
		return nil, fmt.Errorf("%s is required", EnvVMNamespace)
	}
	if config.CheckpointName == "" {
		return nil, fmt.Errorf("%s is required", EnvCheckpointName)
	}

	// Validate backup type
	switch strings.ToLower(config.BackupType) {
	case BackupTypeFull, BackupTypeIncremental:
		// Valid
	default:
		return nil, fmt.Errorf("invalid backup type %q: must be %q or %q",
			config.BackupType, BackupTypeFull, BackupTypeIncremental)
	}

	return config, nil
}

// uploadQcow2Files walks the source path and uploads all qcow2 files.
func uploadQcow2Files(
	_ context.Context, store velero.ObjectStore, config *UploaderConfig, logger logr.Logger,
) ([]CheckpointFile, error) {
	var files []CheckpointFile

	err := filepath.WalkDir(config.SourcePVCPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Only process qcow2 files
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".qcow2") {
			return nil
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %w", path, err)
		}

		// Build object path: checkpoints/<ns>/<vm>/<checkpoint>/<filename>
		objectPath := fmt.Sprintf("checkpoints/%s/%s/%s/%s",
			config.VMNamespace, config.VMName, config.CheckpointName, d.Name())

		// Open file for upload
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}

		logger.Info("Uploading file", "filename", d.Name(), "size", info.Size(), "objectPath", objectPath)

		// Upload file using velero.ObjectStore interface
		uploadErr := store.PutObject(config.BSLBucket, objectPath, file)
		// Close file immediately after upload to avoid resource exhaustion with many files
		_ = file.Close()
		if uploadErr != nil {
			return fmt.Errorf("failed to upload %s: %w", path, uploadErr)
		}

		// Extract disk name from filename (e.g., "vmb-xxx-disk1.qcow2" -> "disk1")
		diskName := extractDiskName(d.Name())

		files = append(files, CheckpointFile{
			Filename:   d.Name(),
			DiskName:   diskName,
			Size:       info.Size(),
			ObjectPath: objectPath,
		})

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk source path: %w", err)
	}

	return files, nil
}

// extractDiskName extracts the disk name from a qcow2 filename.
// E.g., "vmb-xxx-disk1.qcow2" -> "disk1"
func extractDiskName(filename string) string {
	// Remove .qcow2 extension
	name := strings.TrimSuffix(filename, ".qcow2")
	name = strings.TrimSuffix(name, ".QCOW2")

	// Find the last dash and extract everything after it
	parts := strings.Split(name, "-")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return name
}

// updateVMIndex creates or updates the per-VM index.json file.
// The archived parameter provides S3 paths for the VMB/VMBT JSON files
// that were uploaded by archiveKubeResources — these are stored on the
// checkpoint entry so the index always references valid objects.
func updateVMIndex(
	ctx context.Context, store velero.ObjectStore, k8sClient client.Client, config *UploaderConfig,
	files []CheckpointFile, archived *archivedPaths, logger logr.Logger,
) error {
	indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json", config.VMNamespace, config.VMName)

	// Try to load existing index
	var vmIndex VMIndex

	// First check if index exists to distinguish "not found" from other errors
	exists, err := store.ObjectExists(config.BSLBucket, indexPath)
	if err != nil {
		return fmt.Errorf("failed to check if VM index exists: %w", err)
	}

	if exists {
		data, err := getObjectBytes(store, config.BSLBucket, indexPath)
		if err != nil {
			return fmt.Errorf("failed to read existing VM index: %w", err)
		}
		if err := json.Unmarshal(data, &vmIndex); err != nil {
			logger.Info("Failed to parse existing index, creating new", "reason", err.Error())
			vmIndex = VMIndex{}
		}
	} else {
		// Index doesn't exist, create new
		vmIndex = VMIndex{
			VMName:      config.VMName,
			Namespace:   config.VMNamespace,
			Checkpoints: []CheckpointEntry{},
		}
	}

	// Create new checkpoint entry
	vm := &kubevirtcorev1.VirtualMachine{}
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      config.VMName,
		Namespace: config.VMNamespace,
	}, vm); err != nil {
		return fmt.Errorf("failed to get VM %s/%s: %w", config.VMNamespace, config.VMName, err)
	}
	volumeMap := common.GetVolumeMapForVm(vm)

	// Extract PVC/disk names from uploaded files
	var pvcNames []string
	var pvcSizes []resource.Quantity
	for _, f := range files {
		if f.DiskName != "" {
			pvcName := volumeMap[f.DiskName]
			if pvcName == "" {
				pvcName = f.DiskName
			}
			pvcNames = append(pvcNames, pvcName)
			pvc := &corev1.PersistentVolumeClaim{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: config.VMNamespace}, pvc); err != nil {
				return fmt.Errorf("failed to get PVC %s: %w", pvcName, err)
			}
			if storage, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
				pvcSizes = append(pvcSizes, storage)
			} else {
				return fmt.Errorf("Missing storage request value in PVC %s", pvcName)
			}
		}
	}

	// ReferencedBy tracks which Velero backups use this checkpoint
	var referencedBy []string
	if config.VeleroBackupName != "" {
		referencedBy = append(referencedBy, config.VeleroBackupName)
	}

	checkpoint := CheckpointEntry{
		ID:             config.CheckpointName,
		Type:           config.BackupType,
		Timestamp:      time.Now().UTC(),
		VMBackup:       config.VMBName,
		Files:          files,
		PVCs:           pvcNames,
		PVCSizes:       pvcSizes,
		ReferencedBy:   referencedBy,
		VMBObjectPath:  archived.VMBObjectPath,
		VMBTObjectPath: archived.VMBTObjectPath,
	}

	// For incremental backups, validate the S3 chain and set parent checkpoint
	if strings.ToLower(config.BackupType) == BackupTypeIncremental && len(vmIndex.Checkpoints) > 0 {
		latestCP := vmIndex.Checkpoints[len(vmIndex.Checkpoints)-1]
		result, err := validateCheckpointChain(ctx, store, config.BSLBucket, vmIndex.Checkpoints, latestCP.ID)
		if err != nil {
			return fmt.Errorf("failed to validate checkpoint chain: %w", err)
		}
		if !result.Found {
			return fmt.Errorf("incremental backup requested but no valid parent chain exists: %s", result.Message)
		}
		// If the chain fell back to an older checkpoint, the controller should have
		// forced a full backup. Reject the incremental as a safety net.
		if result.LatestCheckpoint != latestCP.ID {
			return fmt.Errorf(
				"checkpoint chain is broken: latest valid checkpoint is %s but expected %s; "+
					"a full backup should have been performed instead",
				result.LatestCheckpoint, latestCP.ID)
		}
		checkpoint.Parent = result.LatestCheckpoint
	}

	// Append new checkpoint (avoid duplicates)
	found := false
	for i, cp := range vmIndex.Checkpoints {
		if cp.ID == checkpoint.ID {
			// Guard against self-referential parent. This can happen when an
			// incremental backup re-runs: the checkpoint is already the last
			// entry, so validateCheckpointChain returns its own ID as the
			// latest valid checkpoint, causing Parent to point to itself.
			// Preserve the original parent from the existing entry instead.
			if checkpoint.Parent == checkpoint.ID {
				checkpoint.Parent = cp.Parent
			}
			// Merge ReferencedBy from the existing entry so we don't lose
			// backup names accumulated by previous runs.
			checkpoint.ReferencedBy = mergeReferencedBy(cp.ReferencedBy, checkpoint.ReferencedBy)
			vmIndex.Checkpoints[i] = checkpoint
			found = true
			break
		}
	}
	if !found {
		vmIndex.Checkpoints = append(vmIndex.Checkpoints, checkpoint)
	}

	// Propagate the current backup name to all ancestor checkpoints in
	// the parent chain. An incremental backup depends on every checkpoint
	// back to the base full backup, so each ancestor must list this backup
	// in its ReferencedBy for correct garbage-collection decisions.
	if config.VeleroBackupName != "" {
		propagateReferencedBy(vmIndex.Checkpoints, checkpoint.ID, config.VeleroBackupName)
	}

	vmIndex.LastUpdated = time.Now().UTC()

	// Write updated index
	indexData, err := json.MarshalIndent(vmIndex, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal VM index: %w", err)
	}

	if err := putObjectBytes(store, config.BSLBucket, indexPath, indexData); err != nil {
		return fmt.Errorf("failed to write VM index: %w", err)
	}

	return nil
}

// updateBackupManifests creates/updates the per-backup manifest files.
func updateBackupManifests(
	_ context.Context, store velero.ObjectStore, config *UploaderConfig, logger logr.Logger,
) error {
	if config.VeleroBackupName == "" {
		logger.Info("No Velero backup name provided, skipping manifest update")
		return nil
	}

	// Load the VM index to get the checkpoint chain
	indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json", config.VMNamespace, config.VMName)
	data, err := getObjectBytes(store, config.BSLBucket, indexPath)
	if err != nil {
		return fmt.Errorf("failed to read VM index: %w", err)
	}

	var vmIndex VMIndex
	if err := json.Unmarshal(data, &vmIndex); err != nil {
		return fmt.Errorf("failed to parse VM index: %w", err)
	}

	// Build checkpoint chain (for restore)
	chain := buildCheckpointChain(vmIndex.Checkpoints, config.CheckpointName)
	if len(chain) == 0 {
		return fmt.Errorf("failed to build checkpoint chain: checkpoint %q not found in VM index", config.CheckpointName)
	}

	// Validate that the chain starts with a full backup (required for restore)
	if strings.ToLower(chain[0].Type) != BackupTypeFull {
		logger.Info("Checkpoint chain does not start with a full backup", "startsWithType", chain[0].Type)
		// This is a warning, not an error - the chain might be valid if this is the first backup
		// and it's marked as incremental by mistake, or the user knows what they're doing
	}

	// Create/update per-backup index.json
	backupIndexPath := fmt.Sprintf("manifests/%s/index.json", config.VeleroBackupName)

	var backupManifest BackupManifest

	// Check if backup manifest exists to distinguish "not found" from other errors
	exists, err := store.ObjectExists(config.BSLBucket, backupIndexPath)
	if err != nil {
		return fmt.Errorf("failed to check if backup manifest exists: %w", err)
	}

	if exists {
		data, err = getObjectBytes(store, config.BSLBucket, backupIndexPath)
		if err != nil {
			return fmt.Errorf("failed to read existing backup manifest: %w", err)
		}
		if err := json.Unmarshal(data, &backupManifest); err != nil {
			logger.Info("Failed to parse existing backup manifest, creating new", "reason", err.Error())
			backupManifest = BackupManifest{}
		}
	} else {
		backupManifest = BackupManifest{
			BackupName: config.VeleroBackupName,
			Timestamp:  time.Now().UTC(),
			VMs:        []VMBackupReference{},
		}
	}

	// Add/update VM reference
	vmManifestPath := fmt.Sprintf("manifests/%s/%s-%s.json",
		config.VeleroBackupName, config.VMNamespace, config.VMName)

	vmRef := VMBackupReference{
		Name:         config.VMName,
		Namespace:    config.VMNamespace,
		CheckpointID: config.CheckpointName,
		ManifestPath: vmManifestPath,
	}

	// Update or add VM reference
	found := false
	for i, ref := range backupManifest.VMs {
		if ref.Name == config.VMName && ref.Namespace == config.VMNamespace {
			backupManifest.VMs[i] = vmRef
			found = true
			break
		}
	}
	if !found {
		backupManifest.VMs = append(backupManifest.VMs, vmRef)
	}

	// Write backup manifest
	manifestData, err := json.MarshalIndent(backupManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal backup manifest: %w", err)
	}

	if err := putObjectBytes(store, config.BSLBucket, backupIndexPath, manifestData); err != nil {
		return fmt.Errorf("failed to write backup manifest: %w", err)
	}

	// Create per-VM backup manifest
	vmBackupManifest := VMBackupManifest{
		Namespace:       config.VMNamespace,
		Name:            config.VMName,
		CheckpointChain: chain,
		BackupName:      config.VeleroBackupName,
		Timestamp:       time.Now().UTC(),
	}

	vmManifestData, err := json.MarshalIndent(vmBackupManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal VM backup manifest: %w", err)
	}

	if err := putObjectBytes(store, config.BSLBucket, vmManifestPath, vmManifestData); err != nil {
		return fmt.Errorf("failed to write VM backup manifest: %w", err)
	}

	return nil
}

// archivedPaths holds the S3 paths where VMB and VMBT CRs were archived.
// These paths are passed to updateVMIndex so the checkpoint entry references them.
type archivedPaths struct {
	VMBObjectPath  string
	VMBTObjectPath string
}

// archiveKubeResources fetches VMB and VMBT CRs from the K8s API and uploads them
// as JSON to S3 alongside the qcow2 files. This is a fatal step — if archiving fails,
// we don't update index.json, ensuring references in the index are always valid.
//
// S3 paths (both in the checkpoint directory):
//   - VMB:  checkpoints/<ns>/<vm>/<checkpoint>/vmb.json
//   - VMBT: checkpoints/<ns>/<vm>/<checkpoint>/vmbt.json
func archiveKubeResources(
	ctx context.Context, store velero.ObjectStore, k8sClient client.Client, cfg *UploaderConfig, logger logr.Logger,
) (*archivedPaths, error) {
	paths := &archivedPaths{}

	// Fetch and archive VMB
	if cfg.VMBName != "" {
		vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name: cfg.VMBName, Namespace: cfg.VMNamespace,
		}, vmb); err != nil {
			return nil, fmt.Errorf("failed to fetch VMB %s/%s: %w", cfg.VMNamespace, cfg.VMBName, err)
		}

		data, err := json.MarshalIndent(vmb, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to serialize VMB: %w", err)
		}

		paths.VMBObjectPath = fmt.Sprintf("checkpoints/%s/%s/%s/vmb.json",
			cfg.VMNamespace, cfg.VMName, cfg.CheckpointName)
		if err := putObjectBytes(store, cfg.BSLBucket, paths.VMBObjectPath, data); err != nil {
			return nil, fmt.Errorf("failed to upload vmb.json: %w", err)
		}
		logger.Info("Archived VMB", "path", paths.VMBObjectPath)
	}

	// Fetch and archive VMBT (stored per-checkpoint for audit trail).
	// Set LatestCheckpoint to the current VMB's checkpoint before archiving.
	// KubeVirt does NOT update VMBT.Status.LatestCheckpoint — that is our
	// responsibility. The archived VMBT with the correct checkpoint is used
	// by prepareVMBackupTracker to set up the next incremental backup.
	if cfg.VMBTName != "" {
		vmbt := &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{}
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name: cfg.VMBTName, Namespace: cfg.VMNamespace,
		}, vmbt); err != nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("failed to fetch VMBT %s/%s: %w",
					cfg.VMNamespace, cfg.VMBTName, err)
			}
			// VMBT was deleted by a concurrent DataUpload's prepareVMBackupTracker.
			// Reconstruct a minimal VMBT for archiving — the only field the controller
			// reads back is Status.LatestCheckpoint, which we set below anyway.
			logger.Info("VMBT not found in cluster, reconstructing for archival",
				"vmbt", cfg.VMBTName)
			vmbt = reconstructVMBT(cfg)
		}

		// Set LatestCheckpoint to the current checkpoint so the next backup
		// can use it for incremental. This is the checkpoint created by the
		// VMB that just completed.
		if cfg.CheckpointName != "" {
			vmbt.Status = &kubevirtbackupv1alpha1.VirtualMachineBackupTrackerStatus{
				LatestCheckpoint: &kubevirtbackupv1alpha1.BackupCheckpoint{
					Name: cfg.CheckpointName,
				},
			}
			logger.Info("Set VMBT LatestCheckpoint before archiving",
				"latestCheckpoint", cfg.CheckpointName)
		}

		data, err := json.MarshalIndent(vmbt, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to serialize VMBT: %w", err)
		}

		paths.VMBTObjectPath = fmt.Sprintf("checkpoints/%s/%s/%s/vmbt.json",
			cfg.VMNamespace, cfg.VMName, cfg.CheckpointName)
		if err := putObjectBytes(store, cfg.BSLBucket, paths.VMBTObjectPath, data); err != nil {
			return nil, fmt.Errorf("failed to upload vmbt.json: %w", err)
		}
		logger.Info("Archived VMBT", "path", paths.VMBTObjectPath)
	}

	return paths, nil
}

// reconstructVMBT builds a minimal VirtualMachineBackupTracker from the uploader
// config. Used when the VMBT was deleted from the cluster by a concurrent
// DataUpload before the datamover pod could archive it. The only field the
// controller reads from the archived vmbt.json is Status.LatestCheckpoint,
// which is set by the caller after this function returns.
func reconstructVMBT(cfg *UploaderConfig) *kubevirtbackupv1alpha1.VirtualMachineBackupTracker {
	apiGroup := apiGroupKubeVirt
	return &kubevirtbackupv1alpha1.VirtualMachineBackupTracker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.VMBTName,
			Namespace: cfg.VMNamespace,
		},
		Spec: kubevirtbackupv1alpha1.VirtualMachineBackupTrackerSpec{
			Source: corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VirtualMachine",
				Name:     cfg.VMName,
			},
		},
	}
}

// cleanupKubeResources deletes VMB CRs from the cluster after they have been
// archived to S3. The VMBT is intentionally preserved so KubeVirt can use it
// during VM lifecycle events (restarts, migrations) to redefine libvirt checkpoints.
// This is non-fatal — if deletion fails, we log a warning but don't fail the upload.
// See https://github.com/migtools/kubevirt-datamover-controller/issues/32.
func cleanupKubeResources(ctx context.Context, k8sClient client.Client, cfg *UploaderConfig, logger logr.Logger) {
	// Delete VMB by constructing a minimal object with just name/namespace.
	// No need to Get first — Delete with NotFound is a no-op.
	if cfg.VMBName != "" {
		vmb := &kubevirtbackupv1alpha1.VirtualMachineBackup{}
		vmb.Name = cfg.VMBName
		vmb.Namespace = cfg.VMNamespace
		if err := k8sClient.Delete(ctx, vmb); err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Info("Failed to delete VMB from cluster",
					"vmb", cfg.VMNamespace+"/"+cfg.VMBName, "reason", err.Error())
			}
		} else {
			logger.Info("Deleted VMB from cluster", "vmb", cfg.VMNamespace+"/"+cfg.VMBName)
		}
	}
}

// newKubeClient creates an in-cluster Kubernetes client with KubeVirt backup types registered.
// Extracted as a variable to allow overriding in tests.
var newKubeClient = func() (client.Client, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := kubevirtbackupv1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register KubeVirt backup scheme: %w", err)
	}
	if err := kubevirtcorev1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to register KubeVirt scheme: %w", err)
	}

	restConfig, err := restcfg.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	k8sClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create K8s client: %w", err)
	}

	return k8sClient, nil
}

// propagateReferencedBy walks the parent chain of the given checkpoint and
// adds backupName to each ancestor's ReferencedBy list (skipping the checkpoint
// itself, which is handled during creation). This ensures that every checkpoint
// in the chain correctly lists all backups that depend on it.
func propagateReferencedBy(checkpoints []CheckpointEntry, startID, backupName string) {
	// Build index for O(1) lookup by ID → slice position.
	idxMap := make(map[string]int, len(checkpoints))
	for i, cp := range checkpoints {
		idxMap[cp.ID] = i
	}

	// Find the starting checkpoint to get its parent.
	startIdx, ok := idxMap[startID]
	if !ok {
		return
	}
	parentID := checkpoints[startIdx].Parent

	// Walk the parent chain, adding backupName to each ancestor.
	visited := make(map[string]bool)
	for parentID != "" {
		if visited[parentID] {
			break // cycle guard
		}
		visited[parentID] = true

		idx, ok := idxMap[parentID]
		if !ok {
			break // parent not found
		}
		checkpoints[idx].ReferencedBy = mergeReferencedBy(
			checkpoints[idx].ReferencedBy, []string{backupName})
		parentID = checkpoints[idx].Parent
	}
}

// mergeReferencedBy returns the union of two ReferencedBy slices, preserving
// order (existing entries first) and eliminating duplicates.
func mergeReferencedBy(existing, additional []string) []string {
	seen := make(map[string]bool, len(existing)+len(additional))
	// Deduplicate existing entries too — corrupted index data or older
	// buggy writes may have introduced duplicates that would otherwise
	// be carried forward on every merge.
	merged := make([]string, 0, len(existing)+len(additional))
	for _, v := range existing {
		if !seen[v] {
			merged = append(merged, v)
			seen[v] = true
		}
	}
	for _, v := range additional {
		if !seen[v] {
			merged = append(merged, v)
			seen[v] = true
		}
	}
	return merged
}

// buildCheckpointChain builds the ordered list of checkpoints needed for restore.
// Starting from the target checkpoint, follows parent references back to the base full backup.
func buildCheckpointChain(checkpoints []CheckpointEntry, targetID string) []CheckpointEntry {
	// Build lookup map
	cpMap := make(map[string]CheckpointEntry)
	for _, cp := range checkpoints {
		cpMap[cp.ID] = cp
	}

	// Build chain by following parents.
	// Track visited IDs to guard against cycles in corrupted index data.
	var chain []CheckpointEntry
	visited := make(map[string]bool)
	currentID := targetID

	for currentID != "" {
		if visited[currentID] {
			break
		}
		cp, ok := cpMap[currentID]
		if !ok {
			break
		}
		visited[currentID] = true
		// Prepend to chain (oldest first)
		chain = append([]CheckpointEntry{cp}, chain...)
		currentID = cp.Parent
	}

	return chain
}
