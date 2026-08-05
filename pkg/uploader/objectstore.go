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
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PutObjectBytes uploads bytes to a velero.ObjectStore.
func PutObjectBytes(store velero.ObjectStore, bucket, key string, data []byte) error {
	return store.PutObject(bucket, key, bytes.NewReader(data))
}

// GetObjectBytes downloads an object as bytes from a velero.ObjectStore.
func GetObjectBytes(store velero.ObjectStore, bucket, key string) ([]byte, error) {
	reader, err := store.GetObject(bucket, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	return io.ReadAll(reader)
}

// GetVMIndex returns a VMIndex for this VM if it exists
func GetVMIndex(store velero.ObjectStore, ns, name, bucket string, logger logr.Logger) (VMIndex, bool, error) {
	indexPath := GetVMIndexPath(ns, name)
	// Try to load existing index
	var vmIndex VMIndex

	exists, err := store.ObjectExists(bucket, indexPath)
	if err != nil {
		return vmIndex, false, fmt.Errorf("failed to check if VM index exists: %w", err)
	}
	if exists {
		data, err := GetObjectBytes(store, bucket, indexPath)
		if err != nil {
			return vmIndex, false, fmt.Errorf("failed to read existing VM index: %w", err)
		}
		if err = json.Unmarshal(data, &vmIndex); err != nil {
			logger.Info("Failed to parse existing index, creating new", "reason", err.Error())
			exists = false
		}
	}
	if !exists {
		// Index doesn't exist, create new
		vmIndex = VMIndex{
			VMName:      name,
			Namespace:   ns,
			Checkpoints: []CheckpointEntry{},
		}
	}
	return vmIndex, exists, err
}

// GetVMIndexPath gets the path for the VMIndex
func GetVMIndexPath(ns, name string) string {
	return fmt.Sprintf("checkpoints/%s/%s/index.json", ns, name)
}

// PutVMIndex uploads a VMIndex to s3
func PutVMIndex(store velero.ObjectStore, ns, name, bucket string, vmIndex VMIndex) error {
	// Write updated index
	indexPath := GetVMIndexPath(ns, name)
	indexData, err := json.MarshalIndent(vmIndex, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal VM index: %w", err)
	}

	if err := PutObjectBytes(store, bucket, indexPath, indexData); err != nil {
		return fmt.Errorf("failed to write VM index: %w", err)
	}
	return nil
}

// GetBackupManifest returns a BackupManifest for this backup if it exists
func GetBackupManifest(
	store velero.ObjectStore,
	backupName, bucket string,
	logger logr.Logger,
) (BackupManifest, bool, error) {
	indexPath := GetBackupManifestPath(backupName)
	// Try to load existing index
	var backupManifest BackupManifest

	exists, err := store.ObjectExists(bucket, indexPath)
	if err != nil {
		return backupManifest, false, fmt.Errorf("failed to check if backup manifest exists: %w", err)
	}
	if exists {
		data, err := GetObjectBytes(store, bucket, indexPath)
		if err != nil {
			return backupManifest, false, fmt.Errorf("failed to read existing backup manifest: %w", err)
		}
		if err = json.Unmarshal(data, &backupManifest); err != nil {
			logger.Info("Failed to parse existing backup manifest, creating new", "reason", err.Error())
			exists = false
		}
	}
	if !exists {
		// manifest doesn't exist, create new
		backupManifest = BackupManifest{
			BackupName: backupName,
			Timestamp:  time.Now().UTC(),
			VMs:        []VMBackupReference{},
		}
	}
	return backupManifest, exists, err
}

// GetBackupManifestPath gets the path for the BackupManifest
func GetBackupManifestPath(backupName string) string {
	return fmt.Sprintf("manifests/%s/index.json", backupName)
}

// PutBackupManifest uploads a BackupManifest to s3
func PutBackupManifest(store velero.ObjectStore, backupName, bucket string, backupManifest BackupManifest) error {
	// Write updated index
	indexPath := GetBackupManifestPath(backupName)
	indexData, err := json.MarshalIndent(backupManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal backup manifest: %w", err)
	}

	if err := PutObjectBytes(store, bucket, indexPath, indexData); err != nil {
		return fmt.Errorf("failed to write backup manifest: %w", err)
	}
	return nil

}

// GetVMBackupManifest returns a VMBackupManifest for this backup if it exists
func GetVMBackupManifest(
	store velero.ObjectStore,
	ns, name, backupName, bucket string,
	logger logr.Logger,
) (VMBackupManifest, bool, error) {
	manifestPath := GetVMBackupManifestPath(ns, name, backupName)
	// Try to load existing index
	var vmBackupManifest VMBackupManifest

	exists, err := store.ObjectExists(bucket, manifestPath)
	if err != nil {
		return vmBackupManifest, false, fmt.Errorf("failed to check if VM backup manifest exists: %w", err)
	}
	if exists {
		data, err := GetObjectBytes(store, bucket, manifestPath)
		if err != nil {
			return vmBackupManifest, false, fmt.Errorf("failed to read existing vm backup manifest: %w", err)
		}
		if err = json.Unmarshal(data, &vmBackupManifest); err != nil {
			logger.Info("Failed to parse existing vm backup manifest", "reason", err.Error())
			exists = false
		}
	}
	return vmBackupManifest, exists, err
}

// GetVMBackupManifestPath gets the path for the VMBackupManifest
func GetVMBackupManifestPath(ns, name, backupName string) string {
	return fmt.Sprintf("manifests/%s/%s-%s.json", backupName, ns, name)
}

// PutVMBackupManifest uploads a BackupManifest to s3
func PutVMBackupManifest(
	store velero.ObjectStore,
	ns, name, backupName, bucket string,
	vmBackupManifest VMBackupManifest,
) error {
	// Write updated index
	indexPath := GetVMBackupManifestPath(ns, name, backupName)
	indexData, err := json.MarshalIndent(vmBackupManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal VM backup manifest: %w", err)
	}

	if err := PutObjectBytes(store, bucket, indexPath, indexData); err != nil {
		return fmt.Errorf("failed to write VM backup manifest: %w", err)
	}
	return nil

}

// GetVMBPath gets the path for the VMBackup
func GetVMBPath(ns, name, checkpoint string) string {
	return fmt.Sprintf("checkpoints/%s/%s/%s/vmb.json", ns, name, checkpoint)
}

// PutVMB uploads a VMB to s3
func PutVMB(
	store velero.ObjectStore,
	ns, name, checkpoint, bucket string,
	vmb *kubevirtbackupv1alpha1.VirtualMachineBackup,
) error {
	// Write updated index
	vmbPath := GetVMBPath(ns, name, checkpoint)
	vmbData, err := json.MarshalIndent(vmb, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize VMB: %w", err)
	}

	if err := PutObjectBytes(store, bucket, vmbPath, vmbData); err != nil {
		return fmt.Errorf("failed to upload vmb.json: %w", err)
	}
	return nil

}

// GetVMBTPath gets the path for the VMBackupTracker
func GetVMBTPath(ns, name, checkpoint string) string {
	return fmt.Sprintf("checkpoints/%s/%s/%s/vmbt.json", ns, name, checkpoint)
}

// PutVMBT uploads a VMBT to s3
func PutVMBT(
	store velero.ObjectStore,
	ns, name, checkpoint, bucket string,
	vmbt *kubevirtbackupv1alpha1.VirtualMachineBackupTracker,
) error {
	// Write updated index
	vmbtPath := GetVMBTPath(ns, name, checkpoint)
	vmbtData, err := json.MarshalIndent(vmbt, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize VMBT: %w", err)
	}

	if err := PutObjectBytes(store, bucket, vmbtPath, vmbtData); err != nil {
		return fmt.Errorf("failed to upload vmbt.json: %w", err)
	}
	return nil

}

// DeleteVMIndex deletes a VMIndex from s3
func DeleteVMIndex(store velero.ObjectStore, ns, name, bucket string) error {
	manifestPath := GetVMIndexPath(ns, name)

	if err := store.DeleteObject(bucket, manifestPath); err != nil {
		return fmt.Errorf("failed to delete vm index: %w", err)
	}
	return nil

}

// DeleteBackupManifest deletes a BackupManifest from s3
func DeleteBackupManifest(store velero.ObjectStore, backupName, bucket string) error {
	manifestPath := GetBackupManifestPath(backupName)

	if err := store.DeleteObject(bucket, manifestPath); err != nil {
		return fmt.Errorf("failed to delete backup manifest: %w", err)
	}
	return nil

}

// DeleteVMBackupManifest deletes a VMBackupManifest from s3
func DeleteVMBackupManifest(store velero.ObjectStore, ns, name, backupName, bucket string) error {
	manifestPath := GetVMBackupManifestPath(ns, name, backupName)

	if err := store.DeleteObject(bucket, manifestPath); err != nil {
		return fmt.Errorf("failed to delete vm backup manifest: %w", err)
	}
	return nil

}

// DeleteVMB deletes a VMB from s3
func DeleteVMB(store velero.ObjectStore, ns, name, checkpoint, bucket string) error {
	vmbPath := GetVMBPath(ns, name, checkpoint)

	if err := store.DeleteObject(bucket, vmbPath); err != nil {
		return fmt.Errorf("failed to delete vmb.json: %w", err)
	}
	return nil

}

// DeleteVMBT deletes a VMBT from s3
func DeleteVMBT(store velero.ObjectStore, ns, name, checkpoint, bucket string) error {
	vmbtPath := GetVMBTPath(ns, name, checkpoint)

	if err := store.DeleteObject(bucket, vmbtPath); err != nil {
		return fmt.Errorf("failed to delete vmbt.json: %w", err)
	}
	return nil

}

// GetQCOWPath gets the path for the qcow2 file
func GetQCOWPath(ns, name, checkpoint, qcowName string) string {
	return fmt.Sprintf("checkpoints/%s/%s/%s/%s", ns, name, checkpoint, qcowName)
}

// DeleteQCOW deletes a qcow2 file from s3
func DeleteQCOW(store velero.ObjectStore, ns, name, checkpoint, qcowName, bucket string) error {
	qcowPath := GetQCOWPath(ns, name, checkpoint, qcowName)

	if err := store.DeleteObject(bucket, qcowPath); err != nil {
		return fmt.Errorf("failed to delete %s: %w", qcowName, err)
	}
	return nil

}

// InitObjectStore creates an ObjectStore based on the provider type.
// Credentials and S3-compatible settings are passed through the config map
// to Init(), which handles temp file creation for in-memory credentials.
// Takes *common.ObjectStoreConfig (not the fuller UploaderConfig) so both
// the uploader and downloader runtimes can share this initialization logic.
func InitObjectStore(cfg *common.ObjectStoreConfig) (velero.ObjectStore, error) {
	if cfg.BSLProvider == "" {
		return nil, fmt.Errorf("BSL provider is required but was empty")
	}

	// Build config map for the object store.
	configMap := map[string]string{
		"bucket": cfg.BSLBucket,
		"prefix": cfg.BSLPrefix,
		"region": cfg.BSLRegion,
	}

	// Credentials: prefer in-memory data (controller path), fall back to file (pod path).
	if len(cfg.CredentialsData) > 0 {
		configMap["credentialsData"] = string(cfg.CredentialsData)
	} else if cfg.CredentialsFile != "" {
		configMap["credentialsFile"] = cfg.CredentialsFile
	}

	const trueStr = "true"

	// S3-compatible storage settings
	if cfg.BSLS3URL != "" {
		configMap["s3Url"] = cfg.BSLS3URL
	}
	if cfg.BSLS3ForcePathStyle {
		configMap["s3ForcePathStyle"] = trueStr
	}
	if cfg.BSLInsecureSkipTLSVerify {
		configMap["insecureSkipTLSVerify"] = trueStr
	}
	if cfg.BSLCACert != "" {
		configMap["caCert"] = cfg.BSLCACert
	}
	if cfg.BSLServerSideEncryption != "" {
		configMap["serverSideEncryption"] = cfg.BSLServerSideEncryption
	}
	if cfg.BSLKMSKeyID != "" {
		configMap["kmsKeyId"] = cfg.BSLKMSKeyID
	}
	if cfg.BSLChecksumAlgorithm != "" {
		configMap["checksumAlgorithm"] = cfg.BSLChecksumAlgorithm
	}
	if cfg.BSLCustomerKeyEncryptionFile != "" {
		configMap["customerKeyEncryptionFile"] = cfg.BSLCustomerKeyEncryptionFile
	}
	if cfg.BSLServiceAccount != "" {
		configMap["serviceAccount"] = cfg.BSLServiceAccount
	}
	if cfg.BSLKMSKeyName != "" {
		configMap["kmsKeyName"] = cfg.BSLKMSKeyName
	}
	if cfg.BSLResourceGroup != "" {
		configMap["resourceGroup"] = cfg.BSLResourceGroup
	}
	if cfg.BSLStorageAccount != "" {
		configMap["storageAccount"] = cfg.BSLStorageAccount
	}
	if cfg.BSLStorageAccountKeyEnvVar != "" {
		configMap["storageAccountKeyEnvVar"] = cfg.BSLStorageAccountKeyEnvVar
	}
	if cfg.BSLStorageAccountURI != "" {
		configMap["storageAccountURI"] = cfg.BSLStorageAccountURI
	}
	if cfg.BSLSubscriptionID != "" {
		configMap["subscriptionId"] = cfg.BSLSubscriptionID
	}
	if cfg.BSLUseAAD {
		configMap["useAAD"] = trueStr
	}
	if cfg.BSLActiveDirectoryAuthorityURI != "" {
		configMap["activeDirectoryAuthorityURI"] = cfg.BSLActiveDirectoryAuthorityURI
	}

	switch strings.ToLower(cfg.BSLProvider) {
	case "aws":
		return NewS3ObjectStore(configMap)
	case "gcp":
		return NewGCPObjectStore(configMap)
	case "azure":
		return NewAzureObjectStore(configMap)
	default:
		// Try S3-compatible for unknown providers
		return NewS3ObjectStore(configMap)
	}
}

// DefaultCredentialKey is the default key in BSL credential secrets
const DefaultCredentialKey = "cloud"

// BSLConfig holds extracted BSL configuration used by both the datamover pod
// and the checkpoint lookup.
type BSLConfig struct {
	Provider       string
	Bucket         string
	Prefix         string // Datamover prefix (e.g., "velero-kubevirt-datamover")
	Region         string
	CredentialName string
	CredentialKey  string

	// S3-compatible storage provider settings
	S3URL                     string
	S3ForcePathStyle          bool
	InsecureSkipTLSVerify     bool
	CACert                    string
	ServerSideEncryption      string
	KMSKeyID                  string
	ChecksumAlgorithm         string
	CustomerKeyEncryptionFile string

	// GCP-specific storage provider settings
	ServiceAccount string
	KMSKeyName     string

	// Azure-specific storage provider settings
	ResourceGroup               string
	StorageAccount              string
	StorageAccountKeyEnvVar     string
	StorageAccountURI           string
	SubscriptionID              string
	UseAAD                      bool
	ActiveDirectoryAuthorityURI string
}

// ExtractBSLConfig extracts and validates common BSL configuration fields.
// The returned prefix includes the datamover suffix (e.g., "velero-kubevirt-datamover").
func ExtractBSLConfig(bsl *velerov1.BackupStorageLocation) (*BSLConfig, error) {
	bucket := ""
	prefix := ""
	if bsl.Spec.ObjectStorage != nil {
		bucket = bsl.Spec.ObjectStorage.Bucket
		prefix = bsl.Spec.ObjectStorage.Prefix
	}
	if bucket == "" {
		return nil, fmt.Errorf("BSL %s has no bucket configured", bsl.Name)
	}

	// Add our datamover prefix to the BSL prefix
	if prefix != "" {
		prefix = prefix + "-" + common.DatamoverBSLPrefix
	} else {
		prefix = common.DatamoverBSLPrefix
	}

	region := ""
	s3URL := ""
	s3ForcePathStyle := false
	insecureSkipTLSVerify := false
	caCert := ""
	serverSideEncryption := ""
	kmsKeyID := ""
	checksumAlgorithm := ""
	customerKeyEncryptionFile := ""
	serviceAccount := ""
	kmsKeyName := ""
	resourceGroup := ""
	storageAccount := ""
	storageAccountKeyEnvVar := ""
	storageAccountURI := ""
	subscriptionID := ""
	useAAD := false
	activeDirectoryAuthorityURI := ""
	if bsl.Spec.Config != nil {
		region = bsl.Spec.Config["region"]
		s3URL = bsl.Spec.Config["s3Url"]
		s3ForcePathStyle = common.ParseBool(bsl.Spec.Config["s3ForcePathStyle"])
		insecureSkipTLSVerify = common.ParseBool(bsl.Spec.Config["insecureSkipTLSVerify"])
		caCert = bsl.Spec.Config["caCert"]
		serverSideEncryption = bsl.Spec.Config["serverSideEncryption"]
		kmsKeyID = bsl.Spec.Config["kmsKeyId"]
		checksumAlgorithm = bsl.Spec.Config["checksumAlgorithm"]
		customerKeyEncryptionFile = bsl.Spec.Config["customerKeyEncryptionFile"]
		serviceAccount = bsl.Spec.Config["serviceAccount"]
		kmsKeyName = bsl.Spec.Config["kmsKeyName"]
		resourceGroup = bsl.Spec.Config["resourceGroup"]
		storageAccount = bsl.Spec.Config["storageAccount"]
		storageAccountKeyEnvVar = bsl.Spec.Config["storageAccountKeyEnvVar"]
		storageAccountURI = bsl.Spec.Config["storageAccountURI"]
		subscriptionID = bsl.Spec.Config["subscriptionId"]
		useAAD = common.ParseBool(bsl.Spec.Config["useAAD"])
		activeDirectoryAuthorityURI = bsl.Spec.Config["activeDirectoryAuthorityURI"]
	}

	credName := ""
	credKey := DefaultCredentialKey
	if bsl.Spec.Credential != nil {
		credName = bsl.Spec.Credential.Name
		if bsl.Spec.Credential.Key != "" {
			credKey = bsl.Spec.Credential.Key
		}
	}

	return &BSLConfig{
		Provider:                    bsl.Spec.Provider,
		Bucket:                      bucket,
		Prefix:                      prefix,
		Region:                      region,
		CredentialName:              credName,
		CredentialKey:               credKey,
		S3URL:                       s3URL,
		S3ForcePathStyle:            s3ForcePathStyle,
		InsecureSkipTLSVerify:       insecureSkipTLSVerify,
		CACert:                      caCert,
		ServerSideEncryption:        serverSideEncryption,
		KMSKeyID:                    kmsKeyID,
		ChecksumAlgorithm:           checksumAlgorithm,
		CustomerKeyEncryptionFile:   customerKeyEncryptionFile,
		ServiceAccount:              serviceAccount,
		KMSKeyName:                  kmsKeyName,
		ResourceGroup:               resourceGroup,
		StorageAccount:              storageAccount,
		StorageAccountKeyEnvVar:     storageAccountKeyEnvVar,
		StorageAccountURI:           storageAccountURI,
		SubscriptionID:              subscriptionID,
		UseAAD:                      useAAD,
		ActiveDirectoryAuthorityURI: activeDirectoryAuthorityURI,
	}, nil
}

// InitObjectStoreFromBSL extracts BSL config, fetches credentials, and initializes
// an ObjectStore client.
func InitObjectStoreFromBSL(
	ctx context.Context,
	k8sClient client.Client,
	oadpNamespace string,
	bsl *velerov1.BackupStorageLocation,
	factory func(c *common.ObjectStoreConfig) (velero.ObjectStore, error),
) (velero.ObjectStore, *BSLConfig, error) {
	cfg, err := ExtractBSLConfig(bsl)
	if err != nil {
		return nil, nil, err
	}

	if cfg.CredentialName == "" {
		return nil, nil, fmt.Errorf("BSL %s has no credential configured", bsl.Name)
	}

	credData, err := GetCredentialsFromBSL(ctx, k8sClient, oadpNamespace, bsl)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get BSL credentials: %w", err)
	}

	if factory == nil {
		factory = InitObjectStore
	}

	store, err := factory(&common.ObjectStoreConfig{
		BSLProvider:                    cfg.Provider,
		BSLBucket:                      cfg.Bucket,
		BSLPrefix:                      cfg.Prefix,
		BSLRegion:                      cfg.Region,
		BSLS3URL:                       cfg.S3URL,
		BSLS3ForcePathStyle:            cfg.S3ForcePathStyle,
		BSLInsecureSkipTLSVerify:       cfg.InsecureSkipTLSVerify,
		BSLCACert:                      cfg.CACert,
		BSLServerSideEncryption:        cfg.ServerSideEncryption,
		BSLKMSKeyID:                    cfg.KMSKeyID,
		BSLChecksumAlgorithm:           cfg.ChecksumAlgorithm,
		BSLCustomerKeyEncryptionFile:   cfg.CustomerKeyEncryptionFile,
		BSLServiceAccount:              cfg.ServiceAccount,
		BSLKMSKeyName:                  cfg.KMSKeyName,
		BSLResourceGroup:               cfg.ResourceGroup,
		BSLStorageAccount:              cfg.StorageAccount,
		BSLStorageAccountKeyEnvVar:     cfg.StorageAccountKeyEnvVar,
		BSLStorageAccountURI:           cfg.StorageAccountURI,
		BSLSubscriptionID:              cfg.SubscriptionID,
		BSLUseAAD:                      cfg.UseAAD,
		BSLActiveDirectoryAuthorityURI: cfg.ActiveDirectoryAuthorityURI,
		CredentialsData:                credData,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize object store: %w", err)
	}

	return store, cfg, nil
}

// GetCredentialsFromBSL reads the BSL credential secret and returns the raw
// credential bytes. The credentials are kept in memory and never written to disk.
func GetCredentialsFromBSL(
	ctx context.Context, k8sClient client.Client, oadpNamespace string, bsl *velerov1.BackupStorageLocation,
) ([]byte, error) {
	if bsl.Spec.Credential == nil {
		return nil, fmt.Errorf("BSL %s has no credential configured", bsl.Name)
	}

	secretName := bsl.Spec.Credential.Name
	secretKey := bsl.Spec.Credential.Key
	if secretKey == "" {
		secretKey = DefaultCredentialKey
	}

	// BSL credentials secret is in the same namespace as the BSL
	namespace := bsl.Namespace
	if namespace == "" {
		namespace = oadpNamespace
	}

	// Fetch the secret
	secret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name: secretName, Namespace: namespace,
	}, secret); err != nil {
		return nil, fmt.Errorf(
			"failed to get credential secret %s/%s: %w",
			namespace, secretName, err,
		)
	}

	credData, ok := secret.Data[secretKey]
	if !ok {
		return nil, fmt.Errorf(
			"credential secret %s/%s does not contain key %q",
			namespace, secretName, secretKey,
		)
	}

	return credData, nil
}
