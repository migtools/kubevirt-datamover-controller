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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-logr/logr"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	kubevirtbackupv1alpha1 "kubevirt.io/api/backup/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Compile-time check that S3ObjectStore implements velero.ObjectStore
var _ velero.ObjectStore = (*S3ObjectStore)(nil)

// S3ObjectStore implements velero.ObjectStore for AWS S3 and S3-compatible storage.
type S3ObjectStore struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
	prefix   string
}

// NewS3ObjectStore creates a new S3ObjectStore from a config map.
// Supported keys: bucket, prefix, region, credentialsFile, credentialsData,
// s3Url, s3ForcePathStyle, insecureSkipTLSVerify, caCert.
func NewS3ObjectStore(configMap map[string]string) (*S3ObjectStore, error) {
	store := &S3ObjectStore{
		bucket: configMap["bucket"],
		prefix: configMap["prefix"],
	}

	if err := store.Init(configMap); err != nil {
		return nil, err
	}

	return store, nil
}

// Init initializes the ObjectStore with the provided config.
// This implements velero.ObjectStore.Init().
// Expected config keys: bucket, prefix, region, credentialsFile, credentialsData,
// s3Url, s3ForcePathStyle, insecureSkipTLSVerify, caCert.
// Credential parsing is delegated to the AWS SDK via
// config.WithSharedCredentialsFiles, matching Velero's approach.
func (s *S3ObjectStore) Init(configMap map[string]string) error {
	bucket := configMap["bucket"]
	prefix := configMap["prefix"]
	region := configMap["region"]

	if bucket == "" {
		return fmt.Errorf("bucket is required in config")
	}

	s.bucket = bucket
	s.prefix = prefix

	ctx := context.Background()

	// Build config options
	var opts []func(*config.LoadOptions) error

	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	// Load credentials using the AWS SDK's built-in parser, matching Velero's
	// pattern (velero/pkg/repository/config/aws.go). This correctly handles
	// quoted values, named profiles, session tokens, and role assumption.
	if credFile := configMap["credentialsFile"]; credFile != "" {
		opts = append(opts,
			config.WithSharedCredentialsFiles([]string{credFile}),
			config.WithSharedConfigFiles([]string{credFile}),
		)
	} else if credData := configMap["credentialsData"]; credData != "" {
		// Write in-memory credentials to a temp file for the AWS SDK to parse.
		tmpFile, err := writeCredentialsToTempFile([]byte(credData))
		if err != nil {
			return fmt.Errorf("failed to write credentials to temp file: %w", err)
		}
		defer func() { _ = os.Remove(tmpFile) }()
		opts = append(opts,
			config.WithSharedCredentialsFiles([]string{tmpFile}),
			config.WithSharedConfigFiles([]string{tmpFile}),
		)
	}

	// Add retry configuration for transient errors
	opts = append(opts, config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 3
		})
	}))

	// Configure custom TLS for S3-compatible endpoints with private CAs
	// or when TLS verification needs to be skipped.
	insecureSkipTLSVerify := strings.EqualFold(configMap["insecureSkipTLSVerify"], "true")
	caCert := configMap["caCert"]
	if insecureSkipTLSVerify || caCert != "" {
		httpClient, err := buildTLSHTTPClient(insecureSkipTLSVerify, caCert)
		if err != nil {
			return fmt.Errorf("failed to configure TLS: %w", err)
		}
		opts = append(opts, config.WithHTTPClient(httpClient))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Apply S3-specific options: custom endpoint URL and path-style addressing.
	s3URL := configMap["s3Url"]
	forcePathStyle := strings.EqualFold(configMap["s3ForcePathStyle"], "true")

	if s3URL != "" {
		if !isValidS3URLScheme(s3URL) {
			return fmt.Errorf("invalid s3Url %q: must start with http:// or https://", s3URL)
		}
	}

	s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		if s3URL != "" {
			o.BaseEndpoint = aws.String(s3URL)
		}
		if forcePathStyle {
			o.UsePathStyle = true
		}
	})
	s.uploader = manager.NewUploader(s.client)

	return nil
}

// fullKey returns the full object key including the prefix.
func (s *S3ObjectStore) fullKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return strings.TrimSuffix(s.prefix, "/") + "/" + strings.TrimPrefix(key, "/")
}

// PutObject uploads an object to S3.
// Implements velero.ObjectStore.PutObject().
func (s *S3ObjectStore) PutObject(bucket, key string, body io.Reader) error {
	_, err := s.uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("failed to upload object %s: %w", key, err)
	}
	return nil
}

// GetObject retrieves an object from S3.
// Implements velero.ObjectStore.GetObject().
func (s *S3ObjectStore) GetObject(bucket, key string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}
	return output.Body, nil
}

// ObjectExists checks if an object exists in S3.
// Implements velero.ObjectStore.ObjectExists().
func (s *S3ObjectStore) ObjectExists(bucket, key string) (bool, error) {
	_, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		// Check if it's a not found error using AWS SDK error types
		var notFound *s3types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		// Also check for NoSuchKey which can be returned for missing objects
		var noSuchKey *s3types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object existence %s: %w", key, err)
	}
	return true, nil
}

// DeleteObject removes an object from S3.
// Implements velero.ObjectStore.DeleteObject().
func (s *S3ObjectStore) DeleteObject(bucket, key string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object %s: %w", key, err)
	}
	return nil
}

// ListCommonPrefixes gets a list of all object key prefixes that start with
// the specified prefix and stop at the next instance of the provided delimiter.
// Implements velero.ObjectStore.ListCommonPrefixes().
func (s *S3ObjectStore) ListCommonPrefixes(bucket, prefix, delimiter string) ([]string, error) {
	fullPrefix := s.fullKey(prefix)

	var prefixes []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(fullPrefix),
		Delimiter: aws.String(delimiter),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list common prefixes: %w", err)
		}
		for _, p := range page.CommonPrefixes {
			if p.Prefix != nil {
				prefixes = append(prefixes, *p.Prefix)
			}
		}
	}

	return prefixes, nil
}

// ListObjects gets a list of all keys in the specified bucket that have the given prefix.
// Implements velero.ObjectStore.ListObjects().
func (s *S3ObjectStore) ListObjects(bucket, prefix string) ([]string, error) {
	fullPrefix := s.fullKey(prefix)

	var keys []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}

	return keys, nil
}

// CreateSignedURL creates a pre-signed URL for the given bucket and key that expires after ttl.
// Implements velero.ObjectStore.CreateSignedURL().
func (s *S3ObjectStore) CreateSignedURL(bucket, key string, ttl time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(s.client)
	req, err := presignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("failed to create signed URL: %w", err)
	}
	return req.URL, nil
}

// Convenience methods for our uploader use case

// PutObjectWithBucket uploads an object using the configured bucket.
func (s *S3ObjectStore) PutObjectWithBucket(key string, body io.Reader) error {
	return s.PutObject(s.bucket, key, body)
}

// GetObjectWithBucket retrieves an object using the configured bucket.
func (s *S3ObjectStore) GetObjectWithBucket(key string) (io.ReadCloser, error) {
	return s.GetObject(s.bucket, key)
}

// PutObjectBytes uploads bytes using the configured bucket.
func (s *S3ObjectStore) PutObjectBytes(key string, data []byte) error {
	return s.PutObject(s.bucket, key, bytes.NewReader(data))
}

// GetObjectBytes downloads an object as bytes using the configured bucket.
func (s *S3ObjectStore) GetObjectBytes(key string) ([]byte, error) {
	reader, err := s.GetObject(s.bucket, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	return io.ReadAll(reader)
}

// isValidS3URLScheme checks if the URL has a valid scheme (http or https).
func isValidS3URLScheme(s3URL string) bool {
	u, err := url.Parse(s3URL)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

// buildTLSHTTPClient creates an *http.Client with custom TLS settings
// for S3-compatible endpoints with private CAs or disabled TLS verification.
func buildTLSHTTPClient(insecureSkipTLSVerify bool, caCert string) (*http.Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecureSkipTLSVerify, //nolint:gosec // User-configured via BSL for private endpoints
	}

	if caCert != "" {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			certPool = x509.NewCertPool()
		}
		if !certPool.AppendCertsFromPEM([]byte(caCert)) {
			return nil, fmt.Errorf("failed to parse CA certificate PEM")
		}
		tlsConfig.RootCAs = certPool
	}

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tlsConfig
	return &http.Client{Transport: tr}, nil

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
		if err := json.Unmarshal(data, &vmIndex); err != nil {
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
	return vmIndex, exists, nil
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
		if err := json.Unmarshal(data, &backupManifest); err != nil {
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
	return backupManifest, exists, nil
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
		if err := json.Unmarshal(data, &vmBackupManifest); err != nil {
			logger.Info("Failed to parse existing vm backup manifest", "reason", err.Error())
			exists = false
		}
	}
	return vmBackupManifest, exists, nil
}

// GetVMBackupManifestPath gets the path for the VMBackupManifest
func GetVMBackupManifestPath(backupName, ns, name string) string {
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
		return fmt.Errorf("failed to delete vmbt.json: %w", err)
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

// GetQCOWPath gets the path for the VMBackupTracker
func GetQCOWPath(ns, name, checkpoint, qcowName string) string {
	return fmt.Sprintf("checkpoints/%s/%s/%s/%s", ns, name, checkpoint, qcowName)
}

// DeleteVMBT deletes a qcow2 file from s3
func DeleteQCOW(store velero.ObjectStore, ns, name, checkpoint, qcowName, bucket string) error {
	qcowPath := GetQCOWPath(ns, name, checkpoint, qcowName)

	if err := store.DeleteObject(bucket, qcowPath); err != nil {
		return fmt.Errorf("failed to delete %s: %w", qcowName, err)
	}
	return nil

}

// writeCredentialsToTempFile writes credential data to a temporary file and returns
// the file path. The caller is responsible for removing the file when done.
// The file is created with restrictive permissions (0600) to protect credentials.
func writeCredentialsToTempFile(data []byte) (string, error) {
	tmpFile, err := os.CreateTemp("", "aws-credentials-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp credentials file: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to write temp credentials file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to close temp credentials file: %w", err)
	}

	return tmpFile.Name(), nil
}

// InitObjectStore creates an ObjectStore based on the provider type.
// Credentials and S3-compatible settings are passed through the config map
// to Init(), which handles temp file creation for in-memory credentials.
func InitObjectStore(cfg *UploaderConfig) (velero.ObjectStore, error) {
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

	// S3-compatible storage settings
	if cfg.BSLS3URL != "" {
		configMap["s3Url"] = cfg.BSLS3URL
	}
	if cfg.BSLS3ForcePathStyle {
		configMap["s3ForcePathStyle"] = "true"
	}
	if cfg.BSLInsecureSkipTLSVerify {
		configMap["insecureSkipTLSVerify"] = "true"
	}
	if cfg.BSLCACert != "" {
		configMap["caCert"] = cfg.BSLCACert
	}

	switch strings.ToLower(cfg.BSLProvider) {
	case "aws":
		return NewS3ObjectStore(configMap)
	case "gcp":
		// TODO: Implement GCP Cloud Storage support (issue #11)
		return nil, fmt.Errorf("gcp object store not yet implemented")
	case "azure":
		// TODO: Implement Azure Blob Storage support (issue #11)
		return nil, fmt.Errorf("azure object store not yet implemented")
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
	S3URL                 string
	S3ForcePathStyle      bool
	InsecureSkipTLSVerify bool
	CACert                string
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
	if bsl.Spec.Config != nil {
		region = bsl.Spec.Config["region"]
		s3URL = bsl.Spec.Config["s3Url"]
		s3ForcePathStyle = strings.EqualFold(bsl.Spec.Config["s3ForcePathStyle"], "true")
		insecureSkipTLSVerify = strings.EqualFold(bsl.Spec.Config["insecureSkipTLSVerify"], "true")
		caCert = bsl.Spec.Config["caCert"]
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
		Provider:              bsl.Spec.Provider,
		Bucket:                bucket,
		Prefix:                prefix,
		Region:                region,
		CredentialName:        credName,
		CredentialKey:         credKey,
		S3URL:                 s3URL,
		S3ForcePathStyle:      s3ForcePathStyle,
		InsecureSkipTLSVerify: insecureSkipTLSVerify,
		CACert:                caCert,
	}, nil
}

// InitObjectStoreFromBSL extracts BSL config, fetches credentials, and initializes
// an ObjectStore client.
func InitObjectStoreFromBSL(
	ctx context.Context,
	k8sClient client.Client,
	oadpNamespace string,
	bsl *velerov1.BackupStorageLocation,
	factory func(c *UploaderConfig) (velero.ObjectStore, error),
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

	store, err := factory(&UploaderConfig{
		BSLProvider:              cfg.Provider,
		BSLBucket:                cfg.Bucket,
		BSLPrefix:                cfg.Prefix,
		BSLRegion:                cfg.Region,
		BSLS3URL:                 cfg.S3URL,
		BSLS3ForcePathStyle:      cfg.S3ForcePathStyle,
		BSLInsecureSkipTLSVerify: cfg.InsecureSkipTLSVerify,
		BSLCACert:                cfg.CACert,
		CredentialsData:          credData,
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
