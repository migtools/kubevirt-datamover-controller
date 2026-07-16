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
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

const (
	// defaultBlockSize is 1MB, matching the Velero Azure plugin
	defaultBlockSize = 1 * 1024 * 1024
)

// Compile-time check that AzureObjectStore implements velero.ObjectStore
var _ velero.ObjectStore = (*AzureObjectStore)(nil)

// AzureObjectStore implements velero.ObjectStore for Microsoft Azure Blob Storage.
type AzureObjectStore struct {
	client    *azblob.Client
	sharedKey *azblob.SharedKeyCredential
	bucket    string
	prefix    string
	blockSize int
}

// NewAzureObjectStore creates a new AzureObjectStore from a config map.
func NewAzureObjectStore(configMap map[string]string) (*AzureObjectStore, error) {
	store := &AzureObjectStore{
		bucket:    configMap["bucket"],
		prefix:    configMap["prefix"],
		blockSize: defaultBlockSize,
	}

	if err := store.Init(configMap); err != nil {
		return nil, err
	}

	return store, nil
}

// Init initializes the ObjectStore with the provided config.
// Expected config keys: bucket, prefix, storageAccount, storageAccountKey (or via credentialsData).
func (a *AzureObjectStore) Init(configMap map[string]string) error {
	bucket := configMap["bucket"]
	prefix := configMap["prefix"]

	if bucket == "" {
		return fmt.Errorf("bucket is required in config")
	}

	a.bucket = bucket
	a.prefix = prefix

	// In a real implementation, you would parse the credentialsData or environment
	// variables to get the storage account name and key, similar to how Velero does it.
	// For this implementation, we'll extract them from the config map directly.
	accountName := configMap["storageAccount"]
	accountKey := configMap["storageAccountKey"]

	// Fallback to parsing credentialsData if provided (e.g., from BSL secret)
	if credData := configMap["credentialsData"]; credData != "" && accountName == "" {
		accountName, accountKey = parseAzureCredentials([]byte(credData))
	}

	if accountName == "" {
		accountName = os.Getenv("AZURE_STORAGE_ACCOUNT")
	}
	if accountKey == "" {
		accountKey = os.Getenv("AZURE_STORAGE_KEY")
	}

	if accountName == "" || accountKey == "" {
		return fmt.Errorf("storageAccount and storageAccountKey are required")
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return fmt.Errorf("failed to create Azure shared key credential: %w", err)
	}
	a.sharedKey = cred

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure blob client: %w", err)
	}
	a.client = client

	return nil
}

// fullKey returns the full object key including the prefix.
func (a *AzureObjectStore) fullKey(key string) string {
	if a.prefix == "" {
		return key
	}
	return strings.TrimSuffix(a.prefix, "/") + "/" + strings.TrimPrefix(key, "/")
}

// PutObject uploads an object to Azure Blob Storage using chunked upload.
func (a *AzureObjectStore) PutObject(bucket, key string, body io.Reader) error {
	fullKey := a.fullKey(key)
	blobClient := a.client.ServiceClient().NewContainerClient(bucket).NewBlockBlobClient(fullKey)

	var (
		block    = make([]byte, a.blockSize)
		blockIDs []string
	)

	for {
		n, err := body.Read(block)
		if n > 0 {
			// blockID needs to be the same length for all blocks, so use a fixed width.
			blockID := fmt.Sprintf("%08d", len(blockIDs))

			br := bytes.NewReader(block[0:n])

			// Wrap *bytes.Reader into an io.ReadSeekCloser
			var rsc io.ReadSeekCloser = struct {
				*bytes.Reader
				io.Closer
			}{
				Reader: br,
				Closer: io.NopCloser(nil),
			}

			_, putErr := blobClient.StageBlock(context.Background(), blockID, rsc, nil)
			if putErr != nil {
				return fmt.Errorf("error putting block %s: %w", blockID, putErr)
			}
			blockIDs = append(blockIDs, blockID)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading block from body: %w", err)
		}
	}

	_, err := blobClient.CommitBlockList(context.Background(), blockIDs, nil)
	if err != nil {
		return fmt.Errorf("error committing block list: %w", err)
	}

	return nil
}

// GetObject retrieves an object from Azure Blob Storage.
func (a *AzureObjectStore) GetObject(bucket, key string) (io.ReadCloser, error) {
	fullKey := a.fullKey(key)

	res, err := a.client.DownloadStream(context.Background(), bucket, fullKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}

	return res.Body, nil
}

// ObjectExists checks if an object exists in Azure Blob Storage.
func (a *AzureObjectStore) ObjectExists(bucket, key string) (bool, error) {
	fullKey := a.fullKey(key)
	blobClient := a.client.ServiceClient().NewContainerClient(bucket).NewBlockBlobClient(fullKey)

	_, err := blobClient.GetProperties(context.Background(), nil)
	if err == nil {
		return true, nil
	}

	if bloberror.HasCode(err, bloberror.ContainerNotFound, bloberror.BlobNotFound) {
		return false, nil
	}

	return false, fmt.Errorf("failed to check object existence %s: %w", key, err)
}

// DeleteObject removes an object from Azure Blob Storage.
func (a *AzureObjectStore) DeleteObject(bucket, key string) error {
	fullKey := a.fullKey(key)

	_, err := a.client.DeleteBlob(context.Background(), bucket, fullKey, nil)
	if err != nil {
		return fmt.Errorf("failed to delete object %s: %w", key, err)
	}

	return nil
}

// ListCommonPrefixes gets a list of all object key prefixes.
func (a *AzureObjectStore) ListCommonPrefixes(bucket, prefix, delimiter string) ([]string, error) {
	fullPrefix := a.fullKey(prefix)
	var prefixes []string

	pager := a.client.ServiceClient().NewContainerClient(bucket).NewListBlobsHierarchyPager(delimiter, &container.ListBlobsHierarchyOptions{
		Prefix: &fullPrefix,
	})

	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list common prefixes: %w", err)
		}
		for _, p := range page.Segment.BlobPrefixes {
			if p.Name != nil {
				prefixes = append(prefixes, *p.Name)
			}
		}
	}

	return prefixes, nil
}

// ListObjects gets a list of all keys in the specified bucket that have the given prefix.
func (a *AzureObjectStore) ListObjects(bucket, prefix string) ([]string, error) {
	fullPrefix := a.fullKey(prefix)
	var keys []string

	pager := a.client.NewListBlobsFlatPager(bucket, &azblob.ListBlobsFlatOptions{
		Prefix: &fullPrefix,
	})

	for pager.More() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		for _, blob := range page.Segment.BlobItems {
			if blob.Name != nil {
				keys = append(keys, *blob.Name)
			}
		}
	}

	return keys, nil
}

// CreateSignedURL creates a pre-signed URL for the given bucket and key that expires after ttl.
func (a *AzureObjectStore) CreateSignedURL(bucket, key string, ttl time.Duration) (string, error) {
	if a.sharedKey == nil {
		return "", errors.New("shared key credential is required to generate SAS URLs")
	}

	fullKey := a.fullKey(key)

	sasQueryParams, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     time.Now().UTC().Add(-10 * time.Minute),
		ExpiryTime:    time.Now().UTC().Add(ttl),
		Permissions:   to.Ptr(sas.BlobPermissions{Read: true}).String(),
		ContainerName: bucket,
		BlobName:      fullKey,
	}.SignWithSharedKey(a.sharedKey)

	if err != nil {
		return "", fmt.Errorf("failed to sign SAS URL: %w", err)
	}

	blobClient := a.client.ServiceClient().NewContainerClient(bucket).NewBlockBlobClient(fullKey)
	return fmt.Sprintf("%s?%s", blobClient.URL(), sasQueryParams.Encode()), nil
}

// Convenience methods for our uploader use case

// PutObjectWithBucket uploads an object using the configured bucket.
func (a *AzureObjectStore) PutObjectWithBucket(key string, body io.Reader) error {
	return a.PutObject(a.bucket, key, body)
}

// GetObjectWithBucket retrieves an object using the configured bucket.
func (a *AzureObjectStore) GetObjectWithBucket(key string) (io.ReadCloser, error) {
	return a.GetObject(a.bucket, key)
}

// PutObjectBytes uploads bytes using the configured bucket.
func (a *AzureObjectStore) PutObjectBytes(key string, data []byte) error {
	return a.PutObject(a.bucket, key, bytes.NewReader(data))
}

// GetObjectBytes downloads an object as bytes using the configured bucket.
func (a *AzureObjectStore) GetObjectBytes(key string) ([]byte, error) {
	reader, err := a.GetObject(a.bucket, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	return io.ReadAll(reader)
}

// parseAzureCredentials is a simple helper to extract AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY
// from a credentials file format (e.g., INI or env file format).
func parseAzureCredentials(data []byte) (string, string) {
	var account, key string
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "AZURE_STORAGE_ACCOUNT=") {
			account = strings.TrimPrefix(line, "AZURE_STORAGE_ACCOUNT=")
		} else if strings.HasPrefix(line, "AZURE_STORAGE_KEY=") {
			key = strings.TrimPrefix(line, "AZURE_STORAGE_KEY=")
		}
	}
	return account, key
}
