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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// Compile-time check that GCPObjectStore implements velero.ObjectStore
var _ velero.ObjectStore = (*GCPObjectStore)(nil)

// GCPObjectStore implements velero.ObjectStore for Google Cloud Storage.
type GCPObjectStore struct {
	client         *storage.Client
	bucket         string
	prefix         string
	kmsKeyName     string
	googleAccessID string
	privateKey     []byte
	iamSvc         *iamcredentials.Service
}

// NewGCPObjectStore creates a new GCPObjectStore from a config map.
// Supported keys: bucket, prefix, credentialsFile, credentialsData,
// serviceAccount, kmsKeyName.
func NewGCPObjectStore(configMap map[string]string) (*GCPObjectStore, error) {
	store := &GCPObjectStore{
		bucket: configMap["bucket"],
		prefix: configMap["prefix"],
	}

	if err := store.Init(configMap); err != nil {
		return nil, err
	}

	return store, nil
}

// credAccountType represents the type field in a GCP credentials JSON file.
type credAccountType string

const (
	serviceAccountType  credAccountType = "service_account"
	externalAccountType credAccountType = "external_account"
)

// getCredAccountType extracts the "type" field from a GCP credentials JSON.
func getCredAccountType(credJSON []byte) (credAccountType, error) {
	var f map[string]any
	if err := json.Unmarshal(credJSON, &f); err != nil {
		return "", fmt.Errorf("failed to parse credentials JSON: %w", err)
	}
	t, ok := f["type"].(string)
	if !ok {
		return "", fmt.Errorf("credentials JSON missing \"type\" field")
	}
	return credAccountType(t), nil
}

// Init initializes the ObjectStore with the provided config.
// This implements velero.ObjectStore.Init().
//
// Supported auth modes (matching Velero GCP plugin, excluding WIF/STS):
//   - SA JSON key file: credentialsFile or credentialsData config key
//   - Compute Engine / GKE: default credentials with serviceAccount config for signing
//   - External account (WIF): detected but not supported for signed URLs
func (g *GCPObjectStore) Init(configMap map[string]string) error {
	bucket := configMap["bucket"]
	prefix := configMap["prefix"]

	if bucket == "" {
		return fmt.Errorf("bucket is required in config")
	}

	g.bucket = bucket
	g.prefix = prefix
	g.kmsKeyName = configMap["kmsKeyName"]

	ctx := context.Background()

	clientOptions := []option.ClientOption{
		option.WithScopes(storage.ScopeReadWrite),
	}

	var creds *google.Credentials
	var err error

	if credFile := configMap["credentialsFile"]; credFile != "" {
		b, err := os.ReadFile(credFile)
		if err != nil {
			return fmt.Errorf("error reading credentials file %s: %w", credFile, err)
		}

		//nolint:staticcheck // credentials from trusted K8s secrets
		creds, err = google.CredentialsFromJSON(ctx, b)
		if err != nil {
			return fmt.Errorf("error parsing credentials file: %w", err)
		}

		//nolint:staticcheck // matching Velero GCP plugin pattern
		clientOptions = append(clientOptions, option.WithCredentialsFile(credFile))
	} else if credData := configMap["credentialsData"]; credData != "" {
		credBytes := []byte(credData)

		//nolint:staticcheck // credentials from trusted K8s secrets
		creds, err = google.CredentialsFromJSON(ctx, credBytes)
		if err != nil {
			return fmt.Errorf("error parsing credentials data: %w", err)
		}

		//nolint:staticcheck // matching Velero GCP plugin pattern
		clientOptions = append(clientOptions, option.WithCredentialsJSON(credBytes))
	} else {
		creds, err = google.FindDefaultCredentials(ctx, storage.ScopeReadWrite)
		if err != nil {
			return fmt.Errorf("error finding default credentials: %w", err)
		}
	}

	if creds.JSON != nil {
		credType, err := getCredAccountType(creds.JSON)
		if err != nil {
			return fmt.Errorf("error determining credential type: %w", err)
		}
		if credType == serviceAccountType {
			if err := g.initFromKeyFile(creds); err != nil {
				return fmt.Errorf("error initializing from service account key: %w", err)
			}
		}
		// external_account: no-op, signed URLs will return an error
	} else {
		if err := g.initFromComputeEngine(configMap); err != nil {
			return fmt.Errorf("error initializing from compute engine: %w", err)
		}
	}

	client, err := storage.NewClient(ctx, clientOptions...)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	g.client = client

	return nil
}

// initFromKeyFile extracts the google access ID and private key from a service
// account credentials JSON for signed URL generation.
func (g *GCPObjectStore) initFromKeyFile(creds *google.Credentials) error {
	jwtConfig, err := google.JWTConfigFromJSON(creds.JSON)
	if err != nil {
		return fmt.Errorf("error parsing service account key: %w", err)
	}
	if jwtConfig.Email == "" {
		return fmt.Errorf("service account key does not contain an email")
	}
	if len(jwtConfig.PrivateKey) == 0 {
		return fmt.Errorf("service account key does not contain a private key")
	}

	g.googleAccessID = jwtConfig.Email
	g.privateKey = jwtConfig.PrivateKey
	return nil
}

// initFromComputeEngine sets up IAM-based signing for compute engine / GKE
// environments where no service account key file is available.
func (g *GCPObjectStore) initFromComputeEngine(config map[string]string) error {
	sa, ok := config["serviceAccount"]
	if !ok || sa == "" {
		return fmt.Errorf("serviceAccount is required in BackupStorageLocation config for compute engine credentials")
	}
	g.googleAccessID = sa

	svc, err := iamcredentials.NewService(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create IAM credentials service: %w", err)
	}
	g.iamSvc = svc
	return nil
}

// SignBytes signs bytes using the IAM signBlob API for compute engine credentials.
func (g *GCPObjectStore) SignBytes(payload []byte) ([]byte, error) {
	name := "projects/-/serviceAccounts/" + g.googleAccessID
	resp, err := g.iamSvc.Projects.ServiceAccounts.SignBlob(name, &iamcredentials.SignBlobRequest{
		Payload: base64.StdEncoding.EncodeToString(payload),
	}).Context(context.Background()).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to sign bytes via IAM: %w", err)
	}
	return base64.StdEncoding.DecodeString(resp.SignedBlob)
}

// fullKey returns the full object key including the prefix.
func (g *GCPObjectStore) fullKey(key string) string {
	if g.prefix == "" {
		return key
	}
	return strings.TrimSuffix(g.prefix, "/") + "/" + strings.TrimPrefix(key, "/")
}

// PutObject uploads an object to GCS.
// Implements velero.ObjectStore.PutObject().
func (g *GCPObjectStore) PutObject(bucket, key string, body io.Reader) error {
	w := g.client.Bucket(bucket).Object(g.fullKey(key)).NewWriter(context.Background())
	w.KMSKeyName = g.kmsKeyName

	// The writer returned by NewWriter is asynchronous, so errors aren't guaranteed
	// until Close() is called
	_, copyErr := io.Copy(w, body)

	closeErr := w.Close()
	if copyErr != nil {
		return fmt.Errorf("failed to upload object %s: %w", key, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to upload object %s: %w", key, closeErr)
	}
	return nil
}

// GetObject retrieves an object from GCS.
// Implements velero.ObjectStore.GetObject().
func (g *GCPObjectStore) GetObject(bucket, key string) (io.ReadCloser, error) {
	r, err := g.client.Bucket(bucket).Object(g.fullKey(key)).NewReader(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}
	return r, nil
}

// ObjectExists checks if an object exists in GCS.
// Implements velero.ObjectStore.ObjectExists().
func (g *GCPObjectStore) ObjectExists(bucket, key string) (bool, error) {
	_, err := g.client.Bucket(bucket).Object(g.fullKey(key)).Attrs(context.Background())
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object existence %s: %w", key, err)
	}
	return true, nil
}

// DeleteObject removes an object from GCS.
// Implements velero.ObjectStore.DeleteObject().
func (g *GCPObjectStore) DeleteObject(bucket, key string) error {
	err := g.client.Bucket(bucket).Object(g.fullKey(key)).Delete(context.Background())
	if err != nil {
		return fmt.Errorf("failed to delete object %s: %w", key, err)
	}
	return nil
}

// ListCommonPrefixes gets a list of all object key prefixes that start with
// the specified prefix and stop at the next instance of the provided delimiter.
// Implements velero.ObjectStore.ListCommonPrefixes().
func (g *GCPObjectStore) ListCommonPrefixes(bucket, prefix, delimiter string) ([]string, error) {
	fullPrefix := g.fullKey(prefix)

	q := &storage.Query{
		Prefix:    fullPrefix,
		Delimiter: delimiter,
	}

	iter := g.client.Bucket(bucket).Objects(context.Background(), q)

	var prefixes []string
	for {
		obj, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list common prefixes: %w", err)
		}
		if obj.Prefix != "" {
			prefixes = append(prefixes, obj.Prefix)
		}
	}

	return prefixes, nil
}

// ListObjects gets a list of all keys in the specified bucket that have the given prefix.
// Implements velero.ObjectStore.ListObjects().
func (g *GCPObjectStore) ListObjects(bucket, prefix string) ([]string, error) {
	fullPrefix := g.fullKey(prefix)

	q := &storage.Query{
		Prefix: fullPrefix,
	}

	var keys []string
	iter := g.client.Bucket(bucket).Objects(context.Background(), q)

	for {
		obj, err := iter.Next()
		if err == iterator.Done {
			return keys, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		keys = append(keys, obj.Name)
	}
}

// CreateSignedURL creates a pre-signed URL for the given bucket and key that expires after ttl.
// Implements velero.ObjectStore.CreateSignedURL().
//
// Requires either a service account key (for local signing) or compute engine
// credentials with serviceAccount config (for IAM-based signing).
// Returns an error for external_account (WIF) credentials.
func (g *GCPObjectStore) CreateSignedURL(bucket, key string, ttl time.Duration) (string, error) {
	if g.googleAccessID == "" {
		return "", fmt.Errorf(
			"GoogleAccessID is empty, cannot create signed URL (external_account credentials are not supported)",
		)
	}

	options := storage.SignedURLOptions{
		GoogleAccessID: g.googleAccessID,
		Method:         "GET",
		Expires:        time.Now().Add(ttl),
	}

	if g.privateKey == nil {
		options.SignBytes = g.SignBytes
	} else {
		options.PrivateKey = g.privateKey
	}

	url, err := storage.SignedURL(bucket, g.fullKey(key), &options)
	if err != nil {
		return "", fmt.Errorf("failed to create signed URL: %w", err)
	}
	return url, nil
}

// Convenience methods for our uploader use case

// PutObjectWithBucket uploads an object using the configured bucket.
func (g *GCPObjectStore) PutObjectWithBucket(key string, body io.Reader) error {
	return g.PutObject(g.bucket, key, body)
}

// GetObjectWithBucket retrieves an object using the configured bucket.
func (g *GCPObjectStore) GetObjectWithBucket(key string) (io.ReadCloser, error) {
	return g.GetObject(g.bucket, key)
}

// PutObjectBytes uploads bytes using the configured bucket.
func (g *GCPObjectStore) PutObjectBytes(key string, data []byte) error {
	return g.PutObject(g.bucket, key, bytes.NewReader(data))
}

// GetObjectBytes downloads an object as bytes using the configured bucket.
func (g *GCPObjectStore) GetObjectBytes(key string) ([]byte, error) {
	reader, err := g.GetObject(g.bucket, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	return io.ReadAll(reader)
}
