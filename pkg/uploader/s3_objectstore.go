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
	"crypto/md5" //nolint:gosec // MD5 required by AWS SSE-C protocol
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// Compile-time check that S3ObjectStore implements velero.ObjectStore
var _ velero.ObjectStore = (*S3ObjectStore)(nil)

// S3ObjectStore implements velero.ObjectStore for AWS S3 and S3-compatible storage.
type S3ObjectStore struct {
	client   *s3.Client
	uploader *transfermanager.Client
	bucket   string
	prefix   string

	serverSideEncryption string
	kmsKeyId             string
	checksumAlgorithm    string
	sseCustomerAlgorithm string
	sseCustomerKey       string
	sseCustomerKeyMD5    string
}

// NewS3ObjectStore creates a new S3ObjectStore from a config map.
// Supported keys: bucket, prefix, region, credentialsFile, credentialsData,
// profile, s3Url, s3ForcePathStyle, insecureSkipTLSVerify, caCert,
// serverSideEncryption, kmsKeyId, checksumAlgorithm, customerKeyEncryptionFile.
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
// profile, s3Url, s3ForcePathStyle, insecureSkipTLSVerify, caCert,
// serverSideEncryption, kmsKeyId, checksumAlgorithm, customerKeyEncryptionFile.
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

	// Select a named profile from the credentials/config file, matching
	// Velero's "profile" BSL config key. Without this, a credentials file
	// containing a non-default profile section is parsed but ignored, and
	// the SDK silently falls back to the default credential chain (e.g.
	// EC2 IMDS), which fails outside of EC2.
	if profile := configMap["profile"]; profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}

	// Add retry configuration for transient errors
	opts = append(opts, config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 3
		})
	}))

	// Configure custom TLS for S3-compatible endpoints with private CAs
	// or when TLS verification needs to be skipped.
	insecureSkipTLSVerify := common.ParseBool(configMap["insecureSkipTLSVerify"])
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
	forcePathStyle := common.ParseBool(configMap["s3ForcePathStyle"])

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
	s.uploader = transfermanager.New(s.client, func(o *transfermanager.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})

	// Configure server-side encryption
	s.serverSideEncryption = configMap["serverSideEncryption"]
	s.kmsKeyId = configMap["kmsKeyId"]

	// Configure checksum algorithm
	if algo := configMap["checksumAlgorithm"]; algo != "" {
		upper := strings.ToUpper(algo)
		switch upper {
		case "CRC32", "CRC32C", "SHA1", "SHA256":
			s.checksumAlgorithm = upper
		default:
			return fmt.Errorf("unsupported checksumAlgorithm %q: must be CRC32, CRC32C, SHA1, or SHA256", algo)
		}
	}

	// Configure SSE-C (customer-provided encryption key)
	if customerKeyFile := configMap["customerKeyEncryptionFile"]; customerKeyFile != "" {
		if s.kmsKeyId != "" {
			return fmt.Errorf("customerKeyEncryptionFile cannot be used with kmsKeyId")
		}
		if s.serverSideEncryption != "" {
			return fmt.Errorf("customerKeyEncryptionFile cannot be used with serverSideEncryption")
		}
		sseKey, err := loadSSECKey(customerKeyFile)
		if err != nil {
			return fmt.Errorf("failed to load SSE-C key: %w", err)
		}
		s.sseCustomerAlgorithm = "AES256"
		s.sseCustomerKey = base64.StdEncoding.EncodeToString(sseKey)
		hash := md5.Sum(sseKey) //nolint:gosec // MD5 required by AWS SSE-C protocol
		s.sseCustomerKeyMD5 = base64.StdEncoding.EncodeToString(hash[:])
	}

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
	input := &transfermanager.UploadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
		Body:   body,
	}
	if s.serverSideEncryption != "" {
		input.ServerSideEncryption = tmtypes.ServerSideEncryption(s.serverSideEncryption)
	}
	if s.kmsKeyId != "" {
		input.SSEKMSKeyID = aws.String(s.kmsKeyId)
	}
	if s.checksumAlgorithm != "" {
		input.ChecksumAlgorithm = tmtypes.ChecksumAlgorithm(s.checksumAlgorithm)
	}
	if s.sseCustomerAlgorithm != "" {
		input.SSECustomerAlgorithm = aws.String(s.sseCustomerAlgorithm)
		input.SSECustomerKey = aws.String(s.sseCustomerKey)
		input.SSECustomerKeyMD5 = aws.String(s.sseCustomerKeyMD5)
	}
	_, err := s.uploader.UploadObject(context.Background(), input)
	if err != nil {
		return fmt.Errorf("failed to upload object %s: %w", key, err)
	}
	return nil
}

// GetObject retrieves an object from S3.
// Implements velero.ObjectStore.GetObject().
func (s *S3ObjectStore) GetObject(bucket, key string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	}
	if s.sseCustomerAlgorithm != "" {
		input.SSECustomerAlgorithm = aws.String(s.sseCustomerAlgorithm)
		input.SSECustomerKey = aws.String(s.sseCustomerKey)
		input.SSECustomerKeyMD5 = aws.String(s.sseCustomerKeyMD5)
	}
	output, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}
	return output.Body, nil
}

// ObjectExists checks if an object exists in S3.
// Implements velero.ObjectStore.ObjectExists().
func (s *S3ObjectStore) ObjectExists(bucket, key string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s.fullKey(key)),
	}
	if s.sseCustomerAlgorithm != "" {
		input.SSECustomerAlgorithm = aws.String(s.sseCustomerAlgorithm)
		input.SSECustomerKey = aws.String(s.sseCustomerKey)
		input.SSECustomerKeyMD5 = aws.String(s.sseCustomerKeyMD5)
	}
	_, err := s.client.HeadObject(context.Background(), input)
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
}

// loadSSECKey reads a 32-byte SSE-C encryption key from the given file path.
func loadSSECKey(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read SSE-C key file %q: %w", path, err)
	}
	if len(data) != 32 {
		data = bytes.TrimRight(data, "\r\n")
	}
	if len(data) != 32 {
		return nil, fmt.Errorf("SSE-C key must be exactly 32 bytes, got %d", len(data))
	}
	return data, nil
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
