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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
)

const (
	testSSEAES256     = "AES256"
	testChecksumCRC32 = "CRC32"
)

// fullKeyTestCases is shared across S3, GCP, and Azure fullKey tests
// since the logic is identical for all providers.
var fullKeyTestCases = []struct {
	name     string
	prefix   string
	key      string
	expected string
}{
	{
		name:     "no prefix",
		prefix:   "",
		key:      "some/path/file.json",
		expected: "some/path/file.json",
	},
	{
		name:     "with prefix no trailing slash",
		prefix:   "backups",
		key:      "checkpoints/ns/vm/index.json",
		expected: "backups/checkpoints/ns/vm/index.json",
	},
	{
		name:     "with prefix with trailing slash",
		prefix:   "backups/",
		key:      "checkpoints/ns/vm/index.json",
		expected: "backups/checkpoints/ns/vm/index.json",
	},
	{
		name:     "key with leading slash",
		prefix:   "backups",
		key:      "/checkpoints/ns/vm/index.json",
		expected: "backups/checkpoints/ns/vm/index.json",
	},
	{
		name:     "both with slashes",
		prefix:   "backups/",
		key:      "/checkpoints/ns/vm/index.json",
		expected: "backups/checkpoints/ns/vm/index.json",
	},
	{
		name:     "nested prefix",
		prefix:   "velero/backups",
		key:      "file.json",
		expected: "velero/backups/file.json",
	},
}

func TestS3ObjectStoreFullKey(t *testing.T) {
	for _, tt := range fullKeyTestCases {
		t.Run(tt.name, func(t *testing.T) {
			store := &S3ObjectStore{prefix: tt.prefix}
			result := store.fullKey(tt.key)
			if result != tt.expected {
				t.Errorf("fullKey(%q) = %q, want %q", tt.key, result, tt.expected)
			}
		})
	}
}

func TestWriteCredentialsToTempFile(t *testing.T) {
	content := "[default]\naws_access_key_id = AKID\naws_secret_access_key = SECRET\n"

	path, err := writeCredentialsToTempFile([]byte(content))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = os.Remove(path) }()

	// Verify file exists and contains expected content
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read temp file: %v", err)
	}
	if string(data) != content {
		t.Errorf("temp file content = %q, want %q", string(data), content)
	}

	// Verify file has restrictive permissions
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat temp file: %v", err)
	}
	perm := info.Mode().Perm()
	if perm&0o077 != 0 {
		t.Errorf("temp file permissions = %o, want no group/other access", perm)
	}
}

func TestInitWithSDKCredentials(t *testing.T) {
	tests := []struct {
		name        string
		credContent string
		expectError bool
	}{
		{
			name: "standard unquoted credentials",
			credContent: "[default]\n" +
				"aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n" +
				"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n",
			expectError: false,
		},
		{
			name: "quoted credentials (the bug this fixes)",
			credContent: "[default]\n" +
				"aws_access_key_id = \"AKIAIOSFODNN7EXAMPLE\"\n" +
				"aws_secret_access_key = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"\n",
			expectError: false,
		},
		{
			name: "credentials with named profile",
			credContent: "[profile backup]\n" +
				"aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n" +
				"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n",
			// SDK loads default profile; a non-default profile without AWS_PROFILE
			// set means no credentials are found, but Init() still succeeds —
			// the error would come at request time, not at config load time.
			expectError: false,
		},
		{
			name: "credentials with session token",
			credContent: "[default]\n" +
				"aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n" +
				"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n" +
				"aws_session_token = FwoGZXIvYXdzEBYaDHqa0AP\n",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Write credentials to a temp file
			tmpDir := t.TempDir()
			credFile := filepath.Join(tmpDir, "credentials")
			if err := os.WriteFile(credFile, []byte(tt.credContent), 0600); err != nil {
				t.Fatalf("failed to write credentials file: %v", err)
			}

			store := &S3ObjectStore{}
			err := store.Init(map[string]string{
				"bucket":          "test-bucket",
				"region":          "us-east-1",
				"credentialsFile": credFile,
			})

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestInitWithNamedProfile(t *testing.T) {
	// Isolate from ambient AWS credentials so this test verifies the named
	// profile is actually selected, not whatever the environment happens
	// to provide (env credentials outrank shared-config profile in the
	// SDK's default chain). Also disable IMDS so a misconfiguration falls
	// back to a fast local error instead of a slow network timeout.
	for _, name := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY",
		"AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY",
		"AWS_SESSION_TOKEN", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_PROFILE", "AWS_DEFAULT_PROFILE",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	// Reproduces issue #179: a BSL credentials secret using a non-default
	// profile name was parsed but ignored, so the SDK silently fell back
	// to the default credential chain (e.g. EC2 IMDS) instead of the
	// configured profile's static credentials.
	credData := "[minio]\n" +
		"aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n" +
		"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n"

	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"region":          "us-east-1",
		"credentialsData": credData,
		"profile":         "minio",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	creds, err := store.client.Options().Credentials.Retrieve(t.Context())
	if err != nil {
		t.Fatalf("failed to retrieve credentials from named profile: %v", err)
	}
	if creds.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
		t.Errorf("AccessKeyID = %q, want %q", creds.AccessKeyID, "AKIAIOSFODNN7EXAMPLE")
	}
}

func TestInitWithCredentialsData(t *testing.T) {
	// Test the in-memory credentials path (controller uses this)
	store := &S3ObjectStore{}
	credData := "[default]\naws_access_key_id = AKIAIOSFODNN7EXAMPLE\n" +
		"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n"

	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"region":          "us-east-1",
		"credentialsData": credData,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInitWithMissingCredentialsFile(t *testing.T) {
	// The AWS SDK does not error when a credentials file is missing — it
	// silently falls back to the default credential chain. Init() should
	// succeed; authentication errors surface at request time.
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"region":          "us-east-1",
		"credentialsFile": "/nonexistent/path/credentials",
	})
	if err != nil {
		t.Errorf("unexpected error: Init should succeed with missing cred file "+
			"(SDK falls back to default chain): %v", err)
	}
}

func TestS3ObjectStoreInit(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]string
		expectError bool
		errorMsg    string
	}{
		{
			name: "missing bucket",
			config: map[string]string{
				"prefix": "backups",
				"region": "us-east-1",
			},
			expectError: true,
			errorMsg:    "bucket is required",
		},
		{
			name: "empty bucket",
			config: map[string]string{
				"bucket": "",
				"prefix": "backups",
				"region": "us-east-1",
			},
			expectError: true,
			errorMsg:    "bucket is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &S3ObjectStore{}
			err := store.Init(tt.config)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestInitObjectStoreWithCredentialsData(t *testing.T) {
	tests := []struct {
		name        string
		credData    string
		expectError bool
	}{
		{
			name: "standard unquoted credentials",
			credData: "[default]\n" +
				"aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n" +
				"aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n",
			expectError: false,
		},
		{
			name: "quoted credentials (the bug this fixes)",
			credData: "[default]\n" +
				"aws_access_key_id = \"AKIAIOSFODNN7EXAMPLE\"\n" +
				"aws_secret_access_key = \"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\"\n",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &UploaderConfig{
				ObjectStoreConfig: common.ObjectStoreConfig{
					BSLProvider:     "aws",
					BSLBucket:       "test-bucket",
					BSLRegion:       "us-east-1",
					CredentialsData: []byte(tt.credData),
				},
			}

			store, err := InitObjectStore(&cfg.ObjectStoreConfig)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if store == nil {
					t.Error("expected non-nil store")
				}
			}
		})
	}
}

func TestInitObjectStoreAzureNotImplemented(t *testing.T) {
	config := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLProvider: "azure",
			BSLBucket:   "test-bucket",
		},
	}

	_, err := InitObjectStore(&config.ObjectStoreConfig)
	if err == nil {
		t.Error("expected error for azure provider, got none")
	}
}

func TestIsValidS3URLScheme(t *testing.T) {
	tests := []struct {
		url  string
		want bool
	}{
		{"https://minio.example.com", true},
		{"http://minio.example.com", true},
		{"https://minio.example.com:9000", true},
		{"http://10.0.0.1:9000", true},
		{"ftp://minio.example.com", false},
		{"s3://my-bucket", false},
		{"", false},
		{"not-a-url", false},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			if got := isValidS3URLScheme(tt.url); got != tt.want {
				t.Errorf("isValidS3URLScheme(%q) = %v, want %v", tt.url, got, tt.want)
			}
		})
	}
}

func TestBuildTLSHTTPClient(t *testing.T) {
	t.Run("insecure skip verify only", func(t *testing.T) {
		client, err := buildTLSHTTPClient(true, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("expected non-nil client")
		}
	})

	t.Run("valid CA cert", func(t *testing.T) {
		caCert := generateTestCACertPEM(t)
		client, err := buildTLSHTTPClient(false, caCert)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("expected non-nil client")
		}
	})

	t.Run("invalid CA cert PEM", func(t *testing.T) {
		_, err := buildTLSHTTPClient(false, "not-a-valid-pem")
		if err == nil {
			t.Error("expected error for invalid PEM, got none")
		}
	})

	t.Run("empty CA cert with no insecure", func(t *testing.T) {
		client, err := buildTLSHTTPClient(false, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("expected non-nil client")
		}
	})
}

func TestInitWithS3URL(t *testing.T) {
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket": "test-bucket",
		"region": "us-east-1",
		"s3Url":  "https://minio.example.com",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInitWithInvalidS3URLScheme(t *testing.T) {
	tests := []struct {
		name  string
		s3URL string
	}{
		{"ftp scheme", "ftp://minio.example.com"},
		{"s3 scheme", "s3://my-bucket"},
		{"no scheme", "minio.example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &S3ObjectStore{}
			err := store.Init(map[string]string{
				"bucket": "test-bucket",
				"s3Url":  tt.s3URL,
			})
			if err == nil {
				t.Error("expected error for invalid s3Url scheme, got none")
			}
		})
	}
}

func TestInitWithForcePathStyle(t *testing.T) {
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":           "test-bucket",
		"region":           "us-east-1",
		"s3Url":            "https://minio.example.com",
		"s3ForcePathStyle": "true",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInitWithCACert(t *testing.T) {
	caCert := generateTestCACertPEM(t)
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket": "test-bucket",
		"region": "us-east-1",
		"caCert": caCert,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInitWithInvalidCACert(t *testing.T) {
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket": "test-bucket",
		"region": "us-east-1",
		"caCert": "not-a-valid-pem",
	})
	if err == nil {
		t.Error("expected error for invalid CA cert, got none")
	}
}

func TestInitWithAllS3CompatibleOptions(t *testing.T) {
	caCert := generateTestCACertPEM(t)
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                "test-bucket",
		"region":                "us-east-1",
		"s3Url":                 "https://minio.example.com:9000",
		"s3ForcePathStyle":      "true",
		"insecureSkipTLSVerify": "false",
		"caCert":                caCert,
		"serverSideEncryption":  testSSEAES256,
		"checksumAlgorithm":     testChecksumCRC32,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if store.serverSideEncryption != testSSEAES256 {
		t.Errorf("serverSideEncryption = %q, want AES256", store.serverSideEncryption)
	}
	if store.checksumAlgorithm != testChecksumCRC32 {
		t.Errorf("checksumAlgorithm = %q, want CRC32", store.checksumAlgorithm)
	}
}

func TestInitWithInsecureSkipTLSVerify(t *testing.T) {
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                "test-bucket",
		"region":                "us-east-1",
		"insecureSkipTLSVerify": "true",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestInitObjectStoreWithS3Settings(t *testing.T) {
	caCert := generateTestCACertPEM(t)

	cfg := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLProvider:              "aws",
			BSLBucket:                "test-bucket",
			BSLRegion:                "us-east-1",
			BSLS3URL:                 "https://minio.example.com:9000",
			BSLS3ForcePathStyle:      true,
			BSLInsecureSkipTLSVerify: false,
			BSLCACert:                caCert,
		},
	}

	store, err := InitObjectStore(&cfg.ObjectStoreConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestInitObjectStoreWithS3SettingsDefaultProvider(t *testing.T) {
	// Unknown providers should fall through to S3-compatible
	cfg := &UploaderConfig{
		ObjectStoreConfig: common.ObjectStoreConfig{
			BSLProvider:         "minio",
			BSLBucket:           "test-bucket",
			BSLRegion:           "us-east-1",
			BSLS3URL:            "https://minio.example.com",
			BSLS3ForcePathStyle: true,
		},
	}

	store, err := InitObjectStore(&cfg.ObjectStoreConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store for unknown provider (S3-compatible fallback)")
	}
}

func TestInitWithServerSideEncryption(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]string
		wantSSE string
		wantKMS string
	}{
		{
			name: testSSEAES256,
			config: map[string]string{
				"bucket":               "test-bucket",
				"serverSideEncryption": testSSEAES256,
			},
			wantSSE: testSSEAES256,
		},
		{
			name: "aws:kms with kmsKeyId",
			config: map[string]string{
				"bucket":               "test-bucket",
				"serverSideEncryption": "aws:kms",
				"kmsKeyId":             "arn:aws:kms:us-east-1:123456789:key/test-key-id",
			},
			wantSSE: "aws:kms",
			wantKMS: "arn:aws:kms:us-east-1:123456789:key/test-key-id",
		},
		{
			name: "aws:kms:dsse",
			config: map[string]string{
				"bucket":               "test-bucket",
				"serverSideEncryption": "aws:kms:dsse",
				"kmsKeyId":             "arn:aws:kms:us-east-1:123456789:key/test-key-id",
			},
			wantSSE: "aws:kms:dsse",
			wantKMS: "arn:aws:kms:us-east-1:123456789:key/test-key-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &S3ObjectStore{}
			err := store.Init(tt.config)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if store.serverSideEncryption != tt.wantSSE {
				t.Errorf("serverSideEncryption = %q, want %q", store.serverSideEncryption, tt.wantSSE)
			}
			if store.kmsKeyId != tt.wantKMS {
				t.Errorf("kmsKeyId = %q, want %q", store.kmsKeyId, tt.wantKMS)
			}
		})
	}
}

func TestInitWithChecksumAlgorithm(t *testing.T) {
	validAlgorithms := []string{testChecksumCRC32, "CRC32C", "SHA1", "SHA256", "crc32", "sha256"}
	for _, algo := range validAlgorithms {
		t.Run("valid_"+algo, func(t *testing.T) {
			store := &S3ObjectStore{}
			err := store.Init(map[string]string{
				"bucket":            "test-bucket",
				"checksumAlgorithm": algo,
			})
			if err != nil {
				t.Fatalf("unexpected error for algorithm %q: %v", algo, err)
			}
			if store.checksumAlgorithm == "" {
				t.Error("checksumAlgorithm should be set")
			}
		})
	}

	t.Run("invalid algorithm", func(t *testing.T) {
		store := &S3ObjectStore{}
		err := store.Init(map[string]string{
			"bucket":            "test-bucket",
			"checksumAlgorithm": "MD5",
		})
		if err == nil {
			t.Fatal("expected error for unsupported algorithm")
		}
	})
}

func writeSSECKeyFile(t *testing.T, data []byte) string {
	t.Helper()
	tmpFile, err := os.CreateTemp("", "sse-c-key-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(tmpFile.Name()) })
	if _, err := tmpFile.Write(data); err != nil {
		t.Fatalf("failed to write key: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("failed to close temp file: %v", err)
	}
	return tmpFile.Name()
}

func TestInitWithSSECKeyFile(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	keyFile := writeSSECKeyFile(t, key)

	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionFile": keyFile,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.sseCustomerAlgorithm != testSSEAES256 {
		t.Errorf("sseCustomerAlgorithm = %q, want AES256", store.sseCustomerAlgorithm)
	}
	if store.sseCustomerKey == "" {
		t.Error("sseCustomerKey should be set")
	}
	if store.sseCustomerKeyMD5 == "" {
		t.Error("sseCustomerKeyMD5 should be set")
	}
}

func TestInitSSECKeyFileWrongSize(t *testing.T) {
	keyFile := writeSSECKeyFile(t, []byte("too-short"))

	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionFile": keyFile,
	})
	if err == nil {
		t.Fatal("expected error for wrong key size")
	}
}

func TestInitSSECMutualExclusivity(t *testing.T) {
	keyFile := writeSSECKeyFile(t, make([]byte, 32))

	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionFile": keyFile,
		"kmsKeyId":                  "arn:aws:kms:us-east-1:123456789:key/test",
	})
	if err == nil {
		t.Fatal("expected error when both customerKeyEncryptionFile and kmsKeyId are set")
	}
}

func TestInitSSECMutualExclusivityWithSSE(t *testing.T) {
	keyFile := writeSSECKeyFile(t, make([]byte, 32))

	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionFile": keyFile,
		"serverSideEncryption":      testSSEAES256,
	})
	if err == nil {
		t.Fatal("expected error when both customerKeyEncryptionFile and serverSideEncryption are set")
	}
}

func TestInitSSECKeyFileWithNewline(t *testing.T) {
	key := make([]byte, 32, 33)
	for i := range key {
		key[i] = byte(i)
	}
	keyWithNewline := append(key, '\n')
	keyFile := writeSSECKeyFile(t, keyWithNewline)

	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionFile": keyFile,
	})
	if err != nil {
		t.Fatalf("expected trailing newline to be trimmed, got error: %v", err)
	}
	if store.sseCustomerAlgorithm != testSSEAES256 {
		t.Errorf("sseCustomerAlgorithm = %q, want %q", store.sseCustomerAlgorithm, testSSEAES256)
	}
}

func TestInitSSECKeyFileExact32BytesEndingInNewline(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	key[31] = '\n'
	keyFile := writeSSECKeyFile(t, key)

	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionFile": keyFile,
	})
	if err != nil {
		t.Fatalf("valid 32-byte key ending in 0x0A should not be trimmed, got error: %v", err)
	}
}

func TestInitObjectStoreWithEncryptionSettings(t *testing.T) {
	cfg := &common.ObjectStoreConfig{
		BSLProvider:             "aws",
		BSLBucket:               "test-bucket",
		BSLRegion:               "us-east-1",
		BSLServerSideEncryption: testSSEAES256,
		BSLChecksumAlgorithm:    testChecksumCRC32,
	}

	store, err := InitObjectStore(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestInitWithBooleanParsing(t *testing.T) {
	store := &S3ObjectStore{}
	err := store.Init(map[string]string{
		"bucket":                "test-bucket",
		"s3ForcePathStyle":      "1",
		"insecureSkipTLSVerify": "t",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// generateTestCACertPEM creates a self-signed CA certificate PEM at runtime for testing.
func generateTestCACertPEM(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	var buf bytes.Buffer
	if err := pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatalf("failed to encode PEM: %v", err)
	}
	return buf.String()
}

// capturingTransport is an http.RoundTripper that records requests and returns
// a canned S3-compatible response so the SDK doesn't error.
type capturingTransport struct {
	mu       sync.Mutex
	requests []*http.Request
}

func (ct *capturingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Drain the request body so the SDK's checksum computation can complete.
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
	}

	ct.mu.Lock()
	ct.requests = append(ct.requests, req.Clone(req.Context()))
	ct.mu.Unlock()

	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Etag":         {`"d41d8cd98f00b204e9800998ecf8427e"`},
			"Content-Type": {"application/octet-stream"},
		},
		Body: io.NopCloser(strings.NewReader("test-content")),
	}, nil
}

func (t *capturingTransport) lastRequest() *http.Request {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.requests) == 0 {
		return nil
	}
	return t.requests[len(t.requests)-1]
}

// newTestS3StoreWithTransport creates an S3ObjectStore with a custom HTTP
// transport for intercepting requests. The store is configured with the
// given encryption/checksum fields already set.
func newTestS3StoreWithTransport(t *testing.T, transport http.RoundTripper, opts func(*S3ObjectStore)) *S3ObjectStore {
	t.Helper()
	httpClient := &http.Client{Transport: transport}
	client := s3.New(s3.Options{
		Region:      "us-east-1",
		HTTPClient:  httpClient,
		Credentials: aws.AnonymousCredentials{},
	})
	store := &S3ObjectStore{
		client:   client,
		uploader: transfermanager.New(client),
		bucket:   "test-bucket",
	}
	if opts != nil {
		opts(store)
	}
	return store
}

func TestPutObjectSendsSSEHeaders(t *testing.T) {
	transport := &capturingTransport{}
	store := newTestS3StoreWithTransport(t, transport, func(s *S3ObjectStore) {
		s.serverSideEncryption = testSSEAES256
	})

	err := store.PutObject("test-bucket", "test-key", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	req := transport.lastRequest()
	if req == nil {
		t.Fatal("no request captured")
	}
	got := req.Header.Get("X-Amz-Server-Side-Encryption")
	if got != testSSEAES256 {
		t.Errorf("X-Amz-Server-Side-Encryption = %q, want %q", got, testSSEAES256)
	}
}

func TestPutObjectSendsKMSHeaders(t *testing.T) {
	transport := &capturingTransport{}
	kmsKey := "arn:aws:kms:us-east-1:123456789:key/test-key-id"
	store := newTestS3StoreWithTransport(t, transport, func(s *S3ObjectStore) {
		s.serverSideEncryption = "aws:kms"
		s.kmsKeyId = kmsKey
	})

	err := store.PutObject("test-bucket", "test-key", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	req := transport.lastRequest()
	if req == nil {
		t.Fatal("no request captured")
	}
	if got := req.Header.Get("X-Amz-Server-Side-Encryption"); got != "aws:kms" {
		t.Errorf("X-Amz-Server-Side-Encryption = %q, want aws:kms", got)
	}
	if got := req.Header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"); got != kmsKey {
		t.Errorf("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id = %q, want %q", got, kmsKey)
	}
}

func TestPutObjectSendsChecksumHeader(t *testing.T) {
	transport := &capturingTransport{}
	store := newTestS3StoreWithTransport(t, transport, func(s *S3ObjectStore) {
		s.checksumAlgorithm = testChecksumCRC32
	})

	err := store.PutObject("test-bucket", "test-key", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	req := transport.lastRequest()
	if req == nil {
		t.Fatal("no request captured")
	}
	// The SDK sends x-amz-sdk-checksum-algorithm to indicate which algorithm is used,
	// and the actual checksum value may be sent as a trailing header.
	foundChecksum := false
	for key := range req.Header {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "checksum") {
			foundChecksum = true
			break
		}
	}
	if !foundChecksum {
		t.Errorf("expected a checksum-related header, got none. Headers: %v", req.Header)
	}
}

func TestPutObjectSendsSSECHeaders(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	keyFile := writeSSECKeyFile(t, key)

	// Init to compute the base64/MD5 values, then copy to a transport-backed store
	initStore := &S3ObjectStore{}
	err := initStore.Init(map[string]string{
		"bucket":                    "test-bucket",
		"customerKeyEncryptionFile": keyFile,
	})
	if err != nil {
		t.Fatalf("Init error: %v", err)
	}

	transport := &capturingTransport{}
	store := newTestS3StoreWithTransport(t, transport, func(s *S3ObjectStore) {
		s.sseCustomerAlgorithm = initStore.sseCustomerAlgorithm
		s.sseCustomerKey = initStore.sseCustomerKey
		s.sseCustomerKeyMD5 = initStore.sseCustomerKeyMD5
	})

	err = store.PutObject("test-bucket", "test-key", strings.NewReader("data"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	req := transport.lastRequest()
	if req == nil {
		t.Fatal("no request captured")
	}
	if got := req.Header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"); got != testSSEAES256 {
		t.Errorf("SSE-C algorithm header = %q, want %q", got, testSSEAES256)
	}
	if got := req.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key"); got == "" {
		t.Error("SSE-C key header should be set")
	}
	if got := req.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key-Md5"); got == "" {
		t.Error("SSE-C key MD5 header should be set")
	}
}

func TestGetObjectSendsSSECHeaders(t *testing.T) {
	transport := &capturingTransport{}
	store := newTestS3StoreWithTransport(t, transport, func(s *S3ObjectStore) {
		s.sseCustomerAlgorithm = testSSEAES256
		s.sseCustomerKey = "dGVzdC1rZXktYmFzZTY0LWVuY29kZWQ="
		s.sseCustomerKeyMD5 = "dGVzdC1tZDU="
	})

	body, err := store.GetObject("test-bucket", "test-key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = body.Close()

	req := transport.lastRequest()
	if req == nil {
		t.Fatal("no request captured")
	}
	if got := req.Header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm"); got != testSSEAES256 {
		t.Errorf("SSE-C algorithm header = %q, want %q", got, testSSEAES256)
	}
	if got := req.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key"); got == "" {
		t.Error("SSE-C key header should be set on GET")
	}
	if got := req.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key-Md5"); got == "" {
		t.Error("SSE-C key MD5 header should be set on GET")
	}
}
