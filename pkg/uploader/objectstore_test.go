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
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
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
				BSLProvider:     "aws",
				BSLBucket:       "test-bucket",
				BSLRegion:       "us-east-1",
				CredentialsData: []byte(tt.credData),
			}

			store, err := InitObjectStore(cfg)

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
		BSLProvider: "azure",
		BSLBucket:   "test-bucket",
	}

	_, err := InitObjectStore(config)
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
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
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
		BSLProvider:              "aws",
		BSLBucket:                "test-bucket",
		BSLRegion:                "us-east-1",
		BSLS3URL:                 "https://minio.example.com:9000",
		BSLS3ForcePathStyle:      true,
		BSLInsecureSkipTLSVerify: false,
		BSLCACert:                caCert,
	}

	store, err := InitObjectStore(cfg)
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
		BSLProvider:         "minio",
		BSLBucket:           "test-bucket",
		BSLRegion:           "us-east-1",
		BSLS3URL:            "https://minio.example.com",
		BSLS3ForcePathStyle: true,
	}

	store, err := InitObjectStore(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store for unknown provider (S3-compatible fallback)")
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
