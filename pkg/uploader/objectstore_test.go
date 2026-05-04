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
	"os"
	"path/filepath"
	"testing"
)

func TestS3ObjectStoreFullKey(t *testing.T) {
	tests := []struct {
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

	for _, tt := range tests {
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

func TestInitObjectStore(t *testing.T) {
	tests := []struct {
		name        string
		provider    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "gcp not implemented",
			provider:    "gcp",
			expectError: true,
			errorMsg:    "GCP object store not yet implemented",
		},
		{
			name:        "azure not implemented",
			provider:    "azure",
			expectError: true,
			errorMsg:    "Azure object store not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &UploaderConfig{
				BSLProvider: tt.provider,
				BSLBucket:   "test-bucket",
			}

			_, err := InitObjectStore(config)

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
