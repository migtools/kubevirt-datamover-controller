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
	"encoding/base64"
	"os"
	"testing"
)

func TestAzureObjectStoreFullKey(t *testing.T) {
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
			store := &AzureObjectStore{prefix: tt.prefix}
			result := store.fullKey(tt.key)
			if result != tt.expected {
				t.Errorf("fullKey(%q) = %q, want %q", tt.key, result, tt.expected)
			}
		})
	}
}

func TestAzureObjectStoreInit(t *testing.T) {
	validDummyKey := base64.StdEncoding.EncodeToString([]byte("dummy-key-data"))

	tests := []struct {
		name        string
		config      map[string]string
		envAccount  string
		envKey      string
		expectError bool
		errorMsg    string
	}{
		{
			name: "missing bucket",
			config: map[string]string{
				"prefix": "backups",
			},
			expectError: true,
			errorMsg:    "bucket is required",
		},
		{
			name: "missing credentials",
			config: map[string]string{
				"bucket": "test-bucket",
			},
			expectError: true,
			errorMsg:    "storageAccount is required",
		},
		{
			name: "valid credentials in config",
			config: map[string]string{
				"bucket":                  "test-bucket",
				"storageAccount":          "testaccount",
				"storageAccountKeyEnvVar": "AZURE_STORAGE_ACCOUNT_ACCESS_KEY",
				"credentialsData":         "AZURE_STORAGE_ACCOUNT_ACCESS_KEY=" + validDummyKey + "\n",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup env vars if needed
			if tt.envAccount != "" {
				os.Setenv("AZURE_STORAGE_ACCOUNT", tt.envAccount)
				defer os.Unsetenv("AZURE_STORAGE_ACCOUNT")
			}
			if tt.envKey != "" {
				os.Setenv("AZURE_STORAGE_ACCOUNT_ACCESS_KEY", tt.envKey)
				defer os.Unsetenv("AZURE_STORAGE_ACCOUNT_ACCESS_KEY")
			}

			store := &AzureObjectStore{}
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

func TestInitObjectStoreAzure(t *testing.T) {
	validDummyKey := base64.StdEncoding.EncodeToString([]byte("dummy-key-data"))
	credData := "AZURE_STORAGE_ACCOUNT_ACCESS_KEY=" + validDummyKey + "\n"

	cfg := &UploaderConfig{
		BSLProvider:                "azure",
		BSLBucket:                  "test-bucket",
		BSLSubscriptionID:          "test-subscription",
		BSLResourceGroup:           "test-group",
		BSLStorageAccountKeyEnvVar: "AZURE_STORAGE_ACCOUNT_ACCESS_KEY",
		CredentialsData:            []byte(credData),
	}

	// Set the storage account in the environment so Velero's util can find it
	os.Setenv("AZURE_STORAGE_ACCOUNT", "testaccount")
	defer os.Unsetenv("AZURE_STORAGE_ACCOUNT")

	store, err := InitObjectStore(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}

	// Verify it's actually an AzureObjectStore
	if _, ok := store.(*AzureObjectStore); !ok {
		t.Errorf("expected store to be of type *AzureObjectStore, got %T", store)
	}
}
