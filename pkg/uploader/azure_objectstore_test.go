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

func TestParseAzureCredentials(t *testing.T) {
	tests := []struct {
		name            string
		data            string
		expectedAccount string
		expectedKey     string
	}{
		{
			name:            "standard format",
			data:            "AZURE_STORAGE_ACCOUNT=myaccount\nAZURE_STORAGE_KEY=mykey\n",
			expectedAccount: "myaccount",
			expectedKey:     "mykey",
		},
		{
			name:            "with spaces and extra lines",
			data:            "\n  AZURE_STORAGE_ACCOUNT=myaccount  \n\n  AZURE_STORAGE_KEY=mykey  \n",
			expectedAccount: "myaccount",
			expectedKey:     "mykey",
		},
		{
			name:            "missing key",
			data:            "AZURE_STORAGE_ACCOUNT=myaccount\n",
			expectedAccount: "myaccount",
			expectedKey:     "",
		},
		{
			name:            "empty",
			data:            "",
			expectedAccount: "",
			expectedKey:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, key := parseAzureCredentials([]byte(tt.data))
			if account != tt.expectedAccount {
				t.Errorf("account = %q, want %q", account, tt.expectedAccount)
			}
			if key != tt.expectedKey {
				t.Errorf("key = %q, want %q", key, tt.expectedKey)
			}
		})
	}
}

func TestAzureObjectStoreInit(t *testing.T) {
	// azblob requires the key to be a valid base64 string
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
			errorMsg:    "storageAccount and storageAccountKey are required",
		},
		{
			name: "valid credentials in config",
			config: map[string]string{
				"bucket":            "test-bucket",
				"storageAccount":    "testaccount",
				"storageAccountKey": validDummyKey,
			},
			expectError: false,
		},
		{
			name: "valid credentials in credentialsData",
			config: map[string]string{
				"bucket":          "test-bucket",
				"credentialsData": "AZURE_STORAGE_ACCOUNT=testaccount\nAZURE_STORAGE_KEY=" + validDummyKey,
			},
			expectError: false,
		},
		{
			name: "valid credentials in env vars",
			config: map[string]string{
				"bucket": "test-bucket",
			},
			envAccount:  "testaccount",
			envKey:      validDummyKey,
			expectError: false,
		},
		{
			name: "invalid base64 key",
			config: map[string]string{
				"bucket":            "test-bucket",
				"storageAccount":    "testaccount",
				"storageAccountKey": "not-base64-!@#$",
			},
			expectError: true,
			errorMsg:    "failed to create Azure shared key credential",
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
				os.Setenv("AZURE_STORAGE_KEY", tt.envKey)
				defer os.Unsetenv("AZURE_STORAGE_KEY")
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
	credData := "AZURE_STORAGE_ACCOUNT=testaccount\nAZURE_STORAGE_KEY=" + validDummyKey

	cfg := &UploaderConfig{
		BSLProvider:     "azure",
		BSLBucket:       "test-bucket",
		CredentialsData: []byte(credData),
	}

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
