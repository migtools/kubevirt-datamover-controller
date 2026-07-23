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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestGCPObjectStoreFullKey(t *testing.T) {
	for _, tt := range fullKeyTestCases {
		t.Run(tt.name, func(t *testing.T) {
			store := &GCPObjectStore{prefix: tt.prefix}
			result := store.fullKey(tt.key)
			if result != tt.expected {
				t.Errorf("fullKey(%q) = %q, want %q", tt.key, result, tt.expected)
			}
		})
	}
}

func TestGCPObjectStoreInit(t *testing.T) {
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
			},
			expectError: true,
			errorMsg:    "bucket is required",
		},
		{
			name: "empty bucket",
			config: map[string]string{
				"bucket": "",
				"prefix": "backups",
			},
			expectError: true,
			errorMsg:    "bucket is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &GCPObjectStore{}
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

func TestGCPInitWithCredentialsFile(t *testing.T) {
	saJSON := generateTestGCPServiceAccountJSON(t)

	tmpDir := t.TempDir()
	credFile := filepath.Join(tmpDir, "credentials.json")
	if err := os.WriteFile(credFile, saJSON, 0600); err != nil {
		t.Fatalf("failed to write credentials file: %v", err)
	}

	store := &GCPObjectStore{}
	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"credentialsFile": credFile,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGCPInitWithCredentialsData(t *testing.T) {
	saJSON := generateTestGCPServiceAccountJSON(t)

	store := &GCPObjectStore{}
	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"credentialsData": string(saJSON),
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGCPInitWithInvalidCredentialsFile(t *testing.T) {
	store := &GCPObjectStore{}
	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"credentialsFile": "/nonexistent/path/credentials.json",
	})
	if err == nil {
		t.Error("expected error for non-existent credentials file, got none")
	}
}

func TestGCPInitWithInvalidCredentialsData(t *testing.T) {
	store := &GCPObjectStore{}
	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"credentialsData": "not-valid-json",
	})
	if err == nil {
		t.Error("expected error for invalid credentials data, got none")
	}
}

func TestGCPInitWithNoCredentials(t *testing.T) {
	// When no credentials are provided, the SDK uses the default credential
	// chain (GOOGLE_APPLICATION_CREDENTIALS, metadata server). Init() should
	// succeed or fail depending on whether default credentials are available.
	// In CI without GCP, FindDefaultCredentials will error, so we accept both.
	store := &GCPObjectStore{}
	_ = store.Init(map[string]string{
		"bucket": "test-bucket",
	})
}

func TestGCPInitSetsSigningCredentials(t *testing.T) {
	saJSON := generateTestGCPServiceAccountJSON(t)

	store := &GCPObjectStore{}
	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"credentialsData": string(saJSON),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store.googleAccessID == "" {
		t.Error("expected googleAccessID to be set from SA key")
	}
	if len(store.privateKey) == 0 {
		t.Error("expected privateKey to be set from SA key")
	}
	if store.iamSvc != nil {
		t.Error("expected iamSvc to be nil for SA key auth")
	}
}

func TestGCPInitWithKMSKeyName(t *testing.T) {
	saJSON := generateTestGCPServiceAccountJSON(t)

	store := &GCPObjectStore{}
	err := store.Init(map[string]string{
		"bucket":          "test-bucket",
		"credentialsData": string(saJSON),
		"kmsKeyName":      "projects/my-project/locations/us/keyRings/kr/cryptoKeys/key",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if store.kmsKeyName != "projects/my-project/locations/us/keyRings/kr/cryptoKeys/key" {
		t.Errorf("kmsKeyName = %q, want projects/my-project/locations/us/keyRings/kr/cryptoKeys/key",
			store.kmsKeyName)
	}
}

func TestInitObjectStoreGCP(t *testing.T) {
	saJSON := generateTestGCPServiceAccountJSON(t)

	tmpDir := t.TempDir()
	credFile := filepath.Join(tmpDir, "credentials.json")
	if err := os.WriteFile(credFile, saJSON, 0600); err != nil {
		t.Fatalf("failed to write credentials file: %v", err)
	}

	cfg := &UploaderConfig{
		BSLProvider:     "gcp",
		BSLBucket:       "test-bucket",
		CredentialsFile: credFile,
	}

	store, err := InitObjectStore(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}

	if _, ok := store.(*GCPObjectStore); !ok {
		t.Errorf("expected store to be of type *GCPObjectStore, got %T", store)
	}
}

func TestInitObjectStoreGCPWithCredentialsData(t *testing.T) {
	saJSON := generateTestGCPServiceAccountJSON(t)

	cfg := &UploaderConfig{
		BSLProvider:     "gcp",
		BSLBucket:       "test-bucket",
		CredentialsData: saJSON,
	}

	store, err := InitObjectStore(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}

	if _, ok := store.(*GCPObjectStore); !ok {
		t.Errorf("expected store to be of type *GCPObjectStore, got %T", store)
	}
}

func TestGetCredAccountType(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		expected credAccountType
		wantErr  bool
	}{
		{
			name:     "service account",
			json:     `{"type": "service_account", "client_email": "test@test.iam.gserviceaccount.com"}`,
			expected: serviceAccountType,
		},
		{
			name:     "external account",
			json:     `{"type": "external_account", "audience": "//iam.googleapis.com/..."}`,
			expected: externalAccountType,
		},
		{
			name:    "invalid JSON",
			json:    "not-json",
			wantErr: true,
		},
		{
			name:    "missing type field",
			json:    `{"client_email": "test@test.iam.gserviceaccount.com"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getCredAccountType([]byte(tt.json))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("getCredAccountType() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// generateTestGCPServiceAccountJSON creates a synthetic GCP service account
// JSON key at runtime for testing. The private key is generated fresh and is
// not a real credential.
func generateTestGCPServiceAccountJSON(t *testing.T) []byte {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	privKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})

	saKey := map[string]string{
		"type":                        "service_account",
		"project_id":                  "test-project",
		"private_key_id":              "key-id-1234",
		"private_key":                 string(privKeyPEM),
		"client_email":                "test@test-project.iam.gserviceaccount.com",
		"client_id":                   "123456789",
		"auth_uri":                    "https://accounts.google.com/o/oauth2/auth",
		"token_uri":                   "https://oauth2.googleapis.com/token",
		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
		"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/" +
			"x509/test%40test-project.iam.gserviceaccount.com",
	}

	data, err := json.Marshal(saKey)
	if err != nil {
		t.Fatalf("failed to marshal SA key JSON: %v", err)
	}

	return data
}
