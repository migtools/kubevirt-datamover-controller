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

package common

// ObjectStoreConfig holds the BSL/object-store connection settings shared by
// both the uploader (backup) and downloader (restore) datamover runtimes.
// Embedded in both UploaderConfig and DownloaderConfig so object-store
// initialization code can be shared without either package depending on the
// other's full config shape.
type ObjectStoreConfig struct {
	// BSL configuration
	BSLProvider string
	BSLBucket   string
	BSLPrefix   string
	BSLRegion   string

	// S3-compatible storage provider settings
	BSLS3URL                 string // Custom S3 endpoint URL (e.g., "https://minio.example.com")
	BSLS3ForcePathStyle      bool   // Use path-style URLs (required by most S3-compatible stores)
	BSLInsecureSkipTLSVerify bool   // Skip TLS certificate verification
	BSLCACert                string // PEM-encoded custom CA certificate

	// GCP-specific storage provider settings
	BSLServiceAccount string // GCP service account email for compute engine signing
	BSLKMSKeyName     string // Cloud KMS key for server-side encryption

	// Azure-specific storage provider settings
	BSLResourceGroup  string
	BSLStorageAccount string
	BSLSubscriptionID string
	BSLUseAAD         bool

	// CredentialsData holds raw credential content (INI-style).
	// Used by the controller to pass credentials from K8s Secrets directly
	// without writing to a temp file. Takes precedence over CredentialsFile.
	CredentialsData []byte

	// CredentialsFile is the path to a credentials file on disk.
	// Used by the datamover pod where credentials are volume-mounted.
	CredentialsFile string
}
