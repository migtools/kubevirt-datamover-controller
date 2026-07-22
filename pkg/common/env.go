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

// Environment variable names for BSL/object-store configuration, shared by
// both the uploader (backup) and downloader (restore) datamover runtimes —
// upload and download pods never run in the same process, so there's no
// collision reusing the same names.
const (
	EnvBSLProvider              = "KUBEVIRT_DM_BSL_PROVIDER"
	EnvBSLBucket                = "KUBEVIRT_DM_BSL_BUCKET"
	EnvBSLPrefix                = "KUBEVIRT_DM_BSL_PREFIX"
	EnvBSLRegion                = "KUBEVIRT_DM_BSL_REGION"
	EnvCredentialsFile          = "KUBEVIRT_DM_CREDENTIALS_FILE"
	EnvBSLS3URL                 = "KUBEVIRT_DM_BSL_S3_URL"
	EnvBSLS3ForcePathStyle      = "KUBEVIRT_DM_BSL_S3_FORCE_PATH_STYLE"
	EnvBSLInsecureSkipTLSVerify = "KUBEVIRT_DM_BSL_INSECURE_SKIP_TLS_VERIFY"
	EnvBSLCACert                = "KUBEVIRT_DM_BSL_CA_CERT"

	// GCP-specific storage provider settings
	EnvBSLServiceAccount = "KUBEVIRT_DM_BSL_SERVICE_ACCOUNT"
	EnvBSLKMSKeyName     = "KUBEVIRT_DM_BSL_KMS_KEY_NAME"

	// Azure-specific storage provider settings
	EnvBSLResourceGroup               = "KUBEVIRT_DM_BSL_RESOURCE_GROUP"
	EnvBSLStorageAccount              = "KUBEVIRT_DM_BSL_STORAGE_ACCOUNT"
	EnvBSLStorageAccountKeyEnvVar     = "KUBEVIRT_DM_BSL_STORAGE_ACCOUNT_KEY_ENV_VAR"
	EnvBSLStorageAccountURI           = "KUBEVIRT_DM_BSL_STORAGE_ACCOUNT_URI"
	EnvBSLSubscriptionID              = "KUBEVIRT_DM_BSL_SUBSCRIPTION_ID"
	EnvBSLUseAAD                      = "KUBEVIRT_DM_BSL_USE_AAD"
	EnvBSLActiveDirectoryAuthorityURI = "KUBEVIRT_DM_BSL_ACTIVE_DIRECTORY_AUTHORITY_URI"
)

// DefaultCredentialsPath is the credentials file path used when neither the
// uploader nor the downloader is given an explicit one — the path where the
// datamover pod's credentials Secret is volume-mounted.
const DefaultCredentialsPath = "/credentials/cloud"
