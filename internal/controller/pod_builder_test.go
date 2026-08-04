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

package controller

import (
	"path"
	"strings"
	"testing"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/downloader"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	corev1 "k8s.io/api/core/v1"
)

//nolint:gocyclo,goconst // Table-driven test with many validation cases
func TestBuildDatamoverPod(t *testing.T) {
	tests := []struct {
		name     string
		config   *DatamoverPodConfig
		validate func(*testing.T, *corev1.Pod)
	}{
		{
			name: "basic pod configuration",
			config: &DatamoverPodConfig{
				Name:                 "kubevirt-dm-test-du",
				Namespace:            "test-ns",
				Image:                "quay.io/test/datamover:latest",
				ImagePullPolicy:      corev1.PullIfNotPresent,
				BSLProvider:          "aws",
				BSLBucket:            "test-bucket",
				BSLPrefix:            "backups",
				BSLRegion:            "us-east-1",
				CredentialSecretName: "cloud-credentials",
				CredentialSecretKey:  "cloud",
				VMName:               "test-vm",
				VMNamespace:          "vm-ns",
				CheckpointName:       "checkpoint-001",
				BackupType:           "full",
				VeleroBackupName:     "backup-001",
				ResourceName:         "test-du",
				ResourceUID:          "uid-12345",
				VMBName:              "vmb-test-du",
				SourcePVCName:        "kubevirt-backup-test-du",
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				// Verify pod metadata
				if pod.Name != "kubevirt-dm-test-du" {
					t.Errorf("pod name = %q, want %q", pod.Name, "kubevirt-dm-test-du")
				}
				if pod.Namespace != "test-ns" {
					t.Errorf("pod namespace = %q, want %q", pod.Namespace, "test-ns")
				}

				// Verify labels
				if pod.Labels[common.LabelDatamoverPod] != "upload" {
					t.Errorf("label %s = %q, want %q", common.LabelDatamoverPod, pod.Labels[common.LabelDatamoverPod], "upload")
				}
				if pod.Labels[common.LabelDataUploadUID] != "uid-12345" {
					t.Errorf("label %s = %q, want %q", common.LabelDataUploadUID, pod.Labels[common.LabelDataUploadUID], "uid-12345")
				}
				if pod.Annotations[common.AnnotationDataUploadName] != "test-du" {
					t.Errorf("annotation %s = %q, want %q", common.AnnotationDataUploadName, pod.Annotations[common.AnnotationDataUploadName], "test-du")
				}

				// Verify pod spec
				if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
					t.Errorf("restart policy = %q, want %q", pod.Spec.RestartPolicy, corev1.RestartPolicyNever)
				}
				if pod.Spec.ServiceAccountName != "velero" {
					t.Errorf("service account = %q, want %q", pod.Spec.ServiceAccountName, "velero")
				}

				// Verify container
				if len(pod.Spec.Containers) != 1 {
					t.Fatalf("expected 1 container, got %d", len(pod.Spec.Containers))
				}
				container := pod.Spec.Containers[0]
				if container.Name != "upload" {
					t.Errorf("container name = %q, want %q", container.Name, "upload")
				}
				if container.Image != "quay.io/test/datamover:latest" {
					t.Errorf("container image = %q, want %q", container.Image, "quay.io/test/datamover:latest")
				}

				// Verify command
				if len(container.Command) != 2 || container.Command[0] != "/manager" || container.Command[1] != "upload" {
					t.Errorf("container command = %v, want [/manager upload]", container.Command)
				}
			},
		},
		{
			name: "download mode sets correct label, container name, and command",
			config: &DatamoverPodConfig{
				OperationMode:        OperationModeDownload,
				Name:                 "test-dl",
				Namespace:            "test-ns",
				Image:                "quay.io/test/datamover:latest",
				ImagePullPolicy:      corev1.PullAlways,
				ResourceName:         "test-dd",
				ResourceUID:          "uid-dl-123",
				UIDLabelKey:          common.LabelDataDownloadUID,
				NameAnnotationKey:    common.AnnotationDataDownloadName,
				CredentialSecretName: "cloud-credentials",
				CredentialSecretKey:  "cloud",
				ScratchPVCName:       "restore-scratch-pvc",
				TargetVolume:         "vm-disk-1",
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				if pod.Labels[common.LabelDatamoverPod] != "download" {
					t.Errorf("label %s = %q, want %q", common.LabelDatamoverPod, pod.Labels[common.LabelDatamoverPod], "download")
				}
				if pod.Labels[common.LabelDataDownloadUID] != "uid-dl-123" {
					t.Errorf("label %s = %q, want %q", common.LabelDataDownloadUID, pod.Labels[common.LabelDataDownloadUID], "uid-dl-123")
				}
				if pod.Annotations[common.AnnotationDataDownloadName] != "test-dd" {
					t.Errorf("annotation %s = %q, want %q", common.AnnotationDataDownloadName, pod.Annotations[common.AnnotationDataDownloadName], "test-dd")
				}
				container := pod.Spec.Containers[0]
				if container.Name != "download" {
					t.Errorf("container name = %q, want %q", container.Name, "download")
				}
				if len(container.Command) != 2 || container.Command[0] != "/manager" || container.Command[1] != "download" {
					t.Errorf("container command = %v, want [/manager download]", container.Command)
				}
			},
		},
		{
			name: "download mode with unset label/annotation keys defaults to DataDownload, not DataUpload",
			config: &DatamoverPodConfig{
				OperationMode: OperationModeDownload,
				Name:          "test-dl",
				Namespace:     "test-ns",
				Image:         "quay.io/test/datamover:latest",
				ResourceName:  "test-dd",
				ResourceUID:   "uid-dl-456",
				// UIDLabelKey/NameAnnotationKey deliberately left unset.
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				if _, ok := pod.Labels[common.LabelDataUploadUID]; ok {
					t.Error("download-mode pod with unset UIDLabelKey must not fall back to the DataUpload UID label")
				}
				if pod.Labels[common.LabelDataDownloadUID] != "uid-dl-456" {
					t.Errorf("label %s = %q, want %q", common.LabelDataDownloadUID, pod.Labels[common.LabelDataDownloadUID], "uid-dl-456")
				}
				if _, ok := pod.Annotations[common.AnnotationDataUploadName]; ok {
					t.Error("download-mode pod with unset NameAnnotationKey must not fall back to the DataUpload name annotation")
				}
				if pod.Annotations[common.AnnotationDataDownloadName] != "test-dd" {
					t.Errorf("annotation %s = %q, want %q", common.AnnotationDataDownloadName, pod.Annotations[common.AnnotationDataDownloadName], "test-dd")
				}
			},
		},
		{
			name: "download mode environment variables are set correctly",
			config: &DatamoverPodConfig{
				OperationMode:            OperationModeDownload,
				Name:                     "test-dl",
				Namespace:                "test-ns",
				Image:                    "quay.io/test/datamover:latest",
				BSLProvider:              "aws",
				BSLBucket:                "my-bucket",
				BSLPrefix:                "my-prefix",
				BSLRegion:                "eu-west-1",
				BSLS3URL:                 "https://minio.example.com",
				BSLS3ForcePathStyle:      "true",
				BSLInsecureSkipTLSVerify: "false",
				BSLCACert:                "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
				CredentialSecretName:     "cloud-credentials",
				CredentialSecretKey:      "cloud",
				VMName:                   "my-vm",
				VMNamespace:              "my-ns",
				VeleroBackupName:         "velero-backup",
				ResourceName:             "dd-001",
				ResourceUID:              "uid-dl-001",
				UIDLabelKey:              common.LabelDataDownloadUID,
				NameAnnotationKey:        common.AnnotationDataDownloadName,
				ScratchPVCName:           "scratch-pvc-001",
				TargetVolume:             "vm-disk-1",
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				container := pod.Spec.Containers[0]
				envMap := make(map[string]string)
				for _, env := range container.Env {
					envMap[env.Name] = env.Value
				}

				expectedEnvs := map[string]string{
					common.EnvBSLProvider:              "aws",
					common.EnvBSLBucket:                "my-bucket",
					common.EnvBSLPrefix:                "my-prefix",
					common.EnvBSLRegion:                "eu-west-1",
					common.EnvBSLS3URL:                 "https://minio.example.com",
					common.EnvBSLS3ForcePathStyle:      "true",
					common.EnvBSLInsecureSkipTLSVerify: "false",
					common.EnvBSLCACert:                "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
					common.EnvCredentialsFile:          common.DefaultCredentialsPath,
					downloader.EnvVMName:               "my-vm",
					downloader.EnvVMNamespace:          "my-ns",
					downloader.EnvVeleroBackupName:     "velero-backup",
					downloader.EnvDataDownloadName:     "dd-001",
					downloader.EnvDataDownloadUID:      "uid-dl-001",
					downloader.EnvTargetVolume:         "vm-disk-1",
					downloader.EnvScratchPath:          downloader.DefaultScratchPath,
				}

				for key, expected := range expectedEnvs {
					if envMap[key] != expected {
						t.Errorf("env %s = %q, want %q", key, envMap[key], expected)
					}
				}

				// TargetPath must be disk.img at the scratch mount root -- the
				// download pod has only one read-write volume, so the flattened raw
				// image and the downloaded qcow2 chain share it until the final PV
				// rebind, and disk.img matches the CDI/KubeVirt filesystem-mode
				// PVC convention since this PV is rebound as the restore target.
				targetPath := envMap[downloader.EnvTargetPath]
				expectedTargetPath := path.Join(downloader.DefaultScratchPath, "disk.img")
				if targetPath != expectedTargetPath {
					t.Errorf("env %s = %q, want %q", downloader.EnvTargetPath, targetPath, expectedTargetPath)
				}

				// Upload-only env vars must not leak into download-mode pods.
				for _, key := range []string{
					uploader.EnvSourcePVCPath,
					uploader.EnvCheckpointName,
					uploader.EnvBackupType,
					uploader.EnvVMBName,
					uploader.EnvVMBTName,
					uploader.EnvDataUploadName,
					uploader.EnvDataUploadUID,
					common.EnvBSLKMSKeyName,
				} {
					if _, ok := envMap[key]; ok {
						t.Errorf("upload-only env %s should not be set in download mode", key)
					}
				}
			},
		},
		{
			name: "download mode volume mounts are configured correctly",
			config: &DatamoverPodConfig{
				OperationMode:        OperationModeDownload,
				Name:                 "test-dl",
				Namespace:            "test-ns",
				Image:                "test-image",
				CredentialSecretName: "cloud-creds",
				CredentialSecretKey:  "credentials",
				ScratchPVCName:       "scratch-pvc-001",
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				container := pod.Spec.Containers[0]

				if len(container.VolumeMounts) != 3 {
					t.Fatalf("expected 3 volume mounts, got %d", len(container.VolumeMounts))
				}

				var scratchMount, credsMount, saTokenMount *corev1.VolumeMount
				for i := range container.VolumeMounts {
					switch container.VolumeMounts[i].Name {
					case scratchDataVolumeName:
						scratchMount = &container.VolumeMounts[i]
					case cloudCredentialsVolumeName:
						credsMount = &container.VolumeMounts[i]
					case boundSATokenVolumeName:
						saTokenMount = &container.VolumeMounts[i]
					}
				}

				if scratchMount == nil {
					t.Fatal("scratch-data volume mount not found")
				}
				if scratchMount.MountPath != downloader.DefaultScratchPath {
					t.Errorf("scratch-data mount path = %q, want %q", scratchMount.MountPath, downloader.DefaultScratchPath)
				}
				if scratchMount.ReadOnly {
					t.Error("scratch-data mount must be read-write (downloader writes the flattened image into it)")
				}

				if credsMount == nil {
					t.Fatal("cloud-credentials volume mount not found")
				}
				expectedCredentialsDir := path.Dir(common.DefaultCredentialsPath)
				if credsMount.MountPath != expectedCredentialsDir {
					t.Errorf("cloud-credentials mount path = %q, want %q", credsMount.MountPath, expectedCredentialsDir)
				}
				if !credsMount.ReadOnly {
					t.Error("cloud-credentials mount should be read-only")
				}

				if saTokenMount == nil {
					t.Fatal("bound-sa-token volume mount not found")
				}
				if !saTokenMount.ReadOnly {
					t.Error("bound-sa-token mount should be read-only")
				}

				if len(pod.Spec.Volumes) != 3 {
					t.Fatalf("expected 3 volumes, got %d", len(pod.Spec.Volumes))
				}

				var pvcVolume, credentialsVolume *corev1.Volume
				for i := range pod.Spec.Volumes {
					switch pod.Spec.Volumes[i].Name {
					case scratchDataVolumeName:
						pvcVolume = &pod.Spec.Volumes[i]
					case cloudCredentialsVolumeName:
						credentialsVolume = &pod.Spec.Volumes[i]
					}
				}
				if pvcVolume == nil || pvcVolume.PersistentVolumeClaim == nil {
					t.Fatal("scratch-data PVC volume not found")
				}
				if pvcVolume.PersistentVolumeClaim.ClaimName != "scratch-pvc-001" {
					t.Errorf("PVC claim name = %q, want %q", pvcVolume.PersistentVolumeClaim.ClaimName, "scratch-pvc-001")
				}
				if pvcVolume.PersistentVolumeClaim.ReadOnly {
					t.Error("scratch-data PVC volume source must not be read-only")
				}

				if credentialsVolume == nil || credentialsVolume.Secret == nil {
					t.Fatal("cloud-credentials secret volume not found")
				}
				if credentialsVolume.Secret.SecretName != "cloud-creds" {
					t.Errorf("secret name = %q, want %q", credentialsVolume.Secret.SecretName, "cloud-creds")
				}
				if credentialsVolume.Secret.DefaultMode == nil || *credentialsVolume.Secret.DefaultMode != 0400 {
					t.Errorf("credentials default mode = %v, want 0400", credentialsVolume.Secret.DefaultMode)
				}
				if len(credentialsVolume.Secret.Items) != 1 {
					t.Errorf("credential item count = %d, want 1", len(credentialsVolume.Secret.Items))
				} else {
					item := credentialsVolume.Secret.Items[0]
					if item.Key != "credentials" {
						t.Errorf("credential key = %q, want %q", item.Key, "credentials")
					}
					if item.Path != path.Base(common.DefaultCredentialsPath) {
						t.Errorf("credential path = %q, want %q", item.Path, path.Base(common.DefaultCredentialsPath))
					}
				}

				// Upload-only backup-data mount must not be present.
				for _, vm := range container.VolumeMounts {
					if vm.Name == backupDataVolumeName {
						t.Error("upload-only backup-data mount should not be present in download mode")
					}
				}
			},
		},
		{
			name: "environment variables are set correctly",
			config: &DatamoverPodConfig{
				Name:                     "test-pod",
				Namespace:                "default",
				Image:                    "test-image",
				BSLProvider:              "aws",
				BSLBucket:                "my-bucket",
				BSLPrefix:                "my-prefix",
				BSLRegion:                "eu-west-1",
				BSLS3URL:                 "https://minio.example.com",
				BSLS3ForcePathStyle:      "true",
				BSLInsecureSkipTLSVerify: "false",
				BSLCACert:                "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
				BSLProfile:               "minio",
				CredentialSecretName:     "secret",
				CredentialSecretKey:      "key",
				VMName:                   "my-vm",
				VMNamespace:              "my-ns",
				CheckpointName:           "cp-001",
				BackupType:               "incremental",
				VeleroBackupName:         "velero-backup",
				ResourceName:             "du-001",
				ResourceUID:              "uid-001",
				VMBName:                  "vmb-001",
				SourcePVCName:            "pvc-001",
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				container := pod.Spec.Containers[0]
				envMap := make(map[string]string)
				for _, env := range container.Env {
					envMap[env.Name] = env.Value
				}

				expectedEnvs := map[string]string{
					common.EnvBSLProvider:              "aws",
					common.EnvBSLBucket:                "my-bucket",
					common.EnvBSLPrefix:                "my-prefix",
					common.EnvBSLRegion:                "eu-west-1",
					common.EnvBSLS3URL:                 "https://minio.example.com",
					common.EnvBSLS3ForcePathStyle:      "true",
					common.EnvBSLInsecureSkipTLSVerify: "false",
					common.EnvBSLCACert:                "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
					common.EnvBSLProfile:               "minio",
					common.EnvCredentialsFile:          common.DefaultCredentialsPath,
					uploader.EnvVMName:                 "my-vm",
					uploader.EnvVMNamespace:            "my-ns",
					uploader.EnvCheckpointName:         "cp-001",
					uploader.EnvBackupType:             "incremental",
					uploader.EnvVeleroBackupName:       "velero-backup",
					uploader.EnvSourcePVCPath:          uploader.DefaultSourcePVCPath,
					uploader.EnvDataUploadName:         "du-001",
					uploader.EnvDataUploadUID:          "uid-001",
					uploader.EnvVMBName:                "vmb-001",
				}

				for key, expected := range expectedEnvs {
					if envMap[key] != expected {
						t.Errorf("env %s = %q, want %q", key, envMap[key], expected)
					}
				}
			},
		},
		{
			name: "volume mounts are configured correctly",
			config: &DatamoverPodConfig{
				Name:                 "test-pod",
				Namespace:            "default",
				Image:                "test-image",
				CredentialSecretName: "cloud-creds",
				CredentialSecretKey:  "credentials",
				SourcePVCName:        "backup-pvc",
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				container := pod.Spec.Containers[0]

				// Verify volume mounts
				if len(container.VolumeMounts) != 3 {
					t.Fatalf("expected 3 volume mounts, got %d", len(container.VolumeMounts))
				}

				// Check backup-data mount
				var backupMount, credsMount, saTokenMount *corev1.VolumeMount
				for i := range container.VolumeMounts {
					switch container.VolumeMounts[i].Name {
					case backupDataVolumeName:
						backupMount = &container.VolumeMounts[i]
					case cloudCredentialsVolumeName:
						credsMount = &container.VolumeMounts[i]
					case "bound-sa-token":
						saTokenMount = &container.VolumeMounts[i]
					}
				}

				if backupMount == nil {
					t.Fatal("backup-data volume mount not found")
				}
				if backupMount.MountPath != uploader.DefaultSourcePVCPath {
					t.Errorf("backup-data mount path = %q, want %q", backupMount.MountPath, uploader.DefaultSourcePVCPath)
				}
				if !backupMount.ReadOnly {
					t.Error("backup-data mount should be read-only")
				}

				if credsMount == nil {
					t.Fatal("cloud-credentials volume mount not found")
				}
				if credsMount.MountPath != "/credentials" {
					t.Errorf("cloud-credentials mount path = %q, want %q", credsMount.MountPath, "/credentials")
				}
				if !credsMount.ReadOnly {
					t.Error("cloud-credentials mount should be read-only")
				}

				if saTokenMount == nil {
					t.Fatal("bound-sa-token volume mount not found")
				}
				if saTokenMount.MountPath != "/var/run/secrets/openshift/serviceaccount" {
					t.Errorf("bound-sa-token mount path = %q, want %q", saTokenMount.MountPath, "/var/run/secrets/openshift/serviceaccount")
				}
				if !saTokenMount.ReadOnly {
					t.Error("bound-sa-token mount should be read-only")
				}

				// Verify volumes
				if len(pod.Spec.Volumes) != 3 {
					t.Fatalf("expected 3 volumes, got %d", len(pod.Spec.Volumes))
				}

				var pvcVolume, secretVolume, saTokenVolume *corev1.Volume
				for i := range pod.Spec.Volumes {
					switch pod.Spec.Volumes[i].Name {
					case backupDataVolumeName:
						pvcVolume = &pod.Spec.Volumes[i]
					case cloudCredentialsVolumeName:
						secretVolume = &pod.Spec.Volumes[i]
					case "bound-sa-token":
						saTokenVolume = &pod.Spec.Volumes[i]
					}
				}

				if pvcVolume == nil || pvcVolume.PersistentVolumeClaim == nil {
					t.Fatal("backup-data PVC volume not found")
				}
				if pvcVolume.PersistentVolumeClaim.ClaimName != "backup-pvc" {
					t.Errorf("PVC claim name = %q, want %q", pvcVolume.PersistentVolumeClaim.ClaimName, "backup-pvc")
				}

				if secretVolume == nil || secretVolume.Secret == nil {
					t.Fatal("cloud-credentials secret volume not found")
				}
				if secretVolume.Secret.SecretName != "cloud-creds" {
					t.Errorf("secret name = %q, want %q", secretVolume.Secret.SecretName, "cloud-creds")
				}

				if saTokenVolume == nil || saTokenVolume.Projected == nil {
					t.Fatal("bound-sa-token projected volume not found")
				}
				sources := saTokenVolume.Projected.Sources
				if len(sources) != 1 || sources[0].ServiceAccountToken == nil {
					t.Fatal("bound-sa-token volume should have exactly one ServiceAccountToken projection")
				}
				saToken := sources[0].ServiceAccountToken
				if saToken.Audience != "openshift" {
					t.Errorf("bound-sa-token audience = %q, want %q", saToken.Audience, "openshift")
				}
				if saToken.Path != "token" {
					t.Errorf("bound-sa-token path = %q, want %q", saToken.Path, "token")
				}
				if saToken.ExpirationSeconds == nil || *saToken.ExpirationSeconds != 3600 {
					t.Errorf("bound-sa-token expirationSeconds = %v, want 3600", saToken.ExpirationSeconds)
				}
			},
		},
		{
			name: "security context is configured correctly",
			config: &DatamoverPodConfig{
				Name:                 "test-pod",
				Namespace:            "default",
				Image:                "test-image",
				CredentialSecretName: "secret",
				CredentialSecretKey:  "key",
				SourcePVCName:        "pvc",
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				// Verify pod security context
				if pod.Spec.SecurityContext == nil {
					t.Fatal("pod security context is nil")
				}
				if pod.Spec.SecurityContext.RunAsUser == nil || *pod.Spec.SecurityContext.RunAsUser != 0 {
					t.Error("pod should run as root (UID 0)")
				}
				if pod.Spec.SecurityContext.SELinuxOptions == nil || pod.Spec.SecurityContext.SELinuxOptions.Type != "spc_t" {
					t.Error("pod should have SELinux type spc_t")
				}

				// Verify container security context
				container := pod.Spec.Containers[0]
				if container.SecurityContext == nil {
					t.Fatal("container security context is nil")
				}
				if container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
					t.Error("container should have read-only root filesystem")
				}
			},
		},
		{
			name: "custom labels are merged",
			config: &DatamoverPodConfig{
				Name:                 "test-pod",
				Namespace:            "default",
				Image:                "test-image",
				ResourceName:         "du-001",
				ResourceUID:          "uid-001",
				CredentialSecretName: "secret",
				CredentialSecretKey:  "key",
				SourcePVCName:        "pvc",
				Labels: map[string]string{
					"custom-label":  "custom-value",
					"another-label": "another-value",
				},
			},
			validate: func(t *testing.T, pod *corev1.Pod) {
				// Check custom labels are present
				if pod.Labels["custom-label"] != "custom-value" {
					t.Errorf("custom-label = %q, want %q", pod.Labels["custom-label"], "custom-value")
				}
				if pod.Labels["another-label"] != "another-value" {
					t.Errorf("another-label = %q, want %q", pod.Labels["another-label"], "another-value")
				}
				// Check default labels are still present
				if pod.Labels[common.LabelDatamoverPod] != "upload" {
					t.Errorf("default label missing: %s", common.LabelDatamoverPod)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := buildDatamoverPod(tt.config)
			if pod == nil {
				t.Fatal("buildDatamoverPod returned nil")
			}
			tt.validate(t, pod)
		})
	}
}

func TestDatamoverPodConfigDefaults(t *testing.T) {
	config := &DatamoverPodConfig{
		Name:                 "test",
		Namespace:            "default",
		Image:                "test",
		CredentialSecretName: "secret",
		CredentialSecretKey:  "key",
		SourcePVCName:        "pvc",
	}

	pod := buildDatamoverPod(config)

	// Verify defaults
	container := pod.Spec.Containers[0]
	envMap := make(map[string]string)
	for _, env := range container.Env {
		envMap[env.Name] = env.Value
	}

	// Check default paths are used
	if envMap[common.EnvCredentialsFile] != common.DefaultCredentialsPath {
		t.Errorf("credentials file = %q, want default %q", envMap[common.EnvCredentialsFile], common.DefaultCredentialsPath)
	}
	if envMap[uploader.EnvSourcePVCPath] != uploader.DefaultSourcePVCPath {
		t.Errorf("source pvc path = %q, want default %q", envMap[uploader.EnvSourcePVCPath], uploader.DefaultSourcePVCPath)
	}
}

// TestS3CompatibleBooleanRoundtrip verifies the strconv.FormatBool → env var →
// strings.EqualFold roundtrip. The controller converts bools to "true"/"false"
// strings for env vars; the uploader parses them back. Only "true" (case-
// insensitive) should produce true; "false" must not.
func TestS3CompatibleBooleanRoundtrip(t *testing.T) {
	tests := []struct {
		name                  string
		s3ForcePathStyle      string
		insecureSkipTLSVerify string
		wantForcePathStyle    bool
		wantInsecureSkip      bool
	}{
		{
			name:                  "both true",
			s3ForcePathStyle:      "true",
			insecureSkipTLSVerify: "true",
			wantForcePathStyle:    true,
			wantInsecureSkip:      true,
		},
		{
			name:                  "both false (from strconv.FormatBool)",
			s3ForcePathStyle:      "false",
			insecureSkipTLSVerify: "false",
			wantForcePathStyle:    false,
			wantInsecureSkip:      false,
		},
		{
			name:                  "both empty",
			s3ForcePathStyle:      "",
			insecureSkipTLSVerify: "",
			wantForcePathStyle:    false,
			wantInsecureSkip:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &DatamoverPodConfig{
				Name:                     "test",
				Namespace:                "default",
				Image:                    "test",
				CredentialSecretName:     "secret",
				CredentialSecretKey:      "key",
				SourcePVCName:            "pvc",
				BSLS3ForcePathStyle:      tt.s3ForcePathStyle,
				BSLInsecureSkipTLSVerify: tt.insecureSkipTLSVerify,
			}

			pod := buildDatamoverPod(config)
			container := pod.Spec.Containers[0]
			envMap := make(map[string]string)
			for _, env := range container.Env {
				envMap[env.Name] = env.Value
			}

			// Verify env var values match input
			if envMap[common.EnvBSLS3ForcePathStyle] != tt.s3ForcePathStyle {
				t.Errorf("env %s = %q, want %q",
					common.EnvBSLS3ForcePathStyle, envMap[common.EnvBSLS3ForcePathStyle], tt.s3ForcePathStyle)
			}
			if envMap[common.EnvBSLInsecureSkipTLSVerify] != tt.insecureSkipTLSVerify {
				t.Errorf("env %s = %q, want %q",
					common.EnvBSLInsecureSkipTLSVerify, envMap[common.EnvBSLInsecureSkipTLSVerify], tt.insecureSkipTLSVerify)
			}

			// Simulate uploader-side parsing (same logic as LoadConfigFromEnv)
			gotForcePathStyle := strings.EqualFold(envMap[common.EnvBSLS3ForcePathStyle], "true")
			gotInsecureSkip := strings.EqualFold(envMap[common.EnvBSLInsecureSkipTLSVerify], "true")

			if gotForcePathStyle != tt.wantForcePathStyle {
				t.Errorf("parsed s3ForcePathStyle = %v, want %v", gotForcePathStyle, tt.wantForcePathStyle)
			}
			if gotInsecureSkip != tt.wantInsecureSkip {
				t.Errorf("parsed insecureSkipTLSVerify = %v, want %v", gotInsecureSkip, tt.wantInsecureSkip)
			}
		})
	}
}
