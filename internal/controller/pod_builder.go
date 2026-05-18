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
	"maps"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DatamoverPodConfig contains configuration for building a datamover pod.
type DatamoverPodConfig struct {
	// Pod identity
	Name      string
	Namespace string

	// Container image
	Image           string
	ImagePullPolicy corev1.PullPolicy

	// BSL configuration
	BSLProvider string
	BSLBucket   string
	BSLPrefix   string
	BSLRegion   string

	// S3-compatible storage provider settings
	BSLS3URL                 string
	BSLS3ForcePathStyle      string
	BSLInsecureSkipTLSVerify string
	BSLCACert                string

	// Credentials
	CredentialSecretName string
	CredentialSecretKey  string

	// VM context
	VMName      string
	VMNamespace string

	// Backup context
	CheckpointName   string
	BackupType       string
	VeleroBackupName string
	DataUploadName   string
	DataUploadUID    string
	VMBName          string
	VMBTName         string

	// Source PVC
	SourcePVCName string

	// Labels for pod
	Labels map[string]string
}

// buildDatamoverPod creates a Pod spec for the datamover.
func buildDatamoverPod(config *DatamoverPodConfig) *corev1.Pod {
	// Merge default labels with provided labels.
	// Use UID for labels (always ≤ 63 chars); name goes in annotations.
	labels := map[string]string{
		common.LabelDatamoverPod:  "uploader",
		common.LabelDataUploadUID: config.DataUploadUID,
	}
	maps.Copy(labels, config.Labels)

	annotations := map[string]string{
		common.AnnotationDataUploadName: config.DataUploadName,
	}

	// Build environment variables
	envVars := []corev1.EnvVar{
		{Name: uploader.EnvBSLProvider, Value: config.BSLProvider},
		{Name: uploader.EnvBSLBucket, Value: config.BSLBucket},
		{Name: uploader.EnvBSLPrefix, Value: config.BSLPrefix},
		{Name: uploader.EnvBSLRegion, Value: config.BSLRegion},
		{Name: uploader.EnvBSLS3URL, Value: config.BSLS3URL},
		{Name: uploader.EnvBSLS3ForcePathStyle, Value: config.BSLS3ForcePathStyle},
		{Name: uploader.EnvBSLInsecureSkipTLSVerify, Value: config.BSLInsecureSkipTLSVerify},
		{Name: uploader.EnvBSLCACert, Value: config.BSLCACert},
		{Name: uploader.EnvCredentialsFile, Value: uploader.DefaultCredentialsPath},
		{Name: uploader.EnvVMName, Value: config.VMName},
		{Name: uploader.EnvVMNamespace, Value: config.VMNamespace},
		{Name: uploader.EnvCheckpointName, Value: config.CheckpointName},
		{Name: uploader.EnvBackupType, Value: config.BackupType},
		{Name: uploader.EnvVeleroBackupName, Value: config.VeleroBackupName},
		{Name: uploader.EnvSourcePVCPath, Value: uploader.DefaultSourcePVCPath},
		{Name: uploader.EnvDataUploadName, Value: config.DataUploadName},
		{Name: uploader.EnvDataUploadUID, Value: config.DataUploadUID},
		{Name: uploader.EnvVMBName, Value: config.VMBName},
		{Name: uploader.EnvVMBTName, Value: config.VMBTName},
	}

	// Security context - follows Velero's pattern for CSI snapshot pods:
	// - Run as root (UID 0) to access qcow2 files which may be owned by different users
	//   (KubeVirt uses qemu user, and file ownership varies by storage provider)
	// - Use spc_t SELinux type to bypass SELinux label checking for cross-namespace volumes
	// - Uses velero service account which has the SCC (SecurityContextConstraints) needed
	//   for privileged access on OpenShift
	runAsUser := int64(0)
	readOnlyRootFilesystem := true

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        config.Name,
			Namespace:   config.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			// Use velero service account which has the SCC needed for root access
			ServiceAccountName: "velero",
			RestartPolicy:      corev1.RestartPolicyNever,
			// Pod-level security context: run as root with spc_t SELinux type
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser: &runAsUser,
				SELinuxOptions: &corev1.SELinuxOptions{
					Type: "spc_t",
				},
			},
			Containers: []corev1.Container{
				{
					Name:            "uploader",
					Image:           config.Image,
					ImagePullPolicy: config.ImagePullPolicy,
					Command:         []string{"/manager", "upload"},
					Env:             envVars,
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "backup-data",
							MountPath: uploader.DefaultSourcePVCPath,
							ReadOnly:  true,
						},
						{
							Name:      "cloud-credentials",
							MountPath: "/credentials",
							ReadOnly:  true,
						},
					},
					SecurityContext: &corev1.SecurityContext{
						ReadOnlyRootFilesystem: &readOnlyRootFilesystem,
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "backup-data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: config.SourcePVCName,
							ReadOnly:  true,
						},
					},
				},
				{
					// Mount BSL credentials secret. The secret key (from BSL config) is mounted
					// to a fixed path "/credentials/cloud" that matches uploader.DefaultCredentialsPath.
					// This is simpler than Velero's dynamic path approach since we control both ends.
					Name: "cloud-credentials",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: config.CredentialSecretName,
							Items: []corev1.KeyToPath{
								{
									Key:  config.CredentialSecretKey,
									Path: "cloud", // Fixed filename, matches uploader.DefaultCredentialsPath
								},
							},
						},
					},
				},
			},
		},
	}

	return pod
}
