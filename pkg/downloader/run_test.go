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

package downloader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/migtools/kubevirt-datamover-controller/pkg/common"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv(common.EnvBSLBucket, "test-bucket")
	t.Setenv(EnvVMName, "test-vm")
	t.Setenv(EnvVMNamespace, "test-ns")
	t.Setenv(EnvVeleroBackupName, "test-backup")
	t.Setenv(EnvTargetVolume, "disk1")
	t.Setenv(EnvTargetPath, "")
	t.Setenv(EnvTargetIsBlockDevice, "")
	t.Setenv(EnvScratchPath, "")
	t.Setenv(common.EnvCredentialsFile, "")
	t.Setenv(EnvDataDownloadName, "")
	t.Setenv(EnvDataDownloadUID, "")
	t.Setenv(common.EnvBSLProvider, "aws")
	t.Setenv(common.EnvBSLPrefix, "")
	t.Setenv(common.EnvBSLRegion, "")
	t.Setenv(common.EnvBSLS3URL, "")
	t.Setenv(common.EnvBSLS3ForcePathStyle, "")
	t.Setenv(common.EnvBSLInsecureSkipTLSVerify, "")
	t.Setenv(common.EnvBSLCACert, "")
	t.Setenv(common.EnvBSLServiceAccount, "")
	t.Setenv(common.EnvBSLResourceGroup, "")
	t.Setenv(common.EnvBSLStorageAccount, "")
	t.Setenv(common.EnvBSLStorageAccountKeyEnvVar, "")
	t.Setenv(common.EnvBSLStorageAccountURI, "")
	t.Setenv(common.EnvBSLSubscriptionID, "")
	t.Setenv(common.EnvBSLUseAAD, "")
	t.Setenv(common.EnvBSLActiveDirectoryAuthorityURI, "")
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Run("valid config with all required fields", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvDataDownloadName, "test-datadownload")
		t.Setenv(EnvDataDownloadUID, "test-uid")
		t.Setenv(common.EnvBSLProvider, "aws")
		t.Setenv(common.EnvBSLPrefix, "velero-kubevirt-datamover")
		t.Setenv(common.EnvBSLRegion, "us-east-1")
		t.Setenv(common.EnvBSLS3URL, "https://s3.example.com")
		t.Setenv(common.EnvBSLS3ForcePathStyle, "true")
		t.Setenv(common.EnvBSLInsecureSkipTLSVerify, "true")
		t.Setenv(common.EnvBSLCACert, "test-ca-cert")

		cfg, err := LoadConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		fields := []struct {
			name string
			got  string
			want string
		}{
			{"BSLBucket", cfg.BSLBucket, "test-bucket"},
			{"VMName", cfg.VMName, "test-vm"},
			{"VMNamespace", cfg.VMNamespace, "test-ns"},
			{"VeleroBackupName", cfg.VeleroBackupName, "test-backup"},
			{"TargetVolume", cfg.TargetVolume, "disk1"},
			{"TargetPath", cfg.TargetPath, DefaultTargetPath},
			{"ScratchPath", cfg.ScratchPath, DefaultScratchPath},
			{"CredentialsFile", cfg.CredentialsFile, common.DefaultCredentialsPath},
			{"DataDownloadName", cfg.DataDownloadName, "test-datadownload"},
			{"DataDownloadUID", cfg.DataDownloadUID, "test-uid"},
			{"BSLProvider", cfg.BSLProvider, "aws"},
			{"BSLPrefix", cfg.BSLPrefix, "velero-kubevirt-datamover"},
			{"BSLRegion", cfg.BSLRegion, "us-east-1"},
			{"BSLS3URL", cfg.BSLS3URL, "https://s3.example.com"},
			{"BSLCACert", cfg.BSLCACert, "test-ca-cert"},
		}
		for _, f := range fields {
			if f.got != f.want {
				t.Errorf("%s = %q, want %q", f.name, f.got, f.want)
			}
		}
		if !cfg.BSLS3ForcePathStyle {
			t.Error("BSLS3ForcePathStyle = false, want true")
		}
		if !cfg.BSLInsecureSkipTLSVerify {
			t.Error("BSLInsecureSkipTLSVerify = false, want true")
		}
	})

	t.Run("GCP and Azure BSL fields are mapped from env", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(common.EnvBSLServiceAccount, "test-service-account")
		t.Setenv(common.EnvBSLResourceGroup, "test-resource-group")
		t.Setenv(common.EnvBSLStorageAccount, "test-storage-account")
		t.Setenv(common.EnvBSLStorageAccountKeyEnvVar, "AZURE_STORAGE_KEY")
		t.Setenv(common.EnvBSLStorageAccountURI, "https://test.blob.core.windows.net")
		t.Setenv(common.EnvBSLSubscriptionID, "test-subscription-id")
		t.Setenv(common.EnvBSLUseAAD, "true")
		t.Setenv(common.EnvBSLActiveDirectoryAuthorityURI, "https://login.microsoftonline.com")

		cfg, err := LoadConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		fields := []struct {
			name string
			got  string
			want string
		}{
			{"BSLServiceAccount", cfg.BSLServiceAccount, "test-service-account"},
			{"BSLResourceGroup", cfg.BSLResourceGroup, "test-resource-group"},
			{"BSLStorageAccount", cfg.BSLStorageAccount, "test-storage-account"},
			{"BSLStorageAccountKeyEnvVar", cfg.BSLStorageAccountKeyEnvVar, "AZURE_STORAGE_KEY"},
			{"BSLStorageAccountURI", cfg.BSLStorageAccountURI, "https://test.blob.core.windows.net"},
			{"BSLSubscriptionID", cfg.BSLSubscriptionID, "test-subscription-id"},
			{"BSLActiveDirectoryAuthorityURI", cfg.BSLActiveDirectoryAuthorityURI, "https://login.microsoftonline.com"},
		}
		for _, f := range fields {
			if f.got != f.want {
				t.Errorf("%s = %q, want %q", f.name, f.got, f.want)
			}
		}
		if !cfg.BSLUseAAD {
			t.Error("BSLUseAAD = false, want true")
		}
	})

	t.Run("explicit target and scratch paths override defaults", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvTargetPath, "/custom-target/disk.raw")
		t.Setenv(EnvScratchPath, "/custom-scratch")

		cfg, err := LoadConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.TargetPath != "/custom-target/disk.raw" {
			t.Errorf("TargetPath = %q, want %q", cfg.TargetPath, "/custom-target/disk.raw")
		}
		if cfg.ScratchPath != "/custom-scratch" {
			t.Errorf("ScratchPath = %q, want %q", cfg.ScratchPath, "/custom-scratch")
		}
	})

	t.Run("relative target path returns error", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvTargetPath, "relative-target/disk.raw")

		_, err := LoadConfigFromEnv()
		if err == nil {
			t.Fatal("expected error for a relative target path")
		}
	})

	t.Run("target path resolving to the filesystem root returns error", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvTargetPath, "/disk.raw")

		_, err := LoadConfigFromEnv()
		if err == nil {
			t.Fatal("expected error for a target path whose directory is the filesystem root")
		}
	})

	t.Run("relative scratch path returns error", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvScratchPath, "relative-scratch")

		_, err := LoadConfigFromEnv()
		if err == nil {
			t.Fatal("expected error for a relative scratch path")
		}
	})

	t.Run("scratch path resolving to the filesystem root returns error", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvScratchPath, "/")

		_, err := LoadConfigFromEnv()
		if err == nil {
			t.Fatal("expected error for a scratch path of the filesystem root")
		}
	})

	t.Run("block device target: TargetPath required, no default, filesystem-root check skipped", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvTargetIsBlockDevice, "true")
		t.Setenv(EnvTargetPath, "/dev/restore-output")

		cfg, err := LoadConfigFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.TargetIsBlockDevice {
			t.Error("expected TargetIsBlockDevice = true")
		}
		if cfg.TargetPath != "/dev/restore-output" {
			t.Errorf("TargetPath = %q, want %q", cfg.TargetPath, "/dev/restore-output")
		}
	})

	t.Run("block device target with no TargetPath returns error (no default device path)", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvTargetIsBlockDevice, "true")

		_, err := LoadConfigFromEnv()
		if err == nil {
			t.Fatal("expected error when TargetIsBlockDevice is true but TargetPath is unset")
		}
	})

	t.Run("block device target with a relative TargetPath returns error", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv(EnvTargetIsBlockDevice, "true")
		t.Setenv(EnvTargetPath, "relative-device")

		_, err := LoadConfigFromEnv()
		if err == nil {
			t.Fatal("expected error for a relative block device path")
		}
	})

	requiredVars := []string{
		common.EnvBSLProvider, common.EnvBSLBucket,
		EnvVMName, EnvVMNamespace, EnvVeleroBackupName, EnvTargetVolume,
	}
	for _, missing := range requiredVars {
		t.Run("missing "+missing+" returns error", func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(missing, "")

			_, err := LoadConfigFromEnv()
			if err == nil {
				t.Fatalf("expected error when %s is missing", missing)
			}
		})
	}
}

func TestPrepareTargetDir(t *testing.T) {
	t.Run("creates a new directory with owner-only permissions", func(t *testing.T) {
		base := t.TempDir()
		targetPath := filepath.Join(base, "nested", "disk.raw")

		if err := prepareTargetDir(targetPath); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		info, err := os.Stat(filepath.Dir(targetPath))
		if err != nil {
			t.Fatalf("failed to stat target dir: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("target dir perm = %o, want %o", perm, 0o700)
		}
	})

	t.Run("hardens a pre-existing permissive directory", func(t *testing.T) {
		base := t.TempDir()
		dir := filepath.Join(base, "preexisting")
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("failed to seed pre-existing dir: %v", err)
		}

		if err := prepareTargetDir(filepath.Join(dir, "disk.raw")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("failed to stat target dir: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("target dir perm = %o, want %o after hardening a pre-existing 0755 dir", perm, 0o700)
		}
	})

	t.Run("rejects a relative target path", func(t *testing.T) {
		if err := prepareTargetDir("relative/disk.raw"); err == nil {
			t.Fatal("expected error for a relative target path")
		}
	})

	t.Run("rejects a target path resolving to the filesystem root", func(t *testing.T) {
		if err := prepareTargetDir("/disk.raw"); err == nil {
			t.Fatal("expected error for a target path whose directory is the filesystem root")
		}
	})
}

func TestPrepareDir(t *testing.T) {
	t.Run("creates a new directory with owner-only permissions", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nested", "scratch")

		if err := prepareDir(dir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("failed to stat dir: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("dir perm = %o, want %o", perm, 0o700)
		}
	})

	t.Run("hardens a pre-existing permissive directory", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "preexisting")
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("failed to seed pre-existing dir: %v", err)
		}

		if err := prepareDir(dir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("failed to stat dir: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o700 {
			t.Errorf("dir perm = %o, want %o after hardening a pre-existing 0755 dir", perm, 0o700)
		}
	})

	t.Run("rejects the filesystem root", func(t *testing.T) {
		if err := prepareDir("/"); err == nil {
			t.Fatal("expected error for the filesystem root")
		}
	})
}
