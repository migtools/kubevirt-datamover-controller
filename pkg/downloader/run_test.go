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
		if cfg.BSLBucket != "test-bucket" {
			t.Errorf("BSLBucket = %q, want %q", cfg.BSLBucket, "test-bucket")
		}
		if cfg.VMName != "test-vm" {
			t.Errorf("VMName = %q, want %q", cfg.VMName, "test-vm")
		}
		if cfg.VMNamespace != "test-ns" {
			t.Errorf("VMNamespace = %q, want %q", cfg.VMNamespace, "test-ns")
		}
		if cfg.VeleroBackupName != "test-backup" {
			t.Errorf("VeleroBackupName = %q, want %q", cfg.VeleroBackupName, "test-backup")
		}
		if cfg.TargetVolume != "disk1" {
			t.Errorf("TargetVolume = %q, want %q", cfg.TargetVolume, "disk1")
		}
		if cfg.TargetPath != DefaultTargetPath {
			t.Errorf("TargetPath = %q, want default %q", cfg.TargetPath, DefaultTargetPath)
		}
		if cfg.ScratchPath != DefaultScratchPath {
			t.Errorf("ScratchPath = %q, want default %q", cfg.ScratchPath, DefaultScratchPath)
		}
		if cfg.CredentialsFile != common.DefaultCredentialsPath {
			t.Errorf("CredentialsFile = %q, want default %q", cfg.CredentialsFile, common.DefaultCredentialsPath)
		}
		if cfg.DataDownloadName != "test-datadownload" {
			t.Errorf("DataDownloadName = %q, want %q", cfg.DataDownloadName, "test-datadownload")
		}
		if cfg.DataDownloadUID != "test-uid" {
			t.Errorf("DataDownloadUID = %q, want %q", cfg.DataDownloadUID, "test-uid")
		}
		if cfg.BSLProvider != "aws" {
			t.Errorf("BSLProvider = %q, want %q", cfg.BSLProvider, "aws")
		}
		if cfg.BSLPrefix != "velero-kubevirt-datamover" {
			t.Errorf("BSLPrefix = %q, want %q", cfg.BSLPrefix, "velero-kubevirt-datamover")
		}
		if cfg.BSLRegion != "us-east-1" {
			t.Errorf("BSLRegion = %q, want %q", cfg.BSLRegion, "us-east-1")
		}
		if cfg.BSLS3URL != "https://s3.example.com" {
			t.Errorf("BSLS3URL = %q, want %q", cfg.BSLS3URL, "https://s3.example.com")
		}
		if !cfg.BSLS3ForcePathStyle {
			t.Error("BSLS3ForcePathStyle = false, want true")
		}
		if !cfg.BSLInsecureSkipTLSVerify {
			t.Error("BSLInsecureSkipTLSVerify = false, want true")
		}
		if cfg.BSLCACert != "test-ca-cert" {
			t.Errorf("BSLCACert = %q, want %q", cfg.BSLCACert, "test-ca-cert")
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
