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
	"testing"

	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

func TestResolveCheckpointFiles(t *testing.T) {
	vmIndex := uploader.VMIndex{
		VMName:    "fedora-test",
		Namespace: "default",
		Checkpoints: []uploader.CheckpointEntry{
			{
				ID:   "checkpoint-1",
				Type: "Full",
				Files: []uploader.CheckpointFile{
					{
						Filename: "disk1-full.qcow2", DiskName: "disk1",
						ObjectPath: "checkpoints/default/fedora-test/checkpoint-1/disk1-full.qcow2",
					},
					{
						Filename: "disk2-full.qcow2", DiskName: "disk2",
						ObjectPath: "checkpoints/default/fedora-test/checkpoint-1/disk2-full.qcow2",
					},
				},
			},
			{
				ID:     "checkpoint-2",
				Type:   "Incremental",
				Parent: "checkpoint-1",
				Files: []uploader.CheckpointFile{
					{
						Filename: "disk1-inc.qcow2", DiskName: "disk1",
						ObjectPath: "checkpoints/default/fedora-test/checkpoint-2/disk1-inc.qcow2",
					},
				},
			},
		},
	}

	t.Run("resolves full chain in order", func(t *testing.T) {
		files, err := ResolveCheckpointFiles(
			"default", "fedora-test", []string{"checkpoint-1", "checkpoint-2"}, vmIndex, "disk1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(files) != 2 {
			t.Fatalf("expected 2 files, got %d", len(files))
		}
		if files[0].Filename != "disk1-full.qcow2" {
			t.Errorf("expected first file disk1-full.qcow2, got %s", files[0].Filename)
		}
		if files[1].Filename != "disk1-inc.qcow2" {
			t.Errorf("expected second file disk1-inc.qcow2, got %s", files[1].Filename)
		}
	})

	t.Run("filters by target volume", func(t *testing.T) {
		files, err := ResolveCheckpointFiles("default", "fedora-test", []string{"checkpoint-1"}, vmIndex, "disk2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(files) != 1 || files[0].Filename != "disk2-full.qcow2" {
			t.Fatalf("expected disk2-full.qcow2, got %+v", files)
		}
	})

	t.Run("errors on empty chain", func(t *testing.T) {
		_, err := ResolveCheckpointFiles("default", "fedora-test", nil, vmIndex, "disk1")
		if err == nil {
			t.Fatal("expected error for an empty checkpoint chain")
		}
	})

	t.Run("errors on missing checkpoint ID", func(t *testing.T) {
		_, err := ResolveCheckpointFiles(
			"default", "fedora-test", []string{"checkpoint-1", "does-not-exist"}, vmIndex, "disk1")
		if err == nil {
			t.Fatal("expected error for missing checkpoint ID")
		}
	})

	t.Run("errors on missing disk match", func(t *testing.T) {
		_, err := ResolveCheckpointFiles("default", "fedora-test", []string{"checkpoint-1", "checkpoint-2"}, vmIndex, "disk2")
		if err == nil {
			t.Fatal("expected error when checkpoint has no file for target volume")
		}
	})

	t.Run("errors on duplicate checkpoint ID in chain", func(t *testing.T) {
		_, err := ResolveCheckpointFiles("default", "fedora-test", []string{"checkpoint-1", "checkpoint-1"}, vmIndex, "disk1")
		if err == nil {
			t.Fatal("expected error when a checkpoint ID appears more than once in the chain")
		}
	})

	t.Run("errors on duplicate checkpoint ID in VM index", func(t *testing.T) {
		dupIndex := uploader.VMIndex{
			VMName:    "fedora-test",
			Namespace: "default",
			Checkpoints: []uploader.CheckpointEntry{
				{ID: "checkpoint-1", Files: []uploader.CheckpointFile{{Filename: "a.qcow2", DiskName: "disk1"}}},
				{ID: "checkpoint-1", Files: []uploader.CheckpointFile{{Filename: "b.qcow2", DiskName: "disk1"}}},
			},
		}
		_, err := ResolveCheckpointFiles("default", "fedora-test", []string{"checkpoint-1"}, dupIndex, "disk1")
		if err == nil {
			t.Fatal("expected error when the VM index has a duplicate checkpoint ID")
		}
	})

	t.Run("errors on multiple files matching target volume in one checkpoint", func(t *testing.T) {
		dupDiskIndex := uploader.VMIndex{
			VMName:    "fedora-test",
			Namespace: "default",
			Checkpoints: []uploader.CheckpointEntry{
				{
					ID:   "checkpoint-1",
					Type: "Full",
					Files: []uploader.CheckpointFile{
						{Filename: "a.qcow2", DiskName: "disk1"},
						{Filename: "b.qcow2", DiskName: "disk1"},
					},
				},
			},
		}
		_, err := ResolveCheckpointFiles("default", "fedora-test", []string{"checkpoint-1"}, dupDiskIndex, "disk1")
		if err == nil {
			t.Fatal("expected error when a checkpoint has multiple files for the same target volume")
		}
	})

	t.Run("errors on reversed chain", func(t *testing.T) {
		_, err := ResolveCheckpointFiles("default", "fedora-test", []string{"checkpoint-2", "checkpoint-1"}, vmIndex, "disk1")
		if err == nil {
			t.Fatal("expected error when the chain starts with an incremental instead of a full backup")
		}
	})

	t.Run("errors on disconnected chain", func(t *testing.T) {
		disconnectedIndex := uploader.VMIndex{
			VMName:    "fedora-test",
			Namespace: "default",
			Checkpoints: []uploader.CheckpointEntry{
				{
					ID:   "checkpoint-1",
					Type: "Full",
					Files: []uploader.CheckpointFile{
						{
							Filename: "disk1-full.qcow2", DiskName: "disk1",
							ObjectPath: "checkpoints/default/fedora-test/checkpoint-1/disk1-full.qcow2",
						},
					},
				},
				{
					ID:     "checkpoint-3",
					Type:   "Incremental",
					Parent: "checkpoint-99", // does not chain onto checkpoint-1
					Files: []uploader.CheckpointFile{
						{Filename: "disk1-inc.qcow2", DiskName: "disk1"},
					},
				},
			},
		}
		_, err := ResolveCheckpointFiles(
			"default", "fedora-test", []string{"checkpoint-1", "checkpoint-3"}, disconnectedIndex, "disk1")
		if err == nil {
			t.Fatal("expected error when an incremental's parent does not match the preceding chain entry")
		}
	})

	t.Run("errors when a later checkpoint has the correct parent but is not marked incremental", func(t *testing.T) {
		wrongTypeIndex := uploader.VMIndex{
			VMName:    "fedora-test",
			Namespace: "default",
			Checkpoints: []uploader.CheckpointEntry{
				{
					ID:   "checkpoint-1",
					Type: "Full",
					Files: []uploader.CheckpointFile{
						{
							Filename: "disk1-full.qcow2", DiskName: "disk1",
							ObjectPath: "checkpoints/default/fedora-test/checkpoint-1/disk1-full.qcow2",
						},
					},
				},
				{
					ID:     "checkpoint-2",
					Type:   "Full", // parent correctly chains onto checkpoint-1, but type is wrong
					Parent: "checkpoint-1",
					Files: []uploader.CheckpointFile{
						{Filename: "disk1-inc.qcow2", DiskName: "disk1"},
					},
				},
			},
		}
		_, err := ResolveCheckpointFiles(
			"default", "fedora-test", []string{"checkpoint-1", "checkpoint-2"}, wrongTypeIndex, "disk1")
		if err == nil {
			t.Fatal("expected error when a non-first chain entry has a correct parent but is not marked incremental")
		}
	})

	t.Run("errors when a file's object path escapes this VM's checkpoint prefix", func(t *testing.T) {
		tamperedIndex := uploader.VMIndex{
			VMName:    "fedora-test",
			Namespace: "default",
			Checkpoints: []uploader.CheckpointEntry{
				{
					ID:   "checkpoint-1",
					Type: "Full",
					Files: []uploader.CheckpointFile{
						{
							Filename: "disk1-full.qcow2", DiskName: "disk1",
							// Points at a different VM's checkpoint data instead
							// of this VM's own prefix.
							ObjectPath: "checkpoints/default/other-vm/checkpoint-1/disk1-full.qcow2",
						},
					},
				},
			},
		}
		_, err := ResolveCheckpointFiles("default", "fedora-test", []string{"checkpoint-1"}, tamperedIndex, "disk1")
		if err == nil {
			t.Fatal("expected error when a file's object path does not match this VM's own checkpoint prefix")
		}
	})
}
