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
	"fmt"
	"strings"

	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

// resolveCheckpointFiles walks the (already-ordered) checkpoint chain and
// returns, for each checkpoint, the single CheckpointFile matching
// targetVolume. The chain is assumed to be full-backup-first,
// incrementals-after, as produced by the uploader at backup time.
//
// vmNamespace/vmName come from the trusted DataDownload/env config, not from
// S3-sourced metadata, and are used to verify each matched file's ObjectPath
// actually falls under this VM's own checkpoint prefix (see the ObjectPath
// check below) before it's ever handed to downloadCheckpointFiles.
func resolveCheckpointFiles(
	vmNamespace, vmName string, chain []string, vmIndex uploader.VMIndex, targetVolume string,
) ([]uploader.CheckpointFile, error) {
	if len(chain) == 0 {
		return nil, fmt.Errorf("backup checkpoint chain is empty")
	}

	entryByID := make(map[string]uploader.CheckpointEntry, len(vmIndex.Checkpoints))
	for _, entry := range vmIndex.Checkpoints {
		if _, exists := entryByID[entry.ID]; exists {
			return nil, fmt.Errorf("duplicate checkpoint %q in VM index", entry.ID)
		}
		entryByID[entry.ID] = entry
	}

	seen := make(map[string]bool, len(chain))
	files := make([]uploader.CheckpointFile, 0, len(chain))
	for pos, id := range chain {
		if seen[id] {
			return nil, fmt.Errorf("checkpoint %q appears more than once in backup chain", id)
		}
		seen[id] = true

		entry, ok := entryByID[id]
		if !ok {
			return nil, fmt.Errorf("checkpoint %q referenced in backup chain not found in VM index", id)
		}

		// The chain is claimed to be full-backup-first, incrementals-after
		// (see doc comment), but that claim comes from S3-sourced metadata.
		// Verify each entry's own recorded Type/Parent actually match that
		// shape before trusting the chain order for reconstruction, since
		// rebaseChain treats this order as the literal qcow2 backing chain.
		if pos == 0 {
			if !strings.EqualFold(entry.Type, uploader.BackupTypeFull) {
				return nil, fmt.Errorf("checkpoint %q is first in the chain but is not a full backup (type %q)", id, entry.Type)
			}
			if entry.Parent != "" {
				return nil, fmt.Errorf("checkpoint %q is first in the chain but has a parent %q", id, entry.Parent)
			}
		} else {
			if !strings.EqualFold(entry.Type, uploader.BackupTypeIncremental) {
				return nil, fmt.Errorf("checkpoint %q is not first in the chain but is not incremental (type %q)", id, entry.Type)
			}
			if entry.Parent != chain[pos-1] {
				return nil, fmt.Errorf("checkpoint %q has parent %q, want preceding chain entry %q", id, entry.Parent, chain[pos-1])
			}
		}

		var match *uploader.CheckpointFile
		for i := range entry.Files {
			if entry.Files[i].DiskName == targetVolume {
				if match != nil {
					return nil, fmt.Errorf("checkpoint %q has multiple files for target volume %q", id, targetVolume)
				}
				match = &entry.Files[i]
			}
		}
		if match == nil {
			return nil, fmt.Errorf("checkpoint %q has no file for target volume %q", id, targetVolume)
		}
		wantObjectPath := uploader.GetQCOWPath(vmNamespace, vmName, id, match.Filename)
		if match.ObjectPath != wantObjectPath {
			return nil, fmt.Errorf("checkpoint %q file %q has object path %q, want %q under this VM's checkpoint prefix",
				id, match.Filename, match.ObjectPath, wantObjectPath)
		}
		files = append(files, *match)
	}

	return files, nil
}
