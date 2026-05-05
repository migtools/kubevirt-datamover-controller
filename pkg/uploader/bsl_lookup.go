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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	velero "github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"golang.org/x/sync/errgroup"
)

// CheckpointLookupResult holds the result of looking up the latest checkpoint from BSL.
type CheckpointLookupResult struct {
	// Found indicates whether a valid checkpoint was found in the BSL
	Found bool

	// LatestCheckpoint is the ID of the latest valid checkpoint
	LatestCheckpoint string

	// IsChainValid indicates whether the full checkpoint chain is valid
	// (all files exist and chain is unbroken from full backup to latest)
	IsChainValid bool

	// ChainLength is the number of checkpoints in the valid chain
	ChainLength int

	// Message provides a human-readable description of the lookup result
	Message string
}

// LookupLatestCheckpoint queries the BSL for existing checkpoints for a VM
// and validates the checkpoint chain. It returns the latest valid checkpoint
// that can be used for incremental backups.
//
// If no checkpoint index exists (first backup), it returns Found=false.
// If the chain is broken (missing files), it walks backward to find the
// last valid checkpoint. If no valid checkpoint can be found, it returns
// Found=false, indicating a full backup should be performed.
//
// IMPORTANT: The ObjectStore must be initialized with the same prefix used by the
// uploader (typically "<bsl-prefix>-kubevirt-datamover") so that index paths resolve
// to the same objects written during upload.
func LookupLatestCheckpoint(
	ctx context.Context, store velero.ObjectStore, bucket, vmNamespace, vmName string,
) (*CheckpointLookupResult, error) {
	// Check for context cancellation before starting
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled before checkpoint lookup: %w", err)
	}

	// Build the index path
	indexPath := fmt.Sprintf("checkpoints/%s/%s/index.json", vmNamespace, vmName)

	// Check if index exists
	exists, err := store.ObjectExists(bucket, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check VM index existence: %w", err)
	}

	if !exists {
		return &CheckpointLookupResult{
			Found:   false,
			Message: "no checkpoint index found (first backup for this VM)",
		}, nil
	}

	// Load the VM index
	vmIndex, err := loadVMIndex(store, bucket, indexPath)
	if err != nil {
		return &CheckpointLookupResult{
			Found:   false,
			Message: fmt.Sprintf("failed to load VM index, falling back to full backup: %v", err),
		}, nil
	}

	if len(vmIndex.Checkpoints) == 0 {
		return &CheckpointLookupResult{
			Found:   false,
			Message: "checkpoint index exists but contains no checkpoints",
		}, nil
	}

	// Get the latest checkpoint (last in the list)
	latestCP := vmIndex.Checkpoints[len(vmIndex.Checkpoints)-1]

	// Validate the checkpoint chain starting from the latest
	result, err := validateCheckpointChain(ctx, store, bucket, vmIndex.Checkpoints, latestCP.ID)
	if err != nil {
		return &CheckpointLookupResult{
			Found:   false,
			Message: fmt.Sprintf("checkpoint chain validation failed, falling back to full backup: %v", err),
		}, nil
	}

	return result, nil
}

// loadVMIndex reads and parses the VM checkpoint index from BSL.
func loadVMIndex(store velero.ObjectStore, bucket, indexPath string) (*VMIndex, error) {
	data, err := GetObjectBytes(store, bucket, indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read VM index at %s: %w", indexPath, err)
	}

	var vmIndex VMIndex
	if err := json.Unmarshal(data, &vmIndex); err != nil {
		return nil, fmt.Errorf("failed to parse VM index: %w", err)
	}

	return &vmIndex, nil
}

// validateCheckpointChain validates the checkpoint chain starting from the target
// checkpoint and walking back to the full backup. It verifies that all qcow2 files
// in the chain exist in the BSL.
//
// If the chain is valid, returns the target checkpoint as the latest valid one.
// If files are missing somewhere in the chain, it iteratively tries shorter chains
// by falling back to earlier checkpoints until a valid chain is found or all
// options are exhausted.
func validateCheckpointChain(
	ctx context.Context, store velero.ObjectStore, bucket string,
	checkpoints []CheckpointEntry, targetID string,
) (*CheckpointLookupResult, error) {
	currentTargetID := targetID

	for {
		// Check for context cancellation between iterations
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("context cancelled during chain validation: %w", err)
		}

		// Build the full chain from current target back to root
		chain := buildCheckpointChain(checkpoints, currentTargetID)
		if len(chain) == 0 {
			return &CheckpointLookupResult{
				Found:   false,
				Message: fmt.Sprintf("checkpoint %q not found in index", currentTargetID),
			}, nil
		}

		// Validate that the chain starts with a full backup
		if strings.ToLower(chain[0].Type) != BackupTypeFull {
			return &CheckpointLookupResult{
				Found: false,
				Message: fmt.Sprintf(
					"checkpoint chain does not start with a full backup (starts with %q type)",
					chain[0].Type),
			}, nil
		}

		// Validate all files in the chain exist
		// Walk from the latest (end) toward the full backup (start)
		// If we find a broken link, try the checkpoint before it
		brokenAt := -1
		var brokenReason string
		for i := len(chain) - 1; i >= 0; i-- {
			cp := chain[i]
			if err := validateCheckpointFiles(ctx, store, bucket, cp.Files); err != nil {
				brokenAt = i
				brokenReason = err.Error()
				break
			}
		}

		if brokenAt == -1 {
			// All files in the chain are valid.
			// IsChainValid is true only if this is the original target (no fallback
			// to a shorter chain). When a fallback occurred, the found chain is
			// valid but the overall checkpoint chain is considered broken because
			// the latest checkpoint in the index could not be used.
			isOriginalTarget := currentTargetID == targetID
			msg := fmt.Sprintf("valid checkpoint chain found: %d checkpoints, latest=%s",
				len(chain), currentTargetID)
			if !isOriginalTarget {
				msg = fmt.Sprintf("checkpoint chain fell back from %s to %s (%d checkpoints valid)",
					targetID, currentTargetID, len(chain))
			}
			return &CheckpointLookupResult{
				Found:            true,
				LatestCheckpoint: currentTargetID,
				IsChainValid:     isOriginalTarget,
				ChainLength:      len(chain),
				Message:          msg,
			}, nil
		}

		if brokenAt == 0 {
			// Even the full backup has missing files - no valid chain possible
			return &CheckpointLookupResult{
				Found: false,
				Message: fmt.Sprintf("full backup checkpoint %q is broken: %s",
					chain[0].ID, brokenReason),
			}, nil
		}

		// Try the previous checkpoint in the chain
		currentTargetID = chain[brokenAt-1].ID
	}
}

// validateCheckpointFiles checks that all qcow2 files in a checkpoint exist in BSL.
// The ObjectPath in each CheckpointFile is expected to be a relative key (without
// the store prefix), matching how the uploader writes them via the same ObjectStore.
// File existence checks are performed in parallel using errgroup for efficiency
// when VMs have multiple disks.
func validateCheckpointFiles(
	_ context.Context, store velero.ObjectStore, bucket string, files []CheckpointFile,
) error {
	var g errgroup.Group

	for _, f := range files {
		file := f
		g.Go(func() error {
			if file.ObjectPath == "" {
				return fmt.Errorf("checkpoint file has empty object path (disk: %s)", file.DiskName)
			}

			exists, err := store.ObjectExists(bucket, file.ObjectPath)
			if err != nil {
				return fmt.Errorf("failed to check file %s: %w", file.ObjectPath, err)
			}
			if !exists {
				return fmt.Errorf("file %s not found in BSL", file.ObjectPath)
			}
			return nil
		})
	}

	return g.Wait()
}
