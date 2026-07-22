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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
)

// runQemuImg is a package var so tests can override it without shelling out
// to a real qemu-img binary, mirroring the newKubeClient injection pattern
// used in pkg/uploader/run.go. Output is mirrored live to the process's own
// stdout/stderr (visible via `kubectl logs`) as well as captured into the
// returned strings, so a long-running invocation like `convert -p` gives
// operators visible progress instead of going silent until it exits.
var runQemuImg = func(ctx context.Context, args ...string) (stdout, stderr string, err error) {
	cmd := exec.CommandContext(ctx, "qemu-img", args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = io.MultiWriter(&outBuf, os.Stdout)
	cmd.Stderr = io.MultiWriter(&errBuf, os.Stderr)
	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

// rebaseChain repoints each qcow2 file's backing-file reference (after the
// first) onto the local path of its predecessor in the chain. The backing
// path recorded at backup time refers to a path on the backup pod's
// filesystem that doesn't exist on the restore pod, so each file must be
// rebased before the chain can be flattened.
//
// Before rebasing, every file in the chain is checked for an external data
// file, and the base (localPaths[0], expected to be the full backup) is also
// checked for a backing file of its own:
//
//   - An external data file is checked on every layer, not just the base:
//     qemu-img reads each layer's own cluster data from its data-file if one
//     is declared, and rebase neither clears nor validates that field, so a
//     tampered incremental could smuggle in data from an arbitrary local
//     path just as easily as a tampered base could.
//   - A backing file is only checked on the base: incrementals are expected
//     to already have one (that's what makes them incremental), and
//     rebaseChain's own rebase call below overwrites it with the correct
//     local sibling path regardless of what it was before.
func rebaseChain(ctx context.Context, localPaths []string) error {
	if len(localPaths) == 0 {
		return nil
	}
	for i, path := range localPaths {
		info, err := inspectQcow2(ctx, path)
		if err != nil {
			return err
		}
		if info.FormatSpecific.Data.DataFile != "" {
			return fmt.Errorf("checkpoint %q unexpectedly has an external data file %q; refusing to restore",
				path, info.FormatSpecific.Data.DataFile)
		}
		if i == 0 && info.BackingFilename != "" {
			return fmt.Errorf("checkpoint base %q unexpectedly has a backing file %q; refusing to restore",
				path, info.BackingFilename)
		}
		if i > 0 && info.BackingFilename == "" {
			return fmt.Errorf("checkpoint increment %q unexpectedly has no backing file; refusing to restore", path)
		}
	}
	for i := 1; i < len(localPaths); i++ {
		_, stderr, err := runQemuImg(ctx, "rebase", "-u", "-f", "qcow2", "-F", "qcow2", "-b", localPaths[i-1], localPaths[i])
		if err != nil {
			return fmt.Errorf("failed to rebase %q onto %q: %w (%s)", localPaths[i], localPaths[i-1], err, stderr)
		}
	}
	return nil
}

// qcow2Info is the subset of `qemu-img info --output=json` fields needed to
// detect a backing file or an external data file.
type qcow2Info struct {
	BackingFilename string `json:"backing-filename"`
	FormatSpecific  struct {
		Data struct {
			DataFile string `json:"data-file"`
		} `json:"data"`
	} `json:"format-specific"`
}

func inspectQcow2(ctx context.Context, path string) (qcow2Info, error) {
	stdout, stderr, err := runQemuImg(ctx, "info", "-f", "qcow2", "--output=json", path)
	if err != nil {
		return qcow2Info{}, fmt.Errorf("failed to inspect %q: %w (%s)", path, err, stderr)
	}

	var info qcow2Info
	if err := json.Unmarshal([]byte(stdout), &info); err != nil {
		return qcow2Info{}, fmt.Errorf("failed to parse qemu-img info for %q: %w", path, err)
	}
	return info, nil
}

// flattenToRaw converts the tip of the (now-rebased) backing chain into a
// single flat raw disk image. qemu-img convert transparently follows the
// backing chain, so this handles both a lone full backup (chain length 1)
// and a full-plus-incrementals chain uniformly.
func flattenToRaw(ctx context.Context, chainTipPath, outputPath string) error {
	// Pre-create the output file with restrictive permissions: qemu-img
	// convert writes into an existing file in place rather than recreating
	// it, so this avoids a window where the restored VM disk briefly exists
	// with the process's default (more permissive) umask. O_TRUNC and an
	// explicit Chmod make this correct even if outputPath already existed
	// with different content or permissions — the O_CREATE mode argument
	// alone is only applied when the file doesn't already exist.
	f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("failed to pre-create raw image %q: %w", outputPath, err)
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(outputPath)
		return fmt.Errorf("failed to pre-create raw image %q: %w", outputPath, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(outputPath)
		return fmt.Errorf("failed to pre-create raw image %q: %w", outputPath, err)
	}

	_, stderr, err := runQemuImg(ctx, "convert", "-p", "-f", "qcow2", "-O", "raw", chainTipPath, outputPath)
	if err != nil {
		_ = os.Remove(outputPath)
		return fmt.Errorf("failed to flatten %q to raw %q: %w (%s)", chainTipPath, outputPath, err, stderr)
	}
	// Kept as defense in depth in case convert ever recreates the file
	// instead of writing in place.
	if err := os.Chmod(outputPath, 0o600); err != nil {
		_ = os.Remove(outputPath)
		return fmt.Errorf("failed to restrict permissions on raw image %q: %w", outputPath, err)
	}
	return nil
}
