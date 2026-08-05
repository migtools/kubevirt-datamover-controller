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
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func withFakeQemuImg(t *testing.T, fn func(ctx context.Context, args ...string) (string, string, error)) {
	t.Helper()
	original := runQemuImg
	runQemuImg = fn
	t.Cleanup(func() { runQemuImg = original })
}

const (
	qemuImgInfoVerb = "info"
	fakeBoomErr     = "boom"

	// fakeInfoNoBackingFile mimics `qemu-img info --output=json` for a
	// standalone image (no backing file).
	fakeInfoNoBackingFile = `{"filename":"x","format":"qcow2"}`

	// fakeInfoWithBackingFile mimics `qemu-img info --output=json` for an
	// incremental image that already has some backing file recorded (the
	// path itself is irrelevant here since rebase overwrites it).
	fakeInfoWithBackingFile = `{"filename":"x","format":"qcow2","backing-filename":"some-predecessor.qcow2"}`
)

func TestRebaseChain(t *testing.T) {
	t.Run("no-op for a single-element chain still verifies the base has no backing file", func(t *testing.T) {
		var invocations [][]string
		withFakeQemuImg(t, func(_ context.Context, args ...string) (string, string, error) {
			invocations = append(invocations, args)
			return fakeInfoNoBackingFile, "", nil
		})
		if err := rebaseChain(context.Background(), []string{"only.qcow2"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{qemuImgInfoVerb, "-f", "qcow2", "--output=json", "only.qcow2"}
		if len(invocations) != 1 || !reflect.DeepEqual(invocations[0], want) {
			t.Errorf("invocations = %v, want exactly one info call %v", invocations, want)
		}
	})

	t.Run("rebases each file after the first onto its predecessor", func(t *testing.T) {
		var invocations [][]string
		withFakeQemuImg(t, func(_ context.Context, args ...string) (string, string, error) {
			invocations = append(invocations, args)
			if args[0] == qemuImgInfoVerb {
				if args[len(args)-1] == "full.qcow2" {
					return fakeInfoNoBackingFile, "", nil
				}
				return fakeInfoWithBackingFile, "", nil
			}
			return "", "", nil
		})

		err := rebaseChain(context.Background(), []string{"full.qcow2", "inc1.qcow2", "inc2.qcow2"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(invocations) != 5 {
			t.Fatalf("expected 3 info + 2 rebase invocations, got %d: %v", len(invocations), invocations)
		}
		wantInfo0 := []string{qemuImgInfoVerb, "-f", "qcow2", "--output=json", "full.qcow2"}
		if !reflect.DeepEqual(invocations[0], wantInfo0) {
			t.Errorf("invocation 0 = %v, want %v", invocations[0], wantInfo0)
		}
		wantInfo1 := []string{qemuImgInfoVerb, "-f", "qcow2", "--output=json", "inc1.qcow2"}
		if !reflect.DeepEqual(invocations[1], wantInfo1) {
			t.Errorf("invocation 1 = %v, want %v", invocations[1], wantInfo1)
		}
		wantInfo2 := []string{qemuImgInfoVerb, "-f", "qcow2", "--output=json", "inc2.qcow2"}
		if !reflect.DeepEqual(invocations[2], wantInfo2) {
			t.Errorf("invocation 2 = %v, want %v", invocations[2], wantInfo2)
		}
		want3 := []string{"rebase", "-u", "-f", "qcow2", "-F", "qcow2", "-b", "full.qcow2", "inc1.qcow2"}
		if !reflect.DeepEqual(invocations[3], want3) {
			t.Errorf("invocation 3 = %v, want %v", invocations[3], want3)
		}
		want4 := []string{"rebase", "-u", "-f", "qcow2", "-F", "qcow2", "-b", "inc1.qcow2", "inc2.qcow2"}
		if !reflect.DeepEqual(invocations[4], want4) {
			t.Errorf("invocation 4 = %v, want %v", invocations[4], want4)
		}
	})

	t.Run("errors when the base image has a backing file", func(t *testing.T) {
		withFakeQemuImg(t, func(_ context.Context, args ...string) (string, string, error) {
			if args[0] == qemuImgInfoVerb {
				return `{"filename":"x","format":"qcow2","backing-filename":"/etc/passwd"}`, "", nil
			}
			t.Fatalf("unexpected qemu-img invocation after a detected backing file: %v", args)
			return "", "", nil
		})
		err := rebaseChain(context.Background(), []string{"tampered.qcow2", "inc1.qcow2"})
		if err == nil {
			t.Fatal("expected error when base image unexpectedly has a backing file")
		}
	})

	t.Run("errors when the base image has an external data file", func(t *testing.T) {
		const fakeInfoWithDataFile = `{"filename":"x","format":"qcow2",` +
			`"format-specific":{"type":"qcow2","data":{"data-file":"/etc/shadow"}}}`
		withFakeQemuImg(t, func(_ context.Context, args ...string) (string, string, error) {
			if args[0] == qemuImgInfoVerb {
				return fakeInfoWithDataFile, "", nil
			}
			t.Fatalf("unexpected qemu-img invocation after a detected external data file: %v", args)
			return "", "", nil
		})
		err := rebaseChain(context.Background(), []string{"tampered.qcow2", "inc1.qcow2"})
		if err == nil {
			t.Fatal("expected error when base image unexpectedly has an external data file")
		}
	})

	t.Run("errors when an incremental (not the base) has an external data file", func(t *testing.T) {
		const fakeInfoWithDataFile = `{"filename":"x","format":"qcow2",` +
			`"format-specific":{"type":"qcow2","data":{"data-file":"/etc/shadow"}}}`
		withFakeQemuImg(t, func(_ context.Context, args ...string) (string, string, error) {
			if args[0] != qemuImgInfoVerb {
				t.Fatalf("unexpected qemu-img invocation after a detected external data file: %v", args)
				return "", "", nil
			}
			if args[len(args)-1] == "inc1.qcow2" {
				return fakeInfoWithDataFile, "", nil
			}
			return fakeInfoNoBackingFile, "", nil
		})
		err := rebaseChain(context.Background(), []string{"full.qcow2", "inc1.qcow2"})
		if err == nil {
			t.Fatal("expected error when an incremental unexpectedly has an external data file")
		}
	})

	t.Run("errors when an incremental (not the base) has no backing file", func(t *testing.T) {
		withFakeQemuImg(t, func(_ context.Context, args ...string) (string, string, error) {
			if args[0] != qemuImgInfoVerb {
				t.Fatalf("unexpected qemu-img invocation after a detected missing backing file: %v", args)
				return "", "", nil
			}
			// Every file, including the incremental, reports no backing
			// file -- suspicious for anything past the base, since that's
			// what's supposed to make it "incremental" in the first place.
			return fakeInfoNoBackingFile, "", nil
		})
		err := rebaseChain(context.Background(), []string{"full.qcow2", "inc1.qcow2"})
		if err == nil {
			t.Fatal("expected error when an incremental unexpectedly has no backing file")
		}
	})

	t.Run("propagates failure from the base info check", func(t *testing.T) {
		withFakeQemuImg(t, func(_ context.Context, _ ...string) (string, string, error) {
			return "", fakeBoomErr, errors.New("qemu-img info failed")
		})
		err := rebaseChain(context.Background(), []string{"full.qcow2", "inc1.qcow2"})
		if err == nil {
			t.Fatal("expected error from failing qemu-img info")
		}
	})

	t.Run("errors on unparseable info output", func(t *testing.T) {
		withFakeQemuImg(t, func(_ context.Context, _ ...string) (string, string, error) {
			return "not json", "", nil
		})
		err := rebaseChain(context.Background(), []string{"full.qcow2", "inc1.qcow2"})
		if err == nil {
			t.Fatal("expected error from unparseable qemu-img info output")
		}
	})

	t.Run("propagates qemu-img failure", func(t *testing.T) {
		withFakeQemuImg(t, func(_ context.Context, args ...string) (string, string, error) {
			if args[0] == qemuImgInfoVerb {
				return fakeInfoNoBackingFile, "", nil
			}
			return "", fakeBoomErr, errors.New("qemu-img failed")
		})
		err := rebaseChain(context.Background(), []string{"full.qcow2", "inc1.qcow2"})
		if err == nil {
			t.Fatal("expected error from failing qemu-img rebase")
		}
	})
}

func TestFlattenToRaw(t *testing.T) {
	t.Run("invokes convert with expected args and restricts output permissions", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "disk.raw")
		if err := os.WriteFile(outputPath, nil, 0o644); err != nil {
			t.Fatalf("failed to seed output file: %v", err)
		}

		var gotArgs []string
		withFakeQemuImg(t, func(_ context.Context, args ...string) (string, string, error) {
			gotArgs = args
			return "", "", nil
		})
		if err := flattenToRaw(context.Background(), "tip.qcow2", outputPath, false); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"convert", "-p", "-f", "qcow2", "-O", "raw", "tip.qcow2", outputPath}
		if !reflect.DeepEqual(gotArgs, want) {
			t.Errorf("convert invocation = %v, want %v", gotArgs, want)
		}

		info, err := os.Stat(outputPath)
		if err != nil {
			t.Fatalf("failed to stat output file: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Errorf("output file permissions = %o, want %o", perm, 0o600)
		}
	})

	t.Run("propagates qemu-img failure", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "disk.raw")
		called := false
		withFakeQemuImg(t, func(_ context.Context, _ ...string) (string, string, error) {
			called = true
			return "", fakeBoomErr, errors.New("qemu-img failed")
		})
		err := flattenToRaw(context.Background(), "tip.qcow2", outputPath, false)
		if err == nil {
			t.Fatal("expected error from failing qemu-img convert")
		}
		if !called {
			t.Fatal("expected qemu-img convert to be invoked")
		}
	})

	t.Run("removes a partial output file on failure", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "disk.raw")
		if err := os.WriteFile(outputPath, []byte("partial"), 0o600); err != nil {
			t.Fatalf("failed to seed partial output file: %v", err)
		}

		withFakeQemuImg(t, func(_ context.Context, _ ...string) (string, string, error) {
			return "", fakeBoomErr, errors.New("qemu-img failed")
		})
		if err := flattenToRaw(context.Background(), "tip.qcow2", outputPath, false); err == nil {
			t.Fatal("expected error from failing qemu-img convert")
		}

		if _, statErr := os.Stat(outputPath); !os.IsNotExist(statErr) {
			t.Errorf("expected partial output file to be removed, stat err = %v", statErr)
		}
	})

	t.Run("outputIsBlockDevice leaves the output path alone on failure", func(t *testing.T) {
		// Models a raw block device target: unlinking it would destroy the
		// pod's only path to that volume, unlike a disposable regular file.
		outputPath := filepath.Join(t.TempDir(), "disk.raw")
		if err := os.WriteFile(outputPath, []byte("partial"), 0o600); err != nil {
			t.Fatalf("failed to seed partial output file: %v", err)
		}

		withFakeQemuImg(t, func(_ context.Context, _ ...string) (string, string, error) {
			return "", fakeBoomErr, errors.New("qemu-img failed")
		})
		if err := flattenToRaw(context.Background(), "tip.qcow2", outputPath, true); err == nil {
			t.Fatal("expected error from failing qemu-img convert")
		}

		if _, statErr := os.Stat(outputPath); statErr != nil {
			t.Errorf("expected output path to be left alone (outputIsBlockDevice=true), stat err = %v", statErr)
		}
	})
}

// TestRebaseChainAndFlattenToRawWithRealQemuImg exercises rebaseChain and
// flattenToRaw against a real qemu-img binary instead of the mocked
// runQemuImg used by the tests above, to catch mismatches between the
// mocked argument expectations and what qemu-img actually accepts. Skips if
// qemu-img isn't available on PATH.
func TestRebaseChainAndFlattenToRawWithRealQemuImg(t *testing.T) {
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not found on PATH, skipping real-binary integration test")
	}

	dir := t.TempDir()
	fullPath := filepath.Join(dir, "full.qcow2")
	incPath := filepath.Join(dir, "inc.qcow2")
	outputPath := filepath.Join(dir, "disk.raw")

	runReal := func(args ...string) {
		t.Helper()
		out, err := exec.Command("qemu-img", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("qemu-img %v failed: %v\n%s", args, err, out)
		}
	}

	const virtualSize = 1024 * 1024 // 1M
	runReal("create", "-f", "qcow2", fullPath, "1M")
	runReal("create", "-f", "qcow2", "-F", "qcow2", "-b", fullPath, incPath)

	if err := rebaseChain(context.Background(), []string{fullPath, incPath}); err != nil {
		t.Fatalf("rebaseChain failed against real qemu-img: %v", err)
	}
	if err := flattenToRaw(context.Background(), incPath, outputPath, false); err != nil {
		t.Fatalf("flattenToRaw failed against real qemu-img: %v", err)
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("failed to stat flattened raw image: %v", err)
	}
	if info.Size() != virtualSize {
		t.Errorf("flattened raw image size = %d, want %d", info.Size(), virtualSize)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("flattened raw image permissions = %o, want %o", perm, 0o600)
	}
}

// TestRebaseChainRejectsRealExternalDataFile exercises the external-data-file
// security gate against a real qemu-img binary, using an image genuinely
// created with an external data file (qemu-img create -o data_file=...)
// rather than a hand-written fake "data-file" JSON field, to confirm
// rebaseChain's rejection matches qemu-img's actual --output=json shape.
// Skips if qemu-img isn't available on PATH.
func TestRebaseChainRejectsRealExternalDataFile(t *testing.T) {
	if _, err := exec.LookPath("qemu-img"); err != nil {
		t.Skip("qemu-img not found on PATH, skipping real-binary integration test")
	}

	dir := t.TempDir()
	dataFilePath := filepath.Join(dir, "data.raw")
	basePath := filepath.Join(dir, "base.qcow2")

	runReal := func(args ...string) {
		t.Helper()
		out, err := exec.Command("qemu-img", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("qemu-img %v failed: %v\n%s", args, err, out)
		}
	}

	runReal("create", dataFilePath, "1M")
	runReal("create", "-f", "qcow2", "-o", "data_file="+dataFilePath+",data_file_raw=on", basePath, "1M")

	err := rebaseChain(context.Background(), []string{basePath})
	if err == nil {
		t.Fatal("expected rebaseChain to reject a real qcow2 image with an external data file")
	}
}
