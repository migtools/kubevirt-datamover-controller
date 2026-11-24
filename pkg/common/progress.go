package common

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ProgressReader wraps an io.Reader and reports progress periodically.
type ProgressReader struct {
	Reader     io.Reader
	BytesRead  int64
	LastUpdate time.Time
	UpdateFreq time.Duration
	Reporter   func(bytesRead int64)
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.Reader.Read(p)
	if n > 0 {
		pr.BytesRead += int64(n)
		pr.report(false)
	}
	if err == io.EOF {
		pr.report(true) // Force update on completion
	}
	return n, err
}

func (pr *ProgressReader) report(force bool) {
	if pr.Reporter != nil && (force || time.Since(pr.LastUpdate) >= pr.UpdateFreq) {
		pr.LastUpdate = time.Now()
		pr.Reporter(pr.BytesRead)
	}
}

// Seek implements io.Seeker so AWS transfermanager can determine the file size
// to calculate part sizes for large uploads (>78GiB).
func (pr *ProgressReader) Seek(offset int64, whence int) (int64, error) {
	seeker, ok := pr.Reader.(io.Seeker)
	if !ok {
		return 0, fmt.Errorf("underlying reader does not implement io.Seeker")
	}
	newOffset, err := seeker.Seek(offset, whence)
	if err == nil {
		pr.BytesRead = newOffset
	}
	return newOffset, err
}

// PatchPodProgress updates the datamover pod with progress annotations.
func PatchPodProgress(
	ctx context.Context, k8sClient client.Client, name, namespace string,
	total, inc, done int64, logger logr.Logger,
) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	patch := client.RawPatch(types.MergePatchType, fmt.Appendf(nil,
		`{"metadata":{"annotations":{`+
			`"%s":"%d",`+
			`"%s":"%d",`+
			`"%s":"%d"}}}`,
		AnnotationProgressTotalBytes, total,
		AnnotationProgressIncrementalBytes, inc,
		AnnotationProgressBytesDone, done))
	if err := k8sClient.Patch(ctx, pod, patch); err != nil {
		logger.Error(err, "Failed to patch pod progress annotations")
	}
}
