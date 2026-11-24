package common

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestProgressReader(t *testing.T) {
	data := []byte("1234567890")
	reader := bytes.NewReader(data)

	reports := 0
	var lastReported int64

	pr := &ProgressReader{
		Reader:     reader,
		UpdateFreq: 10 * time.Millisecond,
		Reporter: func(bytesRead int64) {
			reports++
			lastReported = bytesRead
		},
	}

	buf := make([]byte, 5)
	_, _ = pr.Read(buf) // Reads 5 bytes

	time.Sleep(15 * time.Millisecond) // Wait for throttle

	_, _ = pr.Read(buf) // Reads 5 bytes
	_, _ = pr.Read(buf) // EOF

	if reports == 0 {
		t.Fatalf("Expected reporter to be called")
	}
	if lastReported != 10 {
		t.Fatalf("Expected 10 bytes reported, got %d", lastReported)
	}
}

func TestProgressReader_Seek(t *testing.T) {
	data := []byte("1234567890")
	reader := bytes.NewReader(data)
	pr := &ProgressReader{Reader: reader}

	// Guard: ensure ProgressReader implements io.Seeker
	var _ io.Seeker = pr

	// Seek to end
	n, err := pr.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 10 {
		t.Errorf("expected offset 10, got %d", n)
	}
	if pr.BytesRead != 10 {
		t.Errorf("expected BytesRead 10, got %d", pr.BytesRead)
	}

	// Seek back to start
	n, err = pr.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected offset 0, got %d", n)
	}
	if pr.BytesRead != 0 {
		t.Errorf("expected BytesRead 0, got %d", pr.BytesRead)
	}
}
