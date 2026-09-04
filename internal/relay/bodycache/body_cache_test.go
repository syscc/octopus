package bodycache

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func TestNewExactLimitAndOversizeCleansSpillFile(t *testing.T) {
	t.Setenv(envBodyMaxMB, "2")
	t.Setenv(envMemoryThresholdMB, "1")
	tmpDir := t.TempDir()
	t.Setenv(envTmpDir, tmpDir)

	exact := bytes.Repeat([]byte{'x'}, int(2*bytesPerMB))
	cache, err := New(io.NopCloser(bytes.NewReader(exact)))
	if err != nil {
		t.Fatalf("exact-limit New() error = %v", err)
	}
	if cache.Size() != 2*bytesPerMB || !cache.IsFile() {
		t.Fatalf("exact-limit cache = size %d file=%v, want %d/file", cache.Size(), cache.IsFile(), 2*bytesPerMB)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("cache.Close() error = %v", err)
	}
	if entries, err := os.ReadDir(tmpDir); err != nil {
		t.Fatalf("ReadDir after close: %v", err)
	} else if len(entries) != 0 {
		t.Fatalf("cache.Close left %d temporary files", len(entries))
	}

	over := append(exact, 'y')
	_, err = New(io.NopCloser(bytes.NewReader(over)))
	var tooLarge *BodyTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("oversize New() error = %v, want BodyTooLargeError", err)
	}
	if tooLarge.MaxBytes != 2*bytesPerMB || tooLarge.ActualBytes != 2*bytesPerMB+1 {
		t.Fatalf("oversize details = %+v, want max=%d actual=%d", tooLarge, 2*bytesPerMB, 2*bytesPerMB+1)
	}
	if entries, err := os.ReadDir(tmpDir); err != nil {
		t.Fatalf("ReadDir after oversize: %v", err)
	} else if len(entries) != 0 {
		t.Fatalf("oversize New left %d temporary files", len(entries))
	}
}

type bodyReadError struct {
	data []byte
	err  error
}

func (r *bodyReadError) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

func TestNewReadErrorCleansSpillFile(t *testing.T) {
	t.Setenv(envBodyMaxMB, "2")
	t.Setenv(envMemoryThresholdMB, "1")
	tmpDir := t.TempDir()
	t.Setenv(envTmpDir, tmpDir)

	readErr := errors.New("synthetic body read failure")
	partial := bytes.Repeat([]byte{'p'}, int(bytesPerMB+1))
	_, err := New(io.NopCloser(&bodyReadError{data: partial, err: readErr}))
	if !errors.Is(err, readErr) {
		t.Fatalf("New() error = %v, want %v", err, readErr)
	}
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir after read error: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("read error left %d temporary files", len(entries))
	}
}

func TestBodyCacheEnvironmentValuesClampBeforeMultiplication(t *testing.T) {
	t.Setenv(envBodyMaxMB, "9223372036854775807")
	if got := BodyMaxBytesFromEnv(); got != maxInt64 {
		t.Fatalf("BodyMaxBytesFromEnv() = %d, want max int64", got)
	}
	t.Setenv(envMemoryThresholdMB, "9223372036854775807")
	if got := MemoryThresholdBytesFromEnv(); got != maxInt64 {
		t.Fatalf("MemoryThresholdBytesFromEnv() = %d, want max int64", got)
	}
	t.Setenv(envTmpCleanupHours, "9223372036854775807")
	if got := TmpCleanupOlderThanFromEnv(); got != time.Duration(maxInt64) {
		t.Fatalf("TmpCleanupOlderThanFromEnv() = %d, want max duration", got)
	}
}

type shortSpillFile struct{}

func (shortSpillFile) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func (shortSpillFile) Close() error { return nil }

func TestSpillWriterRejectsShortWritesAndSizeOverflow(t *testing.T) {
	w := &spillWriter{thresholdBytes: -1, f: shortSpillFile{}}
	n, err := w.Write([]byte("abcd"))
	if n != 3 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short spill write = n:%d err:%v, want n=3/io.ErrShortWrite", n, err)
	}
	if w.size != 3 {
		t.Fatalf("short spill size = %d, want 3", w.size)
	}

	w = &spillWriter{thresholdBytes: maxInt64, size: maxInt64}
	if n, err := w.Write([]byte{'x'}); n != 0 || !errors.Is(err, errBodyCacheSizeOverflow) {
		t.Fatalf("overflow spill write = n:%d err:%v, want zero/overflow", n, err)
	}
}

func TestMegabytesToBytesHandlesBoundary(t *testing.T) {
	maxMB := int(maxInt64 / bytesPerMB)
	if got := megabytesToBytes(maxMB); got != int64(maxMB)*bytesPerMB {
		t.Fatalf("boundary conversion = %d", got)
	}
	if got := megabytesToBytes(maxMB + 1); got != maxInt64 {
		t.Fatalf("overflow conversion = %d, want max int64", got)
	}
}
