package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	return len(p) / 2, nil
}

func TestCopyContextReportsShortWrite(t *testing.T) {
	n, err := copyContext(context.Background(), shortWriter{}, strings.NewReader("12345678"))
	if n != 4 || err != io.ErrShortWrite {
		t.Fatalf("want (4, io.ErrShortWrite), got (%d, %v)", n, err)
	}
}
