package app

import (
	"bytes"
	"context"
	"testing"
)

func TestVersionFlag(t *testing.T) {
	old := Version
	t.Cleanup(func() { Version = old })
	Version = "v1.2.3"

	var out bytes.Buffer
	if code := Run(context.Background(), []string{"-version"}, &out); code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}
	if got, want := out.String(), "butterfly-sn v1.2.3\n"; got != want {
		t.Fatalf("output %q, want %q", got, want)
	}
}

func TestMissingConfigStillUsageError(t *testing.T) {
	var out bytes.Buffer
	if code := Run(context.Background(), nil, &out); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
}
