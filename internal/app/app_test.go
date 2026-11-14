package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
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

func TestBadRetryLimitFailsStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("listen: 127.0.0.1:0\nbroker: 127.0.0.1:1883\nretry_limit: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if code := Run(context.Background(), []string{"-config", path}, &out); code == 0 {
		t.Fatalf("exit 0, want non-zero")
	}
	if !strings.Contains(out.String(), "retry_limit") {
		t.Fatalf("output %q, want it to name retry_limit", out.String())
	}
}
