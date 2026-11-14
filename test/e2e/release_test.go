package e2e

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"debug/elf"
	"debug/pe"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

const versionPkg = "github.com/nhermosilla14/butterfly-sn/internal/app.Version"

// buildBinaryVersion builds cmd/butterfly-sn, optionally stamping the version with ldflags.
func buildBinaryVersion(t *testing.T, version string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "butterfly-sn")
	args := []string{"build", "-o", bin}
	if version != "" {
		args = append(args, "-ldflags", "-X "+versionPkg+"="+version)
	}
	args = append(args, "github.com/nhermosilla14/butterfly-sn/cmd/butterfly-sn")
	if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

func TestVersionFlag(t *testing.T) {
	cases := map[string]struct{ ldflags, want string }{
		"stamped": {"v9.9.9-test", "butterfly-sn v9.9.9-test\n"},
		"default": {"", "butterfly-sn dev\n"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			bin := buildBinaryVersion(t, tc.ldflags)
			var stdout, stderr bytes.Buffer
			cmd := exec.Command(bin, "-version")
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("-version: %v; stderr: %s", err, stderr.String())
			}
			if stdout.String() != tc.want {
				t.Fatalf("stdout %q, want %q", stdout.String(), tc.want)
			}
		})
	}
}

// startProcessGateway starts the real binary with a config and returns the process, its logged address and its
// exit channel.
func startProcessGateway(t *testing.T, bin string, cfgPath string) (*exec.Cmd, string, <-chan error) {
	t.Helper()
	return startProcessEnv(t, nil, bin, "-config", cfgPath)
}

func startProcessEnv(t *testing.T, env []string, name string, args ...string) (*exec.Cmd, string, <-chan error) {
	t.Helper()
	var out syncBuffer
	cmd := exec.Command(name, args...)
	cmd.Env = env
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return cmd, waitForProcessAddr(t, &out, exited), exited
}

func TestBuiltBinaryAnswersSearchgw(t *testing.T) {
	bin := buildBinaryVersion(t, "")
	// Discovery never contacts the broker, so any unreachable address will do.
	cfg := writeConfig(t, fmt.Sprintf("listen: 127.0.0.1:0\nbroker: 127.0.0.1:%d\ngateway_id: 42\nadvertise_interval: 0\n", freePort(t)))
	cmd, addr, exited := startProcessGateway(t, bin, cfg)

	c := newSNClient(t, addr)
	c.send(mqttsn.Searchgw{})
	if got := expect[mqttsn.Gwinfo](t, c, waitLong); got.GwID != 42 {
		t.Fatalf("gateway id %d, want 42", got.GwID)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("exit after SIGTERM: %v", err)
		}
	case <-time.After(waitLong):
		t.Fatal("process did not exit after SIGTERM")
	}
}

// makeDist runs `make dist` in the repository root into a fresh temp directory and returns that directory.
func makeDist(t *testing.T, version string) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds three binaries; skipped in -short mode")
	}
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make not on PATH")
	}
	dist := filepath.Join(t.TempDir(), "dist")
	cmd := exec.Command("make", "-C", "../..", "dist", "VERSION="+version, "DIST="+dist)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make dist: %v\n%s", err, out)
	}
	return dist
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDistArtifacts(t *testing.T) {
	const version = "v0.0.0-test"
	dist := makeDist(t, version)

	archives := []string{
		"butterfly-sn_" + version + "_linux_amd64.tar.gz",
		"butterfly-sn_" + version + "_linux_arm64.tar.gz",
		"butterfly-sn_" + version + "_windows_amd64.zip",
	}
	for _, f := range []string{"linux-amd64/butterfly-sn", "linux-arm64/butterfly-sn", "windows-amd64/butterfly-sn.exe"} {
		if _, err := os.Stat(filepath.Join(dist, f)); err != nil {
			t.Errorf("missing binary: %v", err)
		}
	}

	// SHA256SUMS lists exactly the archives, and each sum matches.
	sums := string(readFile(t, filepath.Join(dist, "SHA256SUMS")))
	lines := strings.Split(strings.TrimSuffix(sums, "\n"), "\n")
	if len(lines) != len(archives) {
		t.Fatalf("SHA256SUMS has %d lines, want %d:\n%s", len(lines), len(archives), sums)
	}
	for i, a := range archives {
		sum := sha256.Sum256(readFile(t, filepath.Join(dist, a)))
		if want := hex.EncodeToString(sum[:]) + "  " + a; lines[i] != want {
			t.Errorf("SHA256SUMS line %d = %q, want %q", i, lines[i], want)
		}
	}

	// Each archive holds <name>/{binary,LICENSE,config.example.yaml}.
	for _, a := range archives {
		got := archiveFiles(t, filepath.Join(dist, a))
		top := strings.TrimSuffix(strings.TrimSuffix(a, ".gz"), ".tar")
		top = strings.TrimSuffix(top, ".zip")
		bin := "butterfly-sn"
		if strings.HasSuffix(a, ".zip") {
			bin += ".exe"
		}
		for _, f := range []string{bin, "LICENSE", "config.example.yaml"} {
			if _, ok := got[top+"/"+f]; !ok {
				t.Errorf("%s lacks %s/%s", a, top, f)
			}
		}
		if len(got) != 3 {
			t.Errorf("%s has %d files, want 3", a, len(got))
		}
	}

	// Binary headers and static linking.
	for dir, machine := range map[string]elf.Machine{"linux-amd64": elf.EM_X86_64, "linux-arm64": elf.EM_AARCH64} {
		f, err := elf.Open(filepath.Join(dist, dir, "butterfly-sn"))
		if err != nil {
			t.Fatal(err)
		}
		if f.Machine != machine {
			t.Errorf("%s machine %v, want %v", dir, f.Machine, machine)
		}
		for _, p := range f.Progs {
			if p.Type == elf.PT_INTERP || p.Type == elf.PT_DYNAMIC {
				t.Errorf("%s is not static: has %v", dir, p.Type)
			}
		}
		if f.Section(".dynamic") != nil {
			t.Errorf("%s has a .dynamic section", dir)
		}
		_ = f.Close()
	}
	pf, err := pe.Open(filepath.Join(dist, "windows-amd64", "butterfly-sn.exe"))
	if err != nil {
		t.Fatal(err)
	}
	if pf.Machine != pe.IMAGE_FILE_MACHINE_AMD64 {
		t.Errorf("windows machine %#x, want AMD64", pf.Machine)
	}
	_ = pf.Close()

	// The host-runnable binary reports the stamped version.
	if out, err := exec.Command(filepath.Join(dist, "linux-amd64", "butterfly-sn"), "-version").Output(); err != nil {
		t.Errorf("-version: %v", err)
	} else if string(out) != "butterfly-sn "+version+"\n" {
		t.Errorf("-version output %q", out)
	}

	// Reproducible: a second build gives identical checksums.
	if again := string(readFile(t, filepath.Join(makeDist(t, version), "SHA256SUMS"))); again != sums {
		t.Errorf("second make dist differs:\n%s\nvs\n%s", sums, again)
	}
}

// archiveFiles returns the regular files of a .tar.gz or .zip archive keyed by path.
func archiveFiles(t *testing.T, path string) map[string]bool {
	t.Helper()
	files := map[string]bool{}
	if strings.HasSuffix(path, ".zip") {
		zr, err := zip.OpenReader(path)
		if err != nil {
			t.Fatal(err)
		}
		defer zr.Close()
		for _, f := range zr.File {
			if !f.FileInfo().IsDir() {
				files[f.Name] = true
			}
		}
		return files
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			files[h.Name] = true
		}
	}
}

func TestWindowsBinaryWithWine(t *testing.T) {
	if _, err := exec.LookPath("wine"); err != nil {
		t.Skip("wine not on PATH")
	}
	const version = "v0.0.0-test"
	dist := makeDist(t, version)
	exe := filepath.Join(dist, "windows-amd64", "butterfly-sn.exe")

	prefix := t.TempDir()
	// Headless: no X11 or Wayland driver, so wineboot cannot open windows on the developer's desktop.
	var env []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "DISPLAY=") && !strings.HasPrefix(kv, "WAYLAND_DISPLAY=") {
			env = append(env, kv)
		}
	}
	env = append(env, "WINEPREFIX="+prefix, "WINEDEBUG=-all", "WINEARCH=win64",
		"WINEDLLOVERRIDES=mscoree,mshtml=d;winemenubuilder.exe=d;winex11.drv=d;winewayland.drv=d")
	t.Cleanup(func() {
		if ws := findWineserver(); ws != "" {
			k := exec.Command(ws, "-k")
			k.Env = env
			_ = k.Run()
		}
	})
	wine := func(timeout time.Duration, args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "wine", args...)
		cmd.Env = env
		cmd.WaitDelay = 2 * time.Second
		var stdout bytes.Buffer
		cmd.Stdout = &stdout
		err := cmd.Run()
		return stdout.Bytes(), err
	}

	if _, err := wine(90*time.Second, "cmd", "/c", "exit"); err != nil {
		t.Skipf("wine does not start in 90s probe: %v", err)
	}
	out, err := wine(60*time.Second, exe, "-version")
	if err != nil {
		t.Fatalf("wine -version: %v; output %q", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "butterfly-sn "+version; got != want {
		t.Fatalf("output %q, want %q", got, want)
	}

	// Not checked: SEARCHGW under wine. Go's UDP listen fails there (WSAIoctl error 10045, wine lacks
	// SIO_UDP_CONNRESET), so only -version is asserted.
}

// findWineserver locates wineserver, which distributions often keep off PATH (Debian and Ubuntu put it under
// /usr/lib/<triplet>/wine). An empty result means it was not found.
func findWineserver() string {
	if p, err := exec.LookPath("wineserver"); err == nil {
		return p
	}
	for _, pattern := range []string{"/usr/lib/wine/wineserver*", "/usr/lib/*/wine/wineserver*", "/usr/lib/wine-stable/wineserver*"} {
		if m, _ := filepath.Glob(pattern); len(m) > 0 {
			return m[0]
		}
	}
	return ""
}
