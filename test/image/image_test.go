//go:build image

// Package image smoke-tests the container image built by `task image-test`. It needs rootless podman and
// BUTTERFLY_SN_IMAGE (image name) and BUTTERFLY_SN_IMAGE_VERSION (stamped version) in the environment.
package image

import (
	"archive/tar"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nhermosilla14/butterfly-sn/internal/mqttsn"
)

func setup(t *testing.T) (image, version string) {
	t.Helper()
	if _, err := exec.LookPath("podman"); err != nil {
		t.Skip("podman not on PATH")
	}
	image = os.Getenv("BUTTERFLY_SN_IMAGE")
	if image == "" {
		t.Skip("BUTTERFLY_SN_IMAGE not set; run `task image-test`")
	}
	return image, os.Getenv("BUTTERFLY_SN_IMAGE_VERSION")
}

func podman(t *testing.T, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("podman", args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("podman %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return strings.TrimSpace(stdout.String())
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), time.Now().UnixNano())
}

func TestImageMetadata(t *testing.T) {
	image, version := setup(t)
	f := func(format string) string { return podman(t, "image", "inspect", "--format", format, image) }

	if got := f("{{.Config.User}}"); got != "65534:65534" {
		t.Errorf("user %q, want 65534:65534", got)
	}
	if got := f("{{range $p, $_ := .Config.ExposedPorts}}{{$p}} {{end}}"); strings.TrimSpace(got) != "5684/udp" {
		t.Errorf("exposed ports %q, want 5684/udp", got)
	}
	if got := f("{{json .Config.Entrypoint}}"); got != `["/butterfly-sn"]` {
		t.Errorf("entrypoint %s", got)
	}
	if got := f("{{json .Config.Cmd}}"); got != `["-config","/etc/butterfly-sn/config.yaml"]` {
		t.Errorf("cmd %s", got)
	}
	if got := f(`{{index .Config.Labels "org.opencontainers.image.version"}}`); got != version {
		t.Errorf("version label %q, want %q", got, version)
	}
}

func TestImageContainsOnlyBinary(t *testing.T) {
	image, _ := setup(t)
	name := uniqueName("butterfly-sn-test-fs")
	podman(t, "create", "--name", name, image)
	t.Cleanup(func() { _ = exec.Command("podman", "rm", "-f", name).Run() })

	tarPath := filepath.Join(t.TempDir(), "fs.tar")
	podman(t, "export", "-o", tarPath, name)
	f, err := os.Open(tarPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var files []string
	for tr := tar.NewReader(f); ; {
		h, err := tr.Next()
		if err != nil {
			break
		}
		if h.Typeflag == tar.TypeReg {
			files = append(files, h.Name)
		}
	}
	if !reflect.DeepEqual(files, []string{"butterfly-sn"}) {
		t.Fatalf("regular files %v, want only butterfly-sn", files)
	}
}

func TestImageVersionFlag(t *testing.T) {
	image, version := setup(t)
	got := podman(t, "run", "--rm", "--name", uniqueName("butterfly-sn-test-ver"), image, "-version")
	if want := "butterfly-sn " + version; got != want {
		t.Fatalf("-version printed %q, want %q", got, want)
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

func TestContainerAnswersSearchgwAndStopsCleanly(t *testing.T) {
	image, _ := setup(t)
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	// Discovery never contacts the broker, so an unreachable address will do.
	body := "listen: 0.0.0.0:5684\nbroker: 127.0.0.1:1\ngateway_id: 42\nadvertise_interval: 0\n"
	if err := os.WriteFile(cfg, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	name := uniqueName("butterfly-sn-test-run")
	t.Cleanup(func() { _ = exec.Command("podman", "rm", "-f", name).Run() })
	port := freeUDPPort(t)
	podman(t, "run", "-d", "--name", name,
		"-v", cfg+":/etc/butterfly-sn/config.yaml:ro,Z",
		"-p", fmt.Sprintf("127.0.0.1:%d:5684/udp", port), image)

	deadline := time.Now().Add(15 * time.Second)
	for {
		out, _ := exec.Command("podman", "logs", name).CombinedOutput()
		if strings.Contains(string(out), "gateway started") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway did not start; logs:\n%s", out)
		}
		time.Sleep(200 * time.Millisecond)
	}

	if got := podman(t, "inspect", "--format", "{{.Config.User}}", name); got != "65534:65534" {
		t.Errorf("container user %q, want 65534:65534", got)
	}

	req, err := mqttsn.Encode(mqttsn.Searchgw{})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("udp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var gw *mqttsn.Gwinfo
	buf := make([]byte, 1500)
	for end := time.Now().Add(10 * time.Second); gw == nil && time.Now().Before(end); {
		_, _ = conn.Write(req)
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := conn.Read(buf)
		if err != nil {
			continue
		}
		if m, err := mqttsn.Decode(buf[:n]); err == nil {
			if g, ok := m.(mqttsn.Gwinfo); ok {
				gw = &g
			}
		}
	}
	if gw == nil {
		out, _ := exec.Command("podman", "logs", name).CombinedOutput()
		t.Fatalf("no GWINFO received; logs:\n%s", out)
	}
	if gw.GwID != 42 {
		t.Fatalf("gateway id %d, want 42", gw.GwID)
	}

	podman(t, "stop", "--time", "10", name)
	if got := podman(t, "inspect", "--format", "{{.State.ExitCode}}", name); got != "0" {
		out, _ := exec.Command("podman", "logs", name).CombinedOutput()
		t.Fatalf("exit code %s, want 0; logs:\n%s", got, out)
	}
}
