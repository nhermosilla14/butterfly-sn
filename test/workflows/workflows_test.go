// Package workflows keeps the GitHub Actions workflows consistent with the Taskfile.
package workflows

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

type workflow struct {
	Name        string            `yaml:"name"`
	On          map[string]any    `yaml:"on"`
	Permissions map[string]string `yaml:"permissions"`
	Jobs        map[string]job    `yaml:"jobs"`
}

type job struct {
	If    string `yaml:"if"`
	Steps []step `yaml:"steps"`
}

type step struct {
	Uses string `yaml:"uses"`
	Run  string `yaml:"run"`
}

var (
	pinnedRe   = regexp.MustCompile(`^actions/(checkout|setup-go|upload-artifact)@v\d+$`)
	assignRe   = regexp.MustCompile(`^\w+=`)
	installRe  = regexp.MustCompile(`^go install github\.com/go-task/task/v3/cmd/task@v3\.\d+\.\d+$`)
	pathLineRe = regexp.MustCompile(`^echo "\$\(go env GOPATH\)/bin" >> "\$GITHUB_PATH"$`)
)

func load(t *testing.T, path string) workflow {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return parse(t, data)
}

func parse(t *testing.T, data []byte) workflow {
	t.Helper()
	var w workflow
	if err := yaml.Unmarshal(data, &w); err != nil {
		t.Fatalf("invalid workflow YAML: %v", err)
	}
	return w
}

func taskNames(t *testing.T) map[string]bool {
	t.Helper()
	data, err := os.ReadFile("../../Taskfile.yml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Tasks map[string]any `yaml:"tasks"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid Taskfile YAML: %v", err)
	}
	if len(doc.Tasks) == 0 {
		t.Fatal("no tasks in Taskfile.yml")
	}
	names := map[string]bool{}
	for name := range doc.Tasks {
		names[name] = true
	}
	return names
}

// checkSteps returns problems found in the run and uses steps of w. Every job that calls task must
// install it first with a pinned go install; the only other allowed run lines are the prefixes in
// allowed (release.yml calls the gh CLI and exports its tool path).
func checkSteps(w workflow, targets map[string]bool, allowed ...string) []string {
	var problems []string
	for name, j := range w.Jobs {
		installed := false
		for _, s := range j.Steps {
			if s.Uses != "" && !pinnedRe.MatchString(s.Uses) {
				problems = append(problems, fmt.Sprintf("job %s: action %q is not an allowed major-pinned action", name, s.Uses))
			}
			for _, line := range strings.Split(s.Run, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				if installRe.MatchString(line) {
					installed = true
					continue
				}
				if pathLineRe.MatchString(line) {
					continue
				}
				if strings.HasPrefix(line, "task ") {
					if !installed {
						problems = append(problems, fmt.Sprintf("job %s: task used before the pinned install step: %q", name, line))
					}
					for _, word := range strings.Fields(line)[1:] {
						if strings.HasPrefix(word, "-") || assignRe.MatchString(word) {
							continue
						}
						if !targets[word] {
							problems = append(problems, fmt.Sprintf("job %s: unknown task %q", name, word))
						}
					}
					continue
				}
				if !hasPrefixAny(line, allowed) {
					problems = append(problems, fmt.Sprintf("job %s: command not allowed: %q", name, line))
				}
			}
		}
	}
	return problems
}

func hasPrefixAny(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// The release job's gh step is a multi-line script; only its gh line is checked, the
// other lines (flag selection) are shell glue and are listed as allowed prefixes.
var releaseAllowed = []string{"gh release create", "flags=", "case "}

func callsTask(j job, target string) bool {
	for _, s := range j.Steps {
		for _, line := range strings.Split(s.Run, "\n") {
			f := strings.Fields(line)
			if len(f) < 2 || f[0] != "task" {
				continue
			}
			for _, word := range f[1:] {
				if word == target {
					return true
				}
			}
		}
	}
	return false
}

func TestWorkflowsUseOnlyTaskTargets(t *testing.T) {
	targets := taskNames(t)
	for _, f := range []string{"ci.yml", "release.yml"} {
		w := load(t, "../../.github/workflows/"+f)
		var allowed []string
		if f == "release.yml" {
			allowed = releaseAllowed
		}
		for _, p := range checkSteps(w, targets, allowed...) {
			t.Errorf("%s: %s", f, p)
		}
	}
}

func TestCIStructure(t *testing.T) {
	w := load(t, "../../.github/workflows/ci.yml")
	push, ok := w.On["push"].(map[string]any)
	if !ok {
		t.Fatal("ci.yml must trigger on push")
	}
	branches := fmt.Sprint(push["branches"])
	if !strings.Contains(branches, "develop") || !strings.Contains(branches, "main") {
		t.Errorf("push branches = %s, want develop and main", branches)
	}
	if _, ok := w.On["pull_request"]; !ok {
		t.Error("ci.yml must trigger on pull_request")
	}
	if w.Permissions["contents"] != "read" {
		t.Error("ci.yml must have contents: read")
	}
	for job, target := range map[string]string{"test": "test", "dist": "dist", "docs": "check-docs"} {
		if !callsTask(w.Jobs[job], target) {
			t.Errorf("ci.yml job %s must call task %s", job, target)
		}
	}
	want := "github.event_name == 'pull_request' && github.base_ref == 'develop'"
	if w.Jobs["docs"].If != want {
		t.Errorf("docs job if = %q, want %q", w.Jobs["docs"].If, want)
	}
}

func TestReleaseStructure(t *testing.T) {
	w := load(t, "../../.github/workflows/release.yml")
	if w.Permissions["contents"] != "write" {
		t.Error("release.yml must have contents: write")
	}
	if len(w.On) != 1 || !strings.Contains(fmt.Sprint(w.On["push"]), "v*") {
		t.Errorf("release.yml must trigger only on push of tags v*, got %v", w.On)
	}
	if !callsTask(w.Jobs["release"], "dist") {
		t.Error("release.yml must call task dist")
	}
}

const installStep = `      - run: |
          go install github.com/go-task/task/v3/cmd/task@v3.54.0
          echo "$(go env GOPATH)/bin" >> "$GITHUB_PATH"` + "\n"

func TestCheckerRejectsBadWorkflows(t *testing.T) {
	targets := map[string]bool{"test": true, "dist": true}
	bad := map[string]string{
		"unknown task":        "jobs:\n  a:\n    steps:\n" + installStep + "      - run: task nonexistent\n",
		"task before install": "jobs:\n  a:\n    steps:\n      - run: task test\n",
		"unpinned install":    "jobs:\n  a:\n    steps:\n      - run: go install github.com/go-task/task/v3/cmd/task@latest\n      - run: task test\n",
		"curl":                "jobs:\n  a:\n    steps:\n" + installStep + "      - run: curl http://example.com\n",
		"gh not allowed":      "jobs:\n  a:\n    steps:\n" + installStep + "      - run: gh release create v1\n",
		"third party":         "jobs:\n  a:\n    steps:\n      - uses: evil/action@v1\n",
		"unpinned":            "jobs:\n  a:\n    steps:\n      - uses: actions/checkout@main\n",
		"second line":         "jobs:\n  a:\n    steps:\n" + installStep + "      - run: |\n          task test\n          task nonexistent\n",
	}
	for name, src := range bad {
		if len(checkSteps(parse(t, []byte(src)), targets)) == 0 {
			t.Errorf("%s: checker accepted a bad workflow", name)
		}
	}
	good := "jobs:\n  a:\n    steps:\n      - uses: actions/checkout@v4\n" + installStep +
		"      - run: task -p test VERSION=x dist\n"
	if p := checkSteps(parse(t, []byte(good)), targets); len(p) != 0 {
		t.Errorf("checker rejected a good workflow: %v", p)
	}
}
