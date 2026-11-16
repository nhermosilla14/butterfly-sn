# Operations

How the gateway is built, versioned, packaged and shipped: the version flag, the release binaries, the container
image and the continuous-integration workflows. The `Taskfile.yml` is the single entry point for all of it.

## Behaviours

### Version flag
`butterfly-sn -version` prints `butterfly-sn <version>` followed by a newline and exits 0. It needs no
configuration file and prints nothing else. The version comes from `git describe --tags --always --dirty` at build
time, or `dev` for a plain `go build`, and can be set with `task ... VERSION=...`.
- Criteria:
  - `-version` prints exactly `butterfly-sn <version>` and exits 0 with no `-config`.
  - A binary built with the linker variable reports the stamped version; an unstamped build reports `dev`.
  - Without `-version`, a missing `-config` is still a usage error with exit status 2.
- Tests: TestVersionFlag, TestMissingConfigStillUsageError

### Release binaries
`task dist` builds static binaries for linux/amd64, linux/arm64 and windows/amd64 without cgo and packs each with
`LICENSE` and `config.example.yaml` into an archive named `butterfly-sn_<version>_<os>_<arch>` (`.tar.gz` for
Linux, `.zip` for Windows), plus one `SHA256SUMS` file. Builds are reproducible.
- Criteria:
  - The three binaries and three archives exist and `SHA256SUMS` matches every archive.
  - The Linux binaries are statically linked ELF files for x86-64 and aarch64; the Windows binary is a PE32+
    x86-64 executable.
  - Each archive contains exactly the binary, `LICENSE` and `config.example.yaml` in one top-level directory.
  - Two builds of the same commit give identical checksums.
  - The built Linux binary starts from a configuration file, answers a SEARCHGW with GWINFO and exits 0 on SIGTERM.
  - The Windows binary runs and reports its version.
- Tests: TestDistArtifacts, TestBuiltBinaryAnswersSearchgw, TestWindowsBinaryWithWine

### Container image
`task image` builds, with the podman CLI only, a multi-architecture manifest `butterfly-sn:<version>` for
linux/amd64 and linux/arm64 from `scratch`, assembled from the release binaries. The image holds only the static
binary, runs as UID 65534, exposes UDP 5684 and reads its configuration from `/etc/butterfly-sn/config.yaml`,
which the operator mounts. `task image-test` builds the host-architecture image and smoke-tests it; it needs
podman but no Docker.
- Criteria:
  - The image runs as 65534:65534, exposes `5684/udp`, uses `/butterfly-sn` as entrypoint and
    `-config /etc/butterfly-sn/config.yaml` as default command, and carries the version label.
  - The only regular file in the image is `/butterfly-sn`.
  - The image prints its stamped version with `-version`.
  - With a mounted configuration and UDP 5684 published, the container answers SEARCHGW with GWINFO and exits 0
    on `podman stop`.
  - The podman API socket is never started.
- Tests: TestImageMetadata, TestImageContainsOnlyBinary, TestImageVersionFlag, TestContainerAnswersSearchgwAndStopsCleanly

### Continuous integration and releases
Two GitHub Actions workflows call only `Taskfile.yml` tasks, after installing task with a pinned
`go install github.com/go-task/task/v3/cmd/task@v3.54.0`. `CI` runs `task test` and `task dist` on pushes to
`develop` and `main` and on pull requests, and runs the branch-record merge gate on pull requests into `develop`.
`Release` runs on every push to `main`: it takes the next version from `task next-version` (the latest `v*` tag
with its patch incremented, `v0.1.0` when no tag exists), runs the tests, builds the archives with `task dist` and
that version stamped into the binary, and creates a GitHub release carrying the three archives and `SHA256SUMS`.
Releases are serialised, so two merges in a row cannot compute the same version.
- Criteria:
  - Both workflow files are valid YAML and every `run:` step calls an existing Taskfile task, except the pinned
    task install step, the version output line and the single `gh release create` step.
  - Every job that calls a task installs it first with a pinned `go-task` version.
  - `Release` triggers on `main` pushes only, never on tags, and its release carries the `linux_amd64`,
    `linux_arm64` and `windows_amd64` archives plus `SHA256SUMS`.
  - Triggers, permissions and the jobs named above are present, and only first-party actions pinned to a major
    version are used.
- Tests: TestWorkflowsUseOnlyTaskTargets, TestCIStructure, TestReleaseStructure, TestCheckerRejectsBadWorkflows
- Verified: `CI` ran green on the first push to `main` (run 43943134099, 2026-11-16).

## Not supported
- Pushing images to a registry, signing artifacts, SBOMs and provenance attestations.
- Windows service installers, Debian or RPM packages, and other package managers.
- Running the arm64 image in tests: it is built and inspected only, because the development machine has no
  emulator.
- A Windows SEARCHGW check: under wine the Go UDP listener fails (`WSAIoctl` error 10045), so the Windows binary is
  verified for `-version` only and the network path on real Windows is unverified.
- Configuration by environment variable, health endpoints and metrics.
- `task image` leaves untagged per-platform images in podman's store; remove them with `podman image prune` if
  they are not wanted.
