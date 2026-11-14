# Release artifacts and container image

- Status: In progress
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
Anyone with the repository can produce, from one command each, the deliverables the vision promises: static
binaries for linux/amd64, linux/arm64 and windows/amd64 with checksums, and a minimal container image built with
podman for amd64 and arm64. The binary reports its version, the build is reproducible, and CI runs the checks
and builds on every change and publishes the artifacts when a version tag is pushed.

## Acceptance criteria
- `make dist` builds, from a clean tree and with no cgo, three binaries (linux/amd64, linux/arm64,
  windows/amd64) and packs them with `LICENSE` and `config.example.yaml` into `dist/`: a `.tar.gz` for each Linux
  target and a `.zip` for Windows, named `butterfly-sn_<version>_<os>_<arch>`, plus a `SHA256SUMS` file that
  `sha256sum -c` accepts.
- Every produced binary is statically linked (no dynamic loader), built with `-trimpath`, stripped, and records
  its target: the `file` tool reports ELF x86-64, ELF aarch64 and PE32+ x86-64 respectively.
- `butterfly-sn -version` prints `butterfly-sn <version>` and exits 0 without needing a configuration file. The
  version comes from `git describe --tags --always --dirty`, or `dev` outside a git checkout, and can be
  overridden with `VERSION=...`.
- Two builds of the same commit produce byte-identical binaries.
- The built linux/amd64 binary starts from a configuration file, answers a SEARCHGW with GWINFO and stops cleanly
  on SIGTERM; the Windows binary runs `-version` and, where wine is available, answers a SEARCHGW too.
- `make image` builds, with podman and no daemon socket, a multi-architecture image (linux/amd64 and
  linux/arm64) from `scratch` that contains only the static binary, runs as a non-root user and exposes UDP 5684.
  The default command reads `/etc/butterfly-sn/config.yaml`.
- The amd64 image, run with a mounted configuration, answers SEARCHGW from the host with GWINFO and stops cleanly
  on `podman stop`.
- `make test` runs formatting, vet, build, the root and client test suites and the merge gate; `make clean`
  removes `dist/`.
- GitHub Actions workflows run the checks and cross-builds on pushes and pull requests and, for tags matching
  `v*`, attach the archives and checksums to a GitHub release. Because no remote is configured yet, the
  workflows are recorded as unverified until their first run.
- The README documents building, running, container usage with host networking for discovery, and releasing.

## Spec changes
- Adds: operations / Version flag
- Adds: operations / Release binaries
- Adds: operations / Container image
- Adds: operations / Continuous integration and releases
- Changes: configuration / Not supported

## Out of scope
- Pushing images to a registry, signing artifacts, SBOMs and provenance attestations.
- Windows service installer, Debian/RPM packages, Homebrew or other package managers.
- Environment-variable configuration, health endpoints and metrics.
- Multicast discovery inside containers: containers needing broadcast discovery use host networking.
- Running the arm64 image in tests: there is no emulator on the development machine, so it is built and
  inspected but not executed locally.

## Decisions taken
- GNU make is the single entry point; no new tool is required beyond Go, tar, zip, sha256sum and podman.
- The container image is assembled from the already built static binaries, not compiled inside a build container,
  so the shipped binary is the same artifact as the release archive and the build needs no extra base image.
- Tarballs for Linux and zip for Windows, with checksums in one `SHA256SUMS` file.
- CI uses GitHub Actions because personal projects live on GitHub; it only calls the same `make` targets.
- Version stamping uses a linker variable in `internal/app`; there is no separate version file.
