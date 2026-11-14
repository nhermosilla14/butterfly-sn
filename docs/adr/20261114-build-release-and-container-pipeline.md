# Build, release and container pipeline

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: operations.md, configuration.md

## Context
The earlier delivery ADR (`20261113-deliver-static-binaries-and-container-image`) decided to ship static binaries
for linux/amd64, linux/arm64 and windows/amd64 and a minimal podman-built image, and deferred the build and CI
specifics to a later ADR. This is that ADR. The machine has GNU make, tar, zip, sha256sum, rootless podman and
buildah but no cross-architecture emulator, and no Git remote exists yet. The binary has no cgo and no
dependency that needs a C toolchain.

## Decision
- **Entry point.** A `Makefile` at the repository root with the targets `build` (host binary in `bin/`), `dist`,
  `image`, `test`, `check`, `clean` and `version`. CI calls these targets and nothing else, so local and CI
  builds are the same.
- **Binaries.** `CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "-s -w -X
  github.com/nhermosilla14/butterfly-sn/internal/app.Version=$(VERSION)"` for each target. `-buildvcs=false` and
  `-trimpath` keep the output independent of the build directory and VCS state, so repeated builds are
  byte-identical. `VERSION` defaults to `git describe --tags --always --dirty`, then `dev`.
- **Version flag.** `internal/app` gains `Version` (default `dev`) and a `-version` flag handled before the
  `-config` requirement; it prints `butterfly-sn <version>` to stdout and returns 0.
- **Archives.** `dist/butterfly-sn_<version>_<os>_<arch>.tar.gz` for linux targets and `.zip` for windows, each
  holding the binary, `LICENSE` and `config.example.yaml` in a directory of the same name; one `SHA256SUMS` file
  lists every archive. File times inside archives are normalised so archives are reproducible too.
- **Image.** `Containerfile` is `FROM scratch`, copies `dist/linux-$TARGETARCH/butterfly-sn` to `/butterfly-sn`,
  sets `USER 65534:65534`, `EXPOSE 5684/udp`, `ENTRYPOINT ["/butterfly-sn"]` and
  `CMD ["-config", "/etc/butterfly-sn/config.yaml"]`, with OCI labels for version and source. It has no `RUN`
  step, so building the other architecture needs no emulation. `make image` runs `podman build --platform
  linux/amd64,linux/arm64 --manifest butterfly-sn:$(VERSION)`. Podman is used through its CLI only; the Podman
  API socket is never started.
- **Image configuration.** The image carries no configuration. Operators mount a YAML file at
  `/etc/butterfly-sn/config.yaml`. Broadcast discovery from a container needs `--network host`, as the earlier
  delivery ADR already notes; unicast use only needs `-p 5684:5684/udp`.
- **CI.** `.github/workflows/ci.yml` runs on pushes and pull requests: `make test`, which includes the
  end-to-end suite that needs Docker (available on the runners), and `make dist` as a build check. The branch-
  record merge gate runs on pull requests into `develop`. `.github/workflows/release.yml` runs on tags `v*`,
  runs `make dist` and attaches `dist/*.tar.gz`, `dist/*.zip` and `dist/SHA256SUMS` to a GitHub release with the
  runner's `gh` CLI. Only first-party actions (`actions/checkout`, `actions/setup-go`) are used, pinned to a
  major version. Container images are not pushed anywhere, since no registry has been chosen.
- **Verification.** The binary smoke tests live with the end-to-end suite (a built binary answers SEARCHGW and
  obeys SIGTERM). The image smoke test is a Go test behind an `image` build tag that drives podman and is run by
  `make image-test`, so the default `go test ./...` does not need podman.

## Alternatives considered
- A Taskfile or a shell script as the entry point — adds a tool or loses declarative targets; make is already
  installed everywhere the project builds.
- Compiling inside a Go build container in the image build — ties the shipped binary to a pulled base image and
  makes the image differ from the release archive.
- GoReleaser — convenient, but adds a tool and a config for something three make rules cover.
- A distroless or Alpine base image — the binary is static and needs nothing, so `scratch` is smaller and has
  nothing to patch; a base image would be required only if TLS roots or a shell were needed.
- Pushing to GHCR from the release workflow — reasonable later, but a registry has not been chosen and nothing
  here should publish without that decision.
- Per-architecture images without a manifest list — forces users to pick the tag; one manifest is simpler.

## Consequences
One command produces every deliverable, locally and in CI, and the checksums let users verify downloads.
Reproducible builds make artifacts comparable across machines. The image is tiny and has no shell, so
troubleshooting uses logs and the binary's own flags. The arm64 image cannot be run on the development machine, so
it is only built and inspected; the same static binary is exercised as the native arm64 target by CI on arm
runners later if needed. The GitHub workflows cannot be run before a remote exists, so they are marked
unverified in the spec until their first execution. Adding a registry push, signing or SBOMs later extends the
release workflow without changing the Makefile contract.
