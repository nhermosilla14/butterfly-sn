# Container image

- Status: In progress
- Intents: 20261114-release-artifacts-and-image
- ADRs: 20261114-build-release-and-container-pipeline
- Depends on: 01

## Scope
`make image` builds a multi-architecture scratch image from the release binaries with podman CLI only. The amd64
image runs with a mounted configuration, answers SEARCHGW and stops cleanly.

## Layers touched
- Foundation: `Containerfile`; `make image` and `make image-test` targets.
- Services: none.
- Interface: `podman` CLI, the mounted configuration path and UDP 5684.

## End-to-end test
- `make image` creates a manifest list with linux/amd64 and linux/arm64, each image containing only
  `/butterfly-sn`, running as UID 65534 and exposing UDP 5684.
- A Go test behind the `image` build tag, run by `make image-test`, starts the amd64 image with a generated
  configuration mounted at `/etc/butterfly-sn/config.yaml` and a loopback UDP port mapping, sends SEARCHGW from
  the host, expects GWINFO, then stops the container and checks a zero exit.
- The default `go test ./...` never invokes podman.
- Covers criteria: image content and user, default command, container runtime smoke, no daemon socket.

## Out of scope
- Registry push, signing, SBOMs; running the arm64 image (no emulator locally).
- Environment-variable configuration and health checks.
