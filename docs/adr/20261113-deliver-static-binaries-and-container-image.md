# Deliver static binaries and a container image

- Status: Accepted
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla

## Context
The old gateway ships as an Alpine container (plus a Cygwin installer for Windows). Targets are Linux
(amd64, arm64) and Win64. The owner deploys with podman. UDP multicast discovery does not cross the
default container network.

## Decision
Release static binaries (`CGO_ENABLED=0`) for linux/amd64, linux/arm64 and windows/amd64, and a
minimal container image (scratch or distroless base) built with podman. Document host networking for
multicast discovery. Build and CI specifics go in a later ADR when the first slice needs them.

## Alternatives considered
- Native installers (Inno Setup, deb, rpm) — more maintenance; not required now.

## Consequences
No cgo dependencies are allowed. Containerised deployments that need discovery require `--network host`.
