# CI, releases and documentation

- Intents: 20261114-release-artifacts-and-image
- ADRs: 20261114-build-release-and-container-pipeline
- Depends on: 01, 02

## Scope
GitHub Actions workflows run the same make targets on pushes and pull requests, and publish the archives and
checksums when a `v*` tag is pushed. The README explains building, running, container use and releasing, and the
spec describes the operations behaviour with honest verification status.

## Layers touched
- Foundation: `.github/workflows/ci.yml` and `.github/workflows/release.yml`; the README sections; the
  `docs/spec/operations.md` area and its index entries; the Conventions note in `AGENTS.md` about make targets.
- Services and interface: none.

## End-to-end test
- Both workflow files parse as valid YAML and every `run:` step calls a target that exists in the Makefile; a
  small check (a test or a script run by `make check`) enforces that, so the workflows cannot drift from the
  Makefile.
- The documented commands (`make build`, `make dist`, `make image`, `make test`, the container run example) are
  run exactly as written.
- The workflows themselves cannot be executed without a remote; the spec records them as unverified.
- Covers criteria: CI and release workflow definitions, documentation, documented commands work.

## Out of scope
- Registry publication, signing, environment-variable configuration, and any change to gateway behaviour.
