# Version flag and release binaries

- Status: In progress
- Intents: 20261114-release-artifacts-and-image
- ADRs: 20261114-build-release-and-container-pipeline
- Depends on: none

## Scope
`butterfly-sn -version` works, and `make dist` produces the three static binaries, their archives and a checksum
file, reproducibly. The built binaries are inspected and the Linux and Windows ones are smoke-tested.

## Layers touched
- Foundation: `Makefile` with the `build`, `dist`, `test`, `check`, `clean` and `version` targets; `.gitignore`
  already covers `dist/` and `bin/`.
- Services: `internal/app` `Version` variable and `-version` flag handled before the `-config` requirement.
- Interface: the command line and the files under `dist/`.

## End-to-end test
- `-version` prints `butterfly-sn <version>` and exits 0 with no configuration; the ldflags value shows up in a
  binary built by the Makefile.
- `make dist` twice from the same commit yields identical `SHA256SUMS`; `sha256sum -c SHA256SUMS` passes.
- `file` reports ELF x86-64 and ELF aarch64 (statically linked) and PE32+ x86-64 for the three binaries.
- The built linux/amd64 binary answers SEARCHGW with GWINFO from a configuration file and exits 0 on SIGTERM;
  the Windows binary runs `-version` and, if wine is present, answers SEARCHGW too (skipped otherwise).
- Covers criteria: `make dist` output, static linking, version flag, reproducibility, built-binary smoke.

## Out of scope
- The container image, CI workflows and README release instructions; later slices.
- Signing, SBOMs, packages and installers.
