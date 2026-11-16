# Taskfile as the build entry point

- Replaces: 20261114-build-release-and-container-pipeline
- Status: Accepted
- Date: 2026-11-16
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: operations.md

## Context
The build, test and release pipeline ADR chose GNU make as the single entry point and listed a Taskfile as a
rejected alternative, on the grounds that make is already installed everywhere the project builds. In practice the
recipes grew shell loops, `$$` escaping, `$()` subshells and single-line quoting workarounds (the `DIST` guard in
`image` and `image-test` is one long quoted string), which made them hard to read and easy to break. The
maintainer prefers Taskfile's YAML definitions and its `{{ }}` templating. Task 3.54 is installed on the
development machine, and CI can get it from the Go module proxy.

## Decision
- **Entry point.** A `Taskfile.yml` at the repository root replaces the `Makefile`, which is deleted. It keeps the
  same task names, so documentation and habits translate one to one: `build`, `dist`, `image`, `image-test`,
  `test`, `check-docs`, `check`, `clean` and `version`, plus `help`, which is also the default task and lists the
  tasks. Steps reused by several tasks (`fmt-check`, `test-client`, `require-dist`) are `internal: true` so they
  do not clutter the list.
- **Variables.** `VERSION` (default `git describe --tags --always --dirty`, then `dev`), `DIST` (`dist`), `BIN`
  (`bin`) and `HOSTARCH` are Taskfile variables, overridable on the command line the same way as before:
  `task dist VERSION=v1.2.3`.
- **Same artefacts.** The `dist` task builds the same three static binaries with the same linker flags, the same
  reproducible archives and the same `SHA256SUMS`; `image` and `image-test` run the same podman commands, and the
  image-test tag still carries the shell PID so runs cannot collide. Artifacts are byte-identical to the ones the
  Makefile produced.
- **CI.** `task` is not preinstalled on GitHub runners, so each job that calls a task runs
  `go install github.com/go-task/task/v3/cmd/task@v3.54.0` and adds `$(go env GOPATH)/bin` to `GITHUB_PATH` first.
  The version is pinned in the workflow, and no third-party action is introduced: the first-party-actions-only
  rule of the replaced ADR stays.
- **Tests.** `test/workflows` parses `Taskfile.yml` instead of the `Makefile`'s `.PHONY` line, requires the pinned
  install to precede any task call in a job, and keeps rejecting unknown tasks and non-whitelisted commands. The
  end-to-end release test invokes `task dist`.

## Alternatives considered
- Keep GNU make — rejected: the maintainer finds the recipes harder to read, and its escaping rules were the
  source of the least readable parts of the pipeline.
- Keep a `Makefile` that forwards to `task` — two entry points to keep in sync, and the forwarding target set
  would drift from the Taskfile.
- `arduino/setup-task@v2` in CI — one line and cached, but it adds a third-party action, which the replaced ADR
  forbade and this one keeps forbidding.
- The taskfile.dev `curl | sh` installer — downloads an unpinned latest release and pipes it to a shell.
- Task in a container image built by the project — ties the build entry point to podman and a pulled base image,
  which the pipeline deliberately avoids.

## Consequences
Every deliverable is still produced by one command, locally and in CI, and the task definitions read as ordinary
shell blocks instead of make recipes with escaping. CI pays roughly twenty seconds per job to compile task from
the module proxy; the pinned version makes that reproducible and gives the workflows test something exact to
check. Contributors now need task instead of make: the README prerequisites and `AGENTS.md` say so, and a machine
without `task` fails with a clear "task: command not found" instead of a make error. The `go install` step depends
on the Go toolchain that `setup-go` provides, so the docs job gained a `setup-go` step it did not need before.
