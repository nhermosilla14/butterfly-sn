# Release on every merge to main

- Status: Accepted
- Date: 2026-11-16
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: operations.md

## Context
Releases were tag-driven: the pipeline ADR left the release step to `Release` on tags matching `v*`, and the README
told the maintainer to tag `main` by hand with `vX.Y.Z`. The repository has no tags yet and never published
anything. The maintainer wants each merge to `main` to produce a downloadable release with no manual step, since
the binaries are the whole product and the Go build is reproducible enough to publish per merge.

## Decision
- **Trigger.** `Release` runs on `push` to `main` and nothing else. The tag trigger is removed, so there is exactly
  one event that produces a release: merging or pushing to `main`.
- **Version.** The release version is the highest `v*` tag with its patch incremented; `v0.1.0` when no such tag
  exists. `git tag --sort=-v:refname` picks the highest tag, so a hand-made `v1.2` or `v2.0.0-rc1` degrades to
  `v1.2.1` and `v2.0.1` instead of failing. The rule lives in the `next-version` task, not in workflow shell, so it
  can be run and checked locally.
- **Stamping.** The version is passed to `task dist` through the `VERSION` environment variable, which the Taskfile
  variable now honours, so the binary reports the released version rather than a commit hash. The tag itself is
  created by `gh release create --target "$GITHUB_SHA"`, on the merge commit that triggered the run.
- **Artifacts.** The release carries the same three archives `task dist` always builds — `linux_amd64`,
  `linux_arm64`, `windows_amd64` — plus `SHA256SUMS`. Each holds one statically linked binary with
  `LICENSE` and `config.example.yaml`, so nothing else has to be installed to run it.
- **Ordering.** The job runs the full test suite before building, and a `concurrency` group serialises releases:
  the queued run checks out after the previous one finished, so it sees the tag the previous run created and
  computes the following patch version.
- **Enforcement.** `test/workflows` asserts that `Release` triggers on `main` only, never on tags, that it installs
  task with a pinned version, and that it takes the version from `next-version` and the artifacts from `dist`.

## Alternatives considered
- Workflow run number as the version (`v0.1.<run>`) — monotonic but not semantic, and skipped runs leave gaps.
- Calendar versions — readable but break semver tooling and still need a counter within a day.
- Counting commits since the last release — cheap but noisy, and every rebase or merge style changes the number.
- Creating the tag in the workflow with `git tag` and pushing it — a second credentials path and a chance to push a
  tag that the release then fails to create; `gh release create --target` does both in one step.
- Keeping the tag trigger as well — two release paths, near-duplicate releases and an ambiguous "latest".
- A release per PR merge instead of per push to main — same thing in this repo, and `main` pushes also cover
  direct commits.

## Consequences
Merging anything into `main` publishes a release, including documentation-only merges, so versions are cheap and
`v0.x` numbering is honest about that. Moving to a new minor or major is still a manual tag, which the next merge
then increments. Undoing a bad release means deleting it on GitHub rather than reverting a tag trigger. Release
notes come from `gh --generate-notes`, so the quality of a release page depends on merge commits and PR titles.
The first release lands as `v0.1.0` on the first merge after this change.
