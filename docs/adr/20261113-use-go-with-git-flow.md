# Use Go with git-flow

- Status: Accepted
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla

## Context
The gateway replaces a C++ implementation that is hard to maintain and awkward to run on Windows
(Cygwin). Goals: one static binary, native Linux and Win64 builds, goroutine-based concurrency,
memory safety. This is a personal project published on GitHub; the owner chose git-flow for it.

## Decision
Implement in Go (current stable toolchain, standard library first; every third-party dependency needs
its own ADR). Use git-flow (`git-flow-workflow` skill): `develop` is the integration branch, feature and fix branches
merge into it, `main` holds releases. Commits use the personal git identity. License: MIT; code is written from the spec and from
the old gateway's behaviour, never copied from Paho (EPL/EDL).

## Alternatives considered
- Rust — strong fit for robustness, but the owner is currently learning Go and wants it as the default for services.
- Keeping and fixing the C++ gateway — does not address maintainability or the Windows story.

## Consequences
Cross-compilation is trivial. The team must keep third-party dependencies minimal.
