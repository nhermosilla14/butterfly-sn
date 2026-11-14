# YAML configuration library

- Status: Accepted
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: configuration.md

## Context
The configuration moves from the old `gateway.conf` key=value format to YAML. The Go standard library has no
YAML support. The skeleton needs four keys: `listen`, `broker`, `max_clients`, `log_level`.

## Decision
Use `go.yaml.in/yaml/v3` (the YAML organisation's maintained continuation of `gopkg.in/yaml.v3`), pure Go,
decoding into a struct with unknown keys rejected (`KnownFields(true)`) so typos fail at start-up. Defaults
and validation live in `internal/config`; the gateway exits non-zero on an invalid file. Verified on the Go module proxy: latest is v3.0.6 (2026-10-13); `gopkg.in/yaml.v3` last released v3.0.1 in 2022.

## Alternatives considered
- `gopkg.in/yaml.v3` — unmaintained upstream.
- JSON or TOML — would avoid YAML, but the user chose YAML.
- Flags and environment variables only — rejected, since a file is the chosen configuration format.

## Consequences
One small pure-Go dependency, compatible with static builds. Config schema documentation will grow in the
configuration spec area.
