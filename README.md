# butterfly-sn

A transparent MQTT-SN v1.2 gateway written in Go. It lets UDP-based MQTT-SN clients talk to a plain MQTT
broker, replacing a patched Eclipse Paho MQTT-SN gateway. See [docs/vision.md](docs/vision.md).

Status: early development; no code yet.

## Prerequisites
- Go 1.27 or newer
- podman (container image, planned)

## Getting started
TODO: build, configure (`config.example.yaml`) and run, once the first slice exists.

## Usage
TODO: command-line flags and configuration reference.

## Project structure
```
docs/                         vision, architecture, spec, ADRs, branch records
olddocs/                      reference documentation of the legacy Paho gateway
scripts/check-branch-docs.sh  merge gate for records
.agents/skills/general-software-development/   vendored workflow skill (rules, templates)
AGENTS.md                     rules for humans and agents
```

## Development guidelines
- Workflow: git-flow (integration branch `develop`), following the loop in `AGENTS.md`: intents, ADRs, slices, code and test.
- Templates for intents, ADRs, slices and spec areas are in `.agents/skills/general-software-development/templates/`.
- Build, test, lint: `go build ./...`, `go test ./...`, `gofmt -l .`, `go vet ./...` (once code exists).

## Documentation
See [docs/README.md](docs/README.md).

## License
MIT, see [LICENSE](LICENSE).
