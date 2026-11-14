# butterfly-sn

A transparent MQTT-SN v1.2 gateway written in Go. It lets UDP-based MQTT-SN clients talk to a plain MQTT
broker, replacing a patched Eclipse Paho MQTT-SN gateway. See [docs/vision.md](docs/vision.md).

Status: early development (connect, registered-topic QoS 0 and QoS 1 publish, disconnect, bad-input handling,
broker keepalive, client timeouts and broker-loss handling work).

## Prerequisites
- Go 1.27 or newer
- Docker (end-to-end tests run a Mosquitto container via testcontainers-go; your user must be able to reach the Docker daemon)
- podman (container image, planned)

## Getting started
```
go build -o butterfly-sn ./cmd/butterfly-sn
cp config.example.yaml config.yaml   # edit listen and broker
./butterfly-sn -config config.yaml
```

## Usage
Flag `-config <file>` (required). Configuration keys (`listen`, `broker`, `max_clients`, `log_level`) are documented in `config.example.yaml`. Stop with SIGINT or SIGTERM.

## Manual compatibility check
The automated suite uses its own MQTT-SN helper. For a manual check with a third-party client, run the gateway
and point `mqtt-sn-pub` (or `mqtt-sn-sub`) from `mqtt-sn-tools` at its UDP port. This is not part of the
automated tests.

## Project structure
```
cmd/, internal/               the gateway
butterfly-mqtt-client/        standalone MQTT 3.1.1 client module (own go.mod)
test/e2e/                     end-to-end tests
docs/                         vision, architecture, spec, ADRs, branch records
olddocs/                      reference documentation of the legacy Paho gateway
scripts/check-branch-docs.sh  merge gate for records
.agents/skills/general-software-development/   vendored workflow skill (rules, templates)
AGENTS.md                     rules for humans and agents
```

## Development guidelines
- Workflow: git-flow (integration branch `develop`), following the loop in `AGENTS.md`: intents, ADRs, slices, code and test.
- Templates for intents, ADRs, slices and spec areas are in `.agents/skills/general-software-development/templates/`.
- Build, test, lint: `go build ./...`, `go vet ./...`, `gofmt -l .`, `go test ./...`; the client module is tested separately with `cd butterfly-mqtt-client && go test ./...`. The e2e tests need Docker.

## Documentation
See [docs/README.md](docs/README.md).

## License
MIT, see [LICENSE](LICENSE).
