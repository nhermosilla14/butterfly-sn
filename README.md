# butterfly-sn

A transparent MQTT-SN v1.2 gateway written in Go. It lets UDP-based MQTT-SN clients talk to a plain MQTT
broker, replacing a patched Eclipse Paho MQTT-SN gateway. See [docs/vision.md](docs/vision.md).

Status: early development (connect, QoS 0 and QoS 1 publish in both directions, subscriptions with wildcards
and short topic names, sleep with buffered downlink and wake flush, gateway discovery, disconnect, bad-input handling, broker
keepalive, client timeouts, broker-loss handling and will handling work).

How closely it follows the standard, where it deviates and what it lacks: see [CONFORMANCE.md](CONFORMANCE.md).

## Prerequisites
- Go 1.27 or newer
- Docker (end-to-end tests run a Mosquitto container via testcontainers-go; your user must be able to reach the Docker daemon)
- task 3, tar, zip and sha256sum (release builds; `go install github.com/go-task/task/v3/cmd/task@v3.54.0`)
- podman, rootless, CLI only (container image; the Podman API socket is never used)

## Getting started
```
task build                           # bin/butterfly-sn, or: go build -o butterfly-sn ./cmd/butterfly-sn
cp config.example.yaml config.yaml   # edit listen and broker
bin/butterfly-sn -config config.yaml
```

## Usage
Flag `-config <file>` (required) and `-version`, which prints `butterfly-sn <version>` and exits. Configuration keys (`listen`, `broker`, `max_clients`, `log_level`, `gateway_id`, `advertise_interval`, `advertise_address`, `retry_interval`, `retry_limit`) are documented in `config.example.yaml`. Stop with SIGINT or SIGTERM.

## Building and releasing
`task --list` lists the tasks:
```
task build        # host binary in bin/
task dist         # static binaries for linux/amd64, linux/arm64 and windows/amd64, archives and dist/SHA256SUMS
task image        # multi-arch (amd64, arm64) scratch image butterfly-sn:<version>, built with podman
task image-test   # build the host-arch image and smoke-test it with podman
task test         # formatting, vet, build and the whole test suite (the e2e part needs Docker)
task check        # task test plus the merge gate for docs/branch
task clean        # remove dist/ and bin/
```
`task dist` is reproducible: the same commit gives byte-identical archives. Verify a download with
`sha256sum -c SHA256SUMS`. `VERSION` defaults to `git describe --tags --always --dirty`; override it with
`task dist VERSION=v1.2.3`. `task image` builds from `dist/`, so `DIST` must stay `dist`. It leaves untagged
per-platform images in podman's store; remove them with `podman image prune` if you do not want them.

To publish a release, tag a commit on `main` with `vX.Y.Z` and push the tag. The `Release` workflow tests,
builds `task dist` and attaches the archives and `SHA256SUMS` to a GitHub release (tags containing a hyphen are
prereleases). The workflows have not run on GitHub yet because the repository has no remote.

## Container
The image is `FROM scratch` with only the static binary, runs as UID 65534 and reads
`/etc/butterfly-sn/config.yaml`. Mount your configuration and publish the gateway's UDP port:
```
podman run --rm \
  -v "$PWD/config.yaml:/etc/butterfly-sn/config.yaml:ro" \
  -p 5684:5684/udp \
  butterfly-sn:<version>
```
Set `listen: 0.0.0.0:5684` in the mounted file, and point `broker` at an address reachable from the container
(for example the host's address, not `127.0.0.1`). SEARCHGW replies are unicast, so port publishing is enough
for discovery by search. ADVERTISE is sent as a broadcast on the container's network, so clients that listen for
advertisements need `--network host`. The arm64 image is built but not run by the test suite.

## Manual compatibility check
The automated suite uses its own MQTT-SN helper. For a manual check with a third-party client, run the gateway
and point `mqtt-sn-pub` (or `mqtt-sn-sub`) from `mqtt-sn-tools` at its UDP port. This is not part of the
automated tests.

## Project structure
```
cmd/, internal/               the gateway
butterfly-mqtt-client/        standalone MQTT 3.1.1 client module (own go.mod)
test/e2e/                     end-to-end tests
test/image/                   container image smoke test (build tag `image`, run by `task image-test`)
test/workflows/               checks that the CI workflows call existing tasks
Taskfile.yml, Dockerfile      build, release and image definitions
.github/workflows/            CI and release workflows
docs/                         vision, architecture, spec, ADRs, branch records
scripts/check-branch-docs.sh  merge gate for records
.agents/skills/general-software-development/   vendored workflow skill (rules, templates)
AGENTS.md                     rules for humans and agents
```

## Development guidelines
- Workflow: git-flow (integration branch `develop`), following the loop in `AGENTS.md`: intents, ADRs, slices, code and test.
- Templates for intents, ADRs, slices and spec areas are in `.agents/skills/general-software-development/templates/`.
- Build, test, lint: `task test` runs formatting, `go vet`, `go build` and the root and client test suites; the e2e tests need Docker. `task check` adds the merge gate for `docs/branch`, which fails while a branch still carries records.

## Documentation
See [docs/README.md](docs/README.md).

## License
MIT, see [LICENSE](LICENSE).
