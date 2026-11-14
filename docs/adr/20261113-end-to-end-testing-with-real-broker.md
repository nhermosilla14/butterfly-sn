# End-to-end testing against a real broker

- Status: Accepted
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: transport-and-sessions.md, topics-and-pubsub.md, configuration.md

## Context
Each slice needs an end-to-end test through the public interface (UDP in, TCP broker out). Many criteria
cannot be driven by an off-the-shelf client: malformed datagrams, unknown senders, bad client ids, exact
message sequences. `mqtt-sn-tools` (C, MIT) has no will, no packets above 255 bytes and no retransmission,
so it is useful for manual compatibility checks but not for the automated suite.

## Decision
- E2E tests live under `test/e2e` and start the gateway in-process (the same code path as `main`, with a config
  file written to a temp dir) on a random UDP port.
- The broker is a real Mosquitto (`eclipse-mosquitto:2`) started in a container through `testcontainers-go`, using
  Docker (the only container daemon on the project's machines; the user is in the `docker` group). No Podman socket
  is used. The container has a small generated config (anonymous, connection logging on) copied into it, and its mapped
  port is the broker address. Containers are removed after the tests (testcontainers cleanup). A test fails with a
  clear message when Docker is not reachable. Setting `BUTTERFLY_E2E_BROKER=<host:port>` uses an existing broker
  instead (log-based assertions are then skipped).
  The reason is that a Mosquitto host subprocess cannot be reliably cleaned up from a snap-confined `go` (AppArmor denies
  the signal) and AppArmor also blocks it from reading a config file outside `/etc/mosquitto`.
- The MQTT-SN client in tests is a small helper in `test/e2e` built on `internal/mqttsn` that sends raw
  datagrams and expects replies with timeouts. A separate observer connects to the broker as a plain MQTT client
  using `butterfly-mqtt-client` to check what arrived.
- Manual compatibility check, documented in the README: run the gateway and use `mqtt-sn-pub` from
  `mqtt-sn-tools` against it. Not automated.
- Unit tests live next to the code (codec round-trips, state machines); fuzz targets for both codecs.

## Alternatives considered
- Mosquitto as a host subprocess — rejected, see above (leaks processes under snap Go, depends on a local install).
- Driving the runtime through its CLI from the tests — no dependency, but more harness code to maintain.
- Rootless Podman with its socket service — would run a second container daemon next to Docker; rejected by the owner.
- In-process fake broker only — deterministic, but does not prove compatibility with a real broker; it may be added later for failure injection.
- `mqtt-sn-tools` as the only test client — too limited.

## Consequences
Tests need a reachable Docker daemon and the `eclipse-mosquitto` image (pulled on first run), locally and in CI.
`testcontainers-go` is a test-only dependency with a large transitive graph (its growth is to be reported when added,
since the owner dislikes dependency sprawl). Test helpers reuse the production codecs, so codec bugs are also covered
by the unit and fuzz tests plus the real broker on the other side. Later intents can add a fake broker for failure
scenarios.
