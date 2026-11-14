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
- The broker is a real Mosquitto (`eclipse-mosquitto:2`) run in a container that the test harness drives through the
  container runtime's CLI (`podman` if on `PATH`, otherwise `docker`; `BUTTERFLY_E2E_RUNTIME` forces one). Rootless
  Podman is daemonless, so no socket or service is needed, and the harness never uses a runtime socket. The container
  has a uniquely named, labelled instance with a small generated config (anonymous, connection logging on) and a
  loopback-mapped port that is the broker address; it is always removed after the tests. A test fails with a clear
  message when no runtime works. Setting `BUTTERFLY_E2E_BROKER=<host:port>` uses an existing broker instead
  (log-based assertions are then skipped).
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
- `testcontainers-go` — needs a runtime socket (a Podman service or Docker, which the user cannot access and does not want running as a second daemon) and has a large transitive dependency graph.
- In-process fake broker only — deterministic, but does not prove compatibility with a real broker; it may be added later for failure injection.
- `mqtt-sn-tools` as the only test client — too limited.

## Consequences
Tests need `podman` (or `docker`) and the `eclipse-mosquitto` image (pulled on first run), locally and in CI. No new Go
dependency. The harness owns a little CLI-driving code (start, wait, logs, remove). Test helpers reuse the production
codecs, so codec bugs are also covered by the unit and fuzz tests plus the real broker on the other side. Later
intents can add a fake broker for failure scenarios.
