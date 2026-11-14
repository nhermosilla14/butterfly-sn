# End-to-end testing against a real broker

- Status: Proposed
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Spec area: transport-and-sessions.md, topics-and-pubsub.md, configuration.md

## Context
Each slice needs an end-to-end test through the public interface (UDP in, TCP broker out). Many criteria
cannot be driven by an off-the-shelf client: malformed datagrams, unknown senders, bad client ids, exact
message sequences. `mqtt-sn-tools` (C, MIT) has no will, no packets above 255 bytes and no retransmission,
so it is useful for manual compatibility checks but not for the automated suite.

## Decision
- E2E tests live under `test/e2e` and start the gateway in-process (the same code path as `main`, with a config
  file written to a temp dir) on a random UDP port.
- The broker is a real Mosquitto started as a subprocess on a random port with a generated minimal config
  (anonymous, loopback only). A test fails with a clear message when `mosquitto` is not on `PATH`; CI installs it.
  Setting `BUTTERFLY_E2E_BROKER=<host:port>` uses an existing broker instead.
- The MQTT-SN client in tests is a small helper in `test/e2e` built on `internal/mqttsn` that sends raw
  datagrams and expects replies with timeouts. A separate observer connects to the broker as a plain MQTT client
  using `internal/mqtt` to check what arrived.
- Manual compatibility check, documented in the README: run the gateway and use `mqtt-sn-pub` from
  `mqtt-sn-tools` against it. Not automated.
- Unit tests live next to the code (codec round-trips, state machines); fuzz targets for both codecs.

## Alternatives considered
- In-process fake broker only — deterministic, but does not prove compatibility with a real broker; it may be added later for failure injection.
- `testcontainers` or podman-driven brokers — slower and adds a dependency.
- `mqtt-sn-tools` as the only test client — too limited.

## Consequences
Tests depend on Mosquitto being installed locally and in CI. Test helpers reuse the production codecs, so
codec bugs are also covered by the unit and fuzz tests plus the real broker on the other side. Later intents can
add a fake broker for failure scenarios.
