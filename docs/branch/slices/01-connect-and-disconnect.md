# Connect and disconnect through the gateway

- Status: In progress
- Intents: 20261113-walking-skeleton
- ADRs: 20261113-package-layout-and-session-concurrency, 20261113-own-minimal-mqtt-client, 20261113-yaml-configuration, 20261113-end-to-end-testing-with-real-broker

## Scope
The gateway starts from a YAML file, listens on UDP, and a client can CONNECT, get a CONNACK after the broker
accepted a dedicated connection for it, and DISCONNECT. This is the walking skeleton: it proves the whole path
(UDP, session, broker, test harness) with the smallest set of packets.

## Layers touched
- Foundation: Go module and `butterfly-mqtt-client` module with `replace`, `internal/config`, logging to stdout (`log/slog`), signal handling in `cmd/butterfly-sn`, CI-less `go test` setup.
- Services: `internal/mqttsn` codec for CONNECT, CONNACK, DISCONNECT (plus the length header); `butterfly-mqtt-client` CONNECT, CONNACK, DISCONNECT with framed reads and connect timeout; `internal/gateway` router and session with the states needed here.
- Interface: the UDP socket and the config file. `test/e2e` harness: Mosquitto subprocess, gateway in-process, raw-UDP MQTT-SN helper.

## End-to-end test
- Valid config starts the gateway; missing or invalid config exits non-zero.
- CONNECT gets CONNACK Accepted; the Mosquitto log (connection messages enabled in the generated config) shows one new connection with the client id.
- Broker address pointing to a closed port: CONNECT gets CONNACK Congestion.
- DISCONNECT gets DISCONNECT and the broker connection closes.
- SIGINT and SIGTERM close broker connections and exit 0.
- Covers criteria: startup, CONNECT/CONNACK, unreachable broker, DISCONNECT, signals, end-to-end test.

## Out of scope
- REGISTER and PUBLISH, unknown-sender handling, client-id validation, malformed-datagram handling.
