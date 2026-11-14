# Architecture

Current state after slice 02 (connect, registered-topic QoS 0 publish and disconnect). Decisions: `docs/adr/` and `docs/branch/adr/`.

```
 MQTT-SN client --UDP--> [ router ] --inbox--> [ session goroutine ] --TCP--> MQTT broker
                          (map by sender addr)    owns all session state      (one connection per client)
```

## Packages
| Package | Role |
|---|---|
| `cmd/butterfly-sn` | `main`: signals (SIGINT, SIGTERM), calls `internal/app` |
| `internal/app` | Startup path shared by `main` and the e2e tests: flags, config, logging, run |
| `internal/config` | YAML loading, defaults, validation (unknown keys rejected) |
| `internal/mqttsn` | MQTT-SN packet codec, no I/O (CONNECT, CONNACK, REGISTER, REGACK, PUBLISH, DISCONNECT) |
| `internal/gateway` | UDP listener and router; one session goroutine and normal-topic table per client |
| `butterfly-mqtt-client/` | Standalone MQTT 3.1.1 client module (own `go.mod`, stdlib only, no gateway concepts), joined by a `replace` directive so it can move to its own repo |
| `test/e2e` | End-to-end tests: gateway in-process, real Mosquitto in a container (testcontainers-go, Docker), plain MQTT observer |

## Flow
- `gateway.New` binds the UDP socket; `Run` reads datagrams, decodes them and routes by sender address.
- A CONNECT from an unknown address opens a session (limited by `max_clients`). The session goroutine dials the
  broker with a timeout, sends MQTT CONNECT, and answers CONNACK Accepted, or Congestion if anything failed.
- REGISTER adds the exact topic name to the session's table and answers REGACK. New ids are allocated from 1 to
  0xFFFE; registering the same name returns the existing id.
- A QoS 0 PUBLISH with a normal, known topic id becomes an MQTT QoS 0 PUBLISH with the registered topic name and
  unchanged payload. An unknown id is dropped without a broker write or client acknowledgement.
- DISCONNECT sends MQTT DISCONNECT to the broker, replies DISCONNECT, and removes the session.
- On shutdown (context cancel) every session disconnects from the broker, sends DISCONNECT to its client, and the
  socket closes.

## Test observer
The e2e suite uses `butterfly-mqtt-client` directly against Mosquitto for its independent observer. The client's
QoS 0 SUBSCRIBE and incoming PUBLISH operations exist for this purpose; the gateway does not yet handle client
subscriptions or broker-to-client delivery.
