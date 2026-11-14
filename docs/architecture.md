# Architecture

Current state after slice 01 (connect and disconnect). Decisions: `docs/adr/` and `docs/branch/adr/`.

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
| `internal/mqttsn` | MQTT-SN packet codec, no I/O (CONNECT, CONNACK, DISCONNECT so far) |
| `internal/gateway` | UDP listener and router; one session goroutine per client |
| `butterfly-mqtt-client/` | Standalone MQTT 3.1.1 client module (own `go.mod`, stdlib only, no gateway concepts), joined by a `replace` directive so it can move to its own repo |
| `test/e2e` | End-to-end tests: gateway in-process, real Mosquitto in a container (testcontainers-go, Docker) |

## Flow
- `gateway.New` binds the UDP socket; `Run` reads datagrams, decodes them and routes by sender address.
- A CONNECT from an unknown address opens a session (limited by `max_clients`). The session goroutine dials the
  broker with a timeout, sends MQTT CONNECT, and answers CONNACK Accepted, or Congestion if anything failed.
- DISCONNECT sends MQTT DISCONNECT to the broker, replies DISCONNECT, and removes the session.
- On shutdown (context cancel) every session disconnects from the broker, sends DISCONNECT to its client, and the
  socket closes.
