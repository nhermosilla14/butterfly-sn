# Architecture

Current state after slice 01 of the will intent (connect, registered-topic QoS 0 and QoS 1 publish, disconnect,
bad-input handling, client timeouts, broker keepalive, broker-loss handling and the will handshake). Decisions:
`docs/adr/` and `docs/branch/adr/`.

```
 MQTT-SN client --UDP--> [ router ] --inbox--> [ session goroutine ] --TCP--> MQTT broker
                       (map by sender addr)    owns all session state      (one connection per client)
                                                ^      |                      |
                                                |      +-- PUBLISH (writes) --+
                                                |                             |
                                                +------- broker reader <------+
                                                         (PUBACK, PINGRESP,
                                                          DISCONNECT, errors)
```

## Packages
| Package | Role |
|---|---|
| `cmd/butterfly-sn` | `main`: signals (SIGINT, SIGTERM), calls `internal/app` |
| `internal/app` | Startup path shared by `main` and the e2e tests: flags, config, logging, run |
| `internal/config` | YAML loading, defaults, validation (unknown keys rejected) |
| `internal/mqttsn` | MQTT-SN packet codec, no I/O (CONNECT, CONNACK, REGISTER, REGACK, PUBLISH, PUBACK, PINGREQ, PINGRESP, DISCONNECT, WILLTOPICREQ, WILLTOPIC, WILLMSGREQ, WILLMSG) |
| `internal/gateway` | UDP listener and router; one session goroutine, one broker reader goroutine and a normal-topic table per client |
| `butterfly-mqtt-client/` | Standalone MQTT 3.1.1 client module (own `go.mod`, stdlib only, no gateway concepts), joined by a `replace` directive so it can move to its own repo |
| `test/e2e` | End-to-end tests: gateway in-process, real Mosquitto in a container (testcontainers-go, Docker), plain MQTT observer |

## Flow
- `gateway.New` binds the UDP socket; `Run` reads datagrams, decodes them and routes by sender address. A
  datagram that does not decode is logged and ignored.
- A CONNECT from an unknown address opens a session (limited by `max_clients`). An empty client id is
  rejected with DISCONNECT without creating a session or contacting the broker. The session goroutine dials
  the broker with a timeout, sends MQTT CONNECT, and answers CONNACK Accepted, or Congestion if anything
  failed.
- A CONNECT with the Will flag runs the will handshake before any broker contact: WILLTOPICREQ, WILLTOPIC,
  WILLMSGREQ, WILLMSG. An empty will topic skips the message prompt and connects without a will; a QoS 2 will
  gets CONNACK Not supported. The whole handshake has a 10 second timeout; on timeout the session is removed
  and the client gets DISCONNECT. The collected will travels in the broker CONNECT.
- Any other valid packet from an address with no live session gets DISCONNECT; DISCONNECT itself is ignored
  there to avoid an answer loop. This is also the case for a client whose session just ended.
- REGISTER adds the exact topic name to the session's table and answers REGACK. New ids are allocated from 1 to
  0xFFFE; registering the same name returns the existing id.
- A QoS 0 PUBLISH with a normal, known topic id becomes an MQTT QoS 0 PUBLISH with the registered topic name and
  unchanged payload. An unknown id is dropped without a broker write or client acknowledgement.
- A QoS 1 PUBLISH with a normal, known topic id becomes an MQTT QoS 1 PUBLISH that reuses the client's 16-bit
  msg id as the MQTT packet id on that client's dedicated connection. The session records the outstanding
  id together with its topic id and answer PUBACK Accepted to the client only after the broker PUBACK for
  that id arrives. Broker acknowledgements with an unknown id are logged and ignored.
- Each session has one broker reader goroutine. It reads broker events in arrival order (PUBACK, PINGRESP,
  DISCONNECT, and later downlink traffic) and feeds them into the same serialized session stream, so the
  session goroutine never blocks on a synchronous broker read.
- While a session is active, the gateway keeps its broker connection alive: when half the client's CONNECT
  duration passes without a broker write, it sends an MQTT PINGREQ and expects a PINGRESP. A keepalive of 0
  disables the pings and asks the broker for no keepalive check.
- A client that sends no valid MQTT-SN packet for 1.5 times its CONNECT duration is declared lost: the gateway
  removes the session, sends MQTT DISCONNECT to the broker, closes it and sends a best-effort MQTT-SN
  DISCONNECT to the client. A keepalive of 0 disables this timer. PINGREQ from an active client is answered
  with PINGRESP locally and counts as activity; the gateway does not originate PINGREQ to clients.
- Any broker loss (read or write error, MQTT DISCONNECT, or a PINGREQ still unanswered when the next is due)
  sends a best-effort MQTT-SN DISCONNECT to the client and removes the session, so its slot is free again and
  pending QoS 1 acknowledgements are abandoned. The broker socket is closed without another MQTT DISCONNECT.
- DISCONNECT sends MQTT DISCONNECT to the broker, replies DISCONNECT, and removes the session.
- On shutdown (context cancel) every session disconnects from the broker, sends DISCONNECT to its client, and the
  socket closes. Closing the broker connection unblocks the reader; the session cancels it and waits for it to
  return before the session ends.

## Test observer
The e2e suite uses `butterfly-mqtt-client` directly against Mosquitto for its independent observer. The client's
QoS 0 SUBSCRIBE and incoming PUBLISH operations exist for this purpose; the gateway does not yet handle client
subscriptions or broker-to-client delivery.
