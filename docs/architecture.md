# Architecture

Current state after the broker outage intent (connect, QoS 0 and QoS 1 publish in both directions, subscriptions with
wildcards and short names, gateway topic registration, disconnect, bad-input handling, client timeouts, broker
keepalive, broker-loss handling, will handling, sleep with buffered downlink and wake flush, gateway discovery, retransmission and bounded waits, and release builds with a container image). Decisions:
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
| `internal/app` | Startup path shared by `main` and the e2e tests: flags (`-config`, `-version`), config, logging, run; `Version` is set by the linker |
| `internal/config` | YAML loading, defaults, validation (unknown keys rejected) |
| `internal/mqttsn` | MQTT-SN packet codec, no I/O (CONNECT, CONNACK, REGISTER, REGACK, PUBLISH, PUBACK, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT, will messages including updates and their responses, SEARCHGW, GWINFO, ADVERTISE) |
| `internal/gateway` | UDP listener and router; one session goroutine, one broker reader goroutine and a normal-topic table per client |
| `butterfly-mqtt-client/` | Standalone MQTT 3.1.1 client module (own `go.mod`, stdlib only, no gateway concepts), joined by a `replace` directive so it can move to its own repo |
| `Makefile`, `Containerfile` | Build and release: `make dist` cross-builds static binaries, archives and checksums; `make image` assembles the scratch image from them with podman |
| `.github/workflows` | CI and release workflows; they call only Makefile targets |
| `test/image`, `test/workflows` | Image smoke test (tag `image`) and the check that workflows match the Makefile |
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
- WILLTOPICUPD and WILLMSGUPD from an active client are answered with WILLTOPICRESP and WILLMSGRESP, return code
  Not supported. Nothing else changes: the broker connection and the stored will stay as given at CONNECT. Asleep
  clients ignore them, except during a wake flush where they are answered. Unknown senders get DISCONNECT.
- SEARCHGW from any sender is answered with one unicast GWINFO (configured `gateway_id`) after a random 0-500 ms
  delay; the radius is ignored. GWINFO and ADVERTISE from clients are ignored. Discovery is handled before
  session lookup, never creates a session and never contacts the broker. A pending reply is joined by `Run`
  before the socket closes, so none outlives shutdown. A single advertisement ticker, also joined by `Run`, sends
  ADVERTISE after each full `advertise_interval` to `advertise_address` (default: broadcast on the listen port).
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
- SUBSCRIBE and UNSUBSCRIBE are forwarded on the client's broker connection with the client's msg id as the MQTT
  packet id. SUBACK reports the granted QoS, the table id for an exact name and 0 for a wildcard; UNSUBACK copies
  the msg id.
- A broker PUBLISH for a topic in the client's table becomes an MQTT-SN PUBLISH with that id. An unknown topic
  gets a gateway REGISTER and the PUBLISH is held (at most 20 per session) until REGACK; a rejected
  registration forgets the name so a later publish registers it again with a fresh id. Downlink QoS 1 reuses
  the broker packet id as msg id and acknowledges the broker only after the client PUBACK.
- A downlink message whose datagram exceeds one UDP datagram (65 507 bytes) is never recorded for retransmission:
  it is dropped and logged with sizes, a QoS 1 one is acknowledged to the broker, and an oversize REGISTER also
  forgets its topic name. The client is not disconnected for it.
- The table holds at most 50 names per client, with monotonic ids that are not reused until the counter wraps.
  Short subscriptions live in a separate set, also capped at 50; a broker PUBLISH whose two-character topic is
  short-subscribed is sent as a short PUBLISH without REGISTER. Topic names that are empty or contain wildcards
  are refused at REGISTER and dropped at PUBLISH, so they never look like a broker failure.
- Each session has one broker reader goroutine. It reads broker events in arrival order (PUBACK, PINGRESP,
  DISCONNECT, and later downlink traffic) and feeds them into the same serialized session stream, so the
  session goroutine never blocks on a synchronous broker read.
- While a session is active, the gateway keeps its broker connection alive: when half the client's CONNECT
  duration passes without a broker write, it sends an MQTT PINGREQ and expects a PINGRESP. A keepalive of 0
  disables the pings and asks the broker for no keepalive check.
- DISCONNECT with a duration puts the session to sleep: the broker connection stays open, the inactivity timer is
  replaced by a 1.5x sleep timer, and downlink PUBLISH messages are buffered (max 20, QoS 1 acknowledged to the
  broker at buffering time). A client asleep past its duration is declared lost through the same will-aware
  teardown.
- PINGREQ wakes a sleeping client: the buffer is flushed in order, unknown topics are registered first, and
  PINGRESP is the last datagram before the client sleeps again. CONNECT from sleep gets CONNACK immediately,
  flushes the same way and stays active. Buffered QoS 1 messages get gateway-assigned msg ids, so the broker is
  never acknowledged twice.
- A client that sends no valid MQTT-SN packet for 1.5 times its CONNECT duration is declared lost: the gateway
  removes the session, sends MQTT DISCONNECT to the broker, closes it and sends a best-effort MQTT-SN
  DISCONNECT to the client. A keepalive of 0 disables this timer. PINGREQ from an active client is answered
  with PINGRESP locally and counts as activity; the gateway does not originate PINGREQ to clients.
- Any broker loss (read or write error, MQTT DISCONNECT, or a PINGREQ still unanswered when the next is due)
  sends a best-effort MQTT-SN DISCONNECT to the client and removes the session, so its slot is free again and
  pending QoS 1 acknowledgements are abandoned. The broker socket is closed without another MQTT DISCONNECT.
- DISCONNECT sends MQTT DISCONNECT to the broker, replies DISCONNECT, and removes the session.
- A will-bearing session that ends without the client's consent (keepalive timeout or gateway shutdown) closes
  its broker socket without MQTT DISCONNECT, so the broker publishes the stored will. An explicit client
  DISCONNECT stays graceful; on broker loss the broker publishes the will itself.
- Every REGISTER and QoS 1 PUBLISH the gateway sends to the client (including wake-up flush and short-topic ones)
  is recorded in a per-session retry set with its exact datagram, attempt count and due time, keyed by message
  type and msg id. The session goroutine's single retry timer is armed for the earliest due entry (nil when the
  set is empty). When it fires, each due entry is sent again unchanged (PUBLISH with DUP) and due moves one
  `retry_interval` on; an entry that already used `retry_limit` retransmissions ends the session through the
  client-loss teardown, without a broker PUBACK. REGACK and client PUBACK remove entries, and sleep clears the set.
- A SUBSCRIBE or UNSUBSCRIBE forwarded to the broker records its send time. It is never retransmitted; if it is
  still unanswered after `retry_interval` times `retry_limit`, the session ends through the broker-loss path
  (DISCONNECT to the client, slot freed, broker connection closed without MQTT DISCONNECT). The same session retry
  timer covers these deadlines and the client retry deadlines. Pending state lives in session maps, so a new
  CONNECT with the same client id starts with none of it.
- On shutdown (context cancel) every session ends its broker connection (MQTT DISCONNECT, or a bare close when
  it has a will), sends DISCONNECT to its client, and the socket closes. Closing the broker connection unblocks
  the reader; the session cancels it and waits for it to return before the session ends.

## Test observer
The e2e suite uses `butterfly-mqtt-client` directly against Mosquitto for its independent observer and publisher.
The client's blocking QoS 0 `Subscribe` and `ReadPublish` observer operations exist for this purpose and are
separate from the event-driven operations the gateway uses (`SubscribeRequest`, `ReadEvent`, `Puback`).
