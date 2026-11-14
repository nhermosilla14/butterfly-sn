# Transport and sessions

How the gateway starts, accepts MQTT-SN clients over UDP, keeps one broker connection per client, and ends
sessions. Transparent mode only: the gateway does not aggregate clients or terminate their MQTT sessions.

## Behaviours

### Startup and shutdown
The gateway reads a YAML file at startup, binds the UDP listen address, and logs a startup line with the bound
address. SIGINT and SIGTERM close every broker connection, send DISCONNECT to connected clients and exit 0.
Will-bearing connections are closed so the broker publishes their will (see [will.md](will.md)).
- Criteria:
  - A valid configuration starts the gateway and logs `gateway started` with its UDP address.
  - A missing or invalid configuration, including unknown keys, logs the error and exits non-zero.
  - SIGINT and SIGTERM stop the gateway with status 0, leaving no broker connections or goroutines.
- Tests: TestStartupInvalidConfig, TestSignalShutdown, TestShutdownClosesBrokerAndLeavesNoGoroutines

### Connect and disconnect
CONNECT opens a dedicated MQTT 3.1.1 connection to the broker using the MQTT-SN client id, and the client gets
CONNACK Accepted only after the broker accepted it. If the broker is unreachable or refuses, the client gets
CONNACK Congestion and no session state is kept. A CONNECT with the Will flag runs the will handshake before the
broker is contacted (see [will.md](will.md)). DISCONNECT without a duration closes the broker connection and is
answered with DISCONNECT; DISCONNECT with a duration puts the session to sleep instead (see [sleep.md](sleep.md)).
The CONNECT duration is the client keepalive; 0 disables supervision.
- Criteria:
  - CONNECT gets CONNACK Accepted; the broker sees one connection per MQTT-SN client, with that client id.
  - When the broker address is unreachable, CONNECT gets CONNACK Congestion.
  - DISCONNECT gets DISCONNECT in reply and the broker connection is closed.
- Tests: TestConnectAccepted, TestConnectBrokerUnreachable, TestDisconnect

### Keepalive and liveness
The CONNECT duration is the client keepalive. The gateway answers PINGREQ from an active client with PINGRESP and
counts every valid packet as activity. It keeps each broker connection alive on its own with MQTT PINGREQ at
half the keepalive and declares a client lost after 1.5 times its keepalive with no packet. Keepalive 0
disables all of this.
- Criteria:
  - PINGREQ from a connected client gets PINGRESP, is not forwarded to the broker, and the session stays usable
    for later publishes.
  - A client that only sends PINGREQ stays connected across several keepalive periods, and a later QoS 1
    publish still reaches the broker because the gateway pings the broker itself.
  - Periodic PINGREQ keeps a client alive past the inactivity limit.
  - A client silent beyond 1.5 times its keepalive gets a best-effort DISCONNECT and its session ends; a later
    packet from it gets DISCONNECT. A stored will is published by the broker (see [will.md](will.md)).
  - With keepalive 0 the client is never timed out and the gateway sends no broker PINGREQ; PINGREQ is still
    answered and publishing still works.
  - While a client sleeps, the sleep timer replaces the active inactivity timer (see [sleep.md](sleep.md)).
- Tests: TestClientPingGetsPingresp, TestPingreqGetsPingrespAndSessionSurvives, TestIdleClientSurvivesBrokerKeepalive, TestPeriodicPingreqKeepsClientAlive, TestSilentClientExpiresAndFreesSlot, TestSilentClientIsDisconnectedAfterInactivityLimit, TestZeroKeepaliveNeverExpires, TestZeroDurationNeverTimesOut, TestKeepaliveZeroSendsNoPingreq

### Broker connection loss
When the broker connection ends, the client is told instead of being left to fail later. Any loss cause ends
the session and frees its slot: TCP close or read error, a failed broker write, an MQTT DISCONNECT, or a
PINGREQ the broker never answers, or a SUBSCRIBE or UNSUBSCRIBE it leaves unanswered for `retry_interval` times
`retry_limit` (it is never retransmitted: MQTT over TCP does not retransmit). Pending QoS 1 acknowledgements are
abandoned and every pending exchange is released with the session, so a new CONNECT with the same client id starts
with no leftover state. A stored will is published by the broker. The session teardown, loss cause and client identity are logged, never payload bytes.
- Criteria:
  - A dropped or reset broker connection, or an MQTT DISCONNECT from the broker, gives the client DISCONNECT;
    later packets from it get DISCONNECT.
  - A broker PINGREQ left unanswered gives the client DISCONNECT.
  - With `max_clients` reached, the slot is accepted for a new client once the session loses its broker.
  - A failed QoS 0 PUBLISH, QoS 1 PUBLISH or PINGREQ write is reported as a loss.
  - A SUBSCRIBE or UNSUBSCRIBE the broker does not answer within `retry_interval` times `retry_limit` gives the
    client DISCONNECT and frees the slot, also when the client is asleep; one answered in time leaves the session
    alive.
  - After a loss with pending REGISTER, SUBSCRIBE and downlink state, the same client id connects again with
    none of it: the first downlink needs a new REGISTER and old REGACK or SUBACK ids are ignored.
- Tests: TestBrokerRequestSubscribeUnansweredDisconnectsAndFreesSlot, TestBrokerRequestUnsubscribeUnansweredDisconnectsAndFreesSlot, TestBrokerRequestAnsweredInsideBudgetKeepsSession, TestBrokerRequestPendingWhileSleepingStillTimesOut, TestTeardownReconnectStartsClean, TestBrokerDropDisconnectsClient, TestBrokerDropFreesClientSlot, TestBrokerDisconnectPacketDisconnectsClient, TestBrokerCloseDisconnectsClient, TestBrokerResetDisconnectsClient, TestBrokerPingTimeoutDisconnectsClient, TestPublishQoS0WriteFailureIsError, TestPublishQoS1WriteFailureIsError, TestPingreqWriteFailureIsError

### Client identity
Client ids are whole strings matched exactly and case-sensitively. A client id that is only a prefix of another
connected id gets its own session and broker connection. An empty client id makes CONNECT invalid.
- Criteria:
  - Two connected clients whose ids differ only by a prefix both work independently.
  - CONNECT with an empty client id gets DISCONNECT without creating a session or contacting the broker.
  - A valid CONNECT from the same address after a rejected one is accepted.
- Tests: TestClientIDPrefixCreatesIndependentSession, TestEmptyClientIDRejected

### Unknown or disconnected senders
A valid non-CONNECT packet from an address with no live session is answered with DISCONNECT, whether the sender
never connected or its session already ended. DISCONNECT from an unknown sender is ignored to avoid an answer
loop. Discovery packets (SEARCHGW, GWINFO, ADVERTISE) are handled before this rule and never get DISCONNECT
(see [discovery.md](discovery.md)).
- Criteria:
  - A REGISTER from a fresh address gets DISCONNECT.
  - A client that completed DISCONNECT and sends another non-CONNECT gets DISCONNECT.
- Tests: TestUnknownSenderGetsDisconnect, TestDisconnectedSenderGetsDisconnect

### Malformed datagrams
Malformed input never crashes the gateway and never gets a reply.
- Criteria:
  - Zero-length datagrams, datagrams shorter than their declared length, and oversized invalid datagrams are
    logged and ignored.
  - A valid CONNECT from the same socket works afterwards.
  - Decoding is panic-free for arbitrary input.
- Tests: TestMalformedDatagramsIgnored, TestDecodeErrors, FuzzDecode

## Not supported
- Will persistence across sessions; planned in a later intent. Will updates are rejected (see [will.md](will.md)).
- Gateway-originated PINGREQ towards clients: supervision is passive, based on packets the client sends.
- Persistent (non-clean) broker sessions: CONNECT is accepted, but the broker session is always clean.
- Reconnection or session resumption for the same client id; the client sends CONNECT again.
