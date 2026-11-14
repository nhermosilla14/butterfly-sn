# Transport and sessions

How the gateway starts, accepts MQTT-SN clients over UDP, keeps one broker connection per client, and ends
sessions. Transparent mode only: the gateway does not aggregate clients or terminate their MQTT sessions.

## Behaviours

### Startup and shutdown
The gateway reads a YAML file at startup, binds the UDP listen address, and logs a startup line with the bound
address. SIGINT and SIGTERM close every broker connection, send DISCONNECT to connected clients and exit 0.
- Criteria:
  - A valid configuration starts the gateway and logs `gateway started` with its UDP address.
  - A missing or invalid configuration, including unknown keys, logs the error and exits non-zero.
  - SIGINT and SIGTERM stop the gateway with status 0, leaving no broker connections or goroutines.
- Tests: TestStartupInvalidConfig, TestSignalShutdown, TestShutdownClosesBrokerAndLeavesNoGoroutines

### Connect and disconnect
CONNECT opens a dedicated MQTT 3.1.1 connection to the broker using the MQTT-SN client id, and the client gets
CONNACK Accepted only after the broker accepted it. If the broker is unreachable or refuses, the client gets
CONNACK Congestion and no session state is kept. DISCONNECT closes the broker connection and is answered with
DISCONNECT.
- Criteria:
  - CONNECT gets CONNACK Accepted; the broker sees one connection per MQTT-SN client, with that client id.
  - When the broker address is unreachable, CONNECT gets CONNACK Congestion.
  - DISCONNECT gets DISCONNECT in reply and the broker connection is closed.
- Tests: TestConnectAccepted, TestConnectBrokerUnreachable, TestDisconnect

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
loop.
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
- Discovery (SEARCHGW, GWINFO, ADVERTISE); planned.
- Will, sleep, keepalive enforcement and client timeouts; planned. CONNECT is accepted with its duration, which
  is passed to the broker as keepalive, but the gateway does not answer PINGREQ yet.
- Persistent (non-clean) broker sessions: CONNECT is accepted, but the broker session is always clean.
- Broker connection drops mid-session and notifying the client; planned.
- Idle-slot reclamation; planned.
