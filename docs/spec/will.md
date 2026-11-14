# Will

How a client's will topic and message travel from the MQTT-SN handshake into the broker connection, and when the
broker publishes the will.

## Behaviours

### Will handshake
A CONNECT with the Will flag runs WILLTOPICREQ, WILLTOPIC, WILLMSGREQ and WILLMSG before the gateway contacts the
broker. The collected topic, payload, QoS and retain flag are carried in the broker CONNECT, and CONNACK follows
only after the broker accepted. An empty WILLTOPIC means no will. QoS 2 wills are refused. The handshake has a
fixed 10 second timeout.
- Criteria:
  - The prompts arrive in order and the broker CONNECT carries the exact will topic, payload, QoS and retain.
  - CONNACK Accepted is sent only after the broker accepted the connection.
  - Empty WILLTOPIC skips WILLMSGREQ and connects without a will.
  - QoS 2 gets CONNACK Not supported and no broker connection or session is left behind.
  - A client DISCONNECT during the handshake ends the session without broker contact.
  - An unanswered handshake gets DISCONNECT after the timeout, with no broker contact and a free slot.
  - CONNECT without the Will flag still contacts the broker immediately.
- Tests: TestWillHandshakeOrderAndBrokerWill, TestWillHandshakeQoS1Retain, TestWillEmptyTopicSkipsWillMsgReq, TestWillEmptyTopicConnectsWithoutWill, TestWillQoS2RefusedWithoutBrokerContact, TestWillQoS2RefusedThenConnectAccepted, TestWillDisconnectDuringHandshakeEndsSession, TestWillHandshakeTimeoutFreesSlot, TestConnectWithoutWillConnectsImmediately

### Will publication
The broker publishes the stored will when the client is lost abnormally or the gateway shuts down. The gateway
closes a will-bearing broker connection without MQTT DISCONNECT in those cases and never publishes the will
itself. An explicit client DISCONNECT stays graceful and publishes nothing.
- Criteria:
  - A client that misses its keepalive has its broker connection closed bare; an independent subscriber gets
    the exact will topic and payload.
  - Gateway shutdown closes will-bearing broker connections the same way, clients get DISCONNECT and the
    gateway exits 0.
  - A broker connection lost mid-session publishes the will through the broker; the client gets DISCONNECT.
  - Explicit client DISCONNECT publishes no will.
  - A client that connected with an empty will topic publishes nothing when it is lost.
  - Will-less sessions keep MQTT DISCONNECT on timeout and shutdown.
- Tests: TestWillPublishedOnClientTimeout, TestWillPublishedOnGatewayShutdown, TestWillPublishedOnBrokerLoss, TestWillNotPublishedOnExplicitDisconnect, TestEmptyWillTopicPublishesNothing, TestClientTimeoutWithWillClosesBrokerWithoutDisconnect, TestClientTimeoutWithoutWillSendsDisconnect, TestShutdownWithWillClosesBrokerWithoutDisconnect, TestShutdownWithoutWillSendsDisconnect, TestExplicitDisconnectWithWillSendsDisconnect

## Not supported
- WILLTOPICUPD and WILLMSGUPD; MQTT 3.1.1 cannot change a will in place, so a later intent must design the
  reconnect path.
- Persisting will data across sessions (CleanSession=0); planned with sleep and session resumption.
- Publishing wills when the gateway crashes or fails at startup; only timeout, broker loss and clean shutdown
  are covered.
- QoS 2 wills: the handshake refuses them with CONNACK Not supported.
