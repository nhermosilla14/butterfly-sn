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

### Will update requests
WILLTOPICUPD (including the empty form that asks to delete the will) and WILLMSGUPD are answered with
WILLTOPICRESP and WILLMSGRESP, return code Not supported. MQTT-SN 1.2 (6.4) says the stored will is overwritten and
both messages are acknowledged, with a rejection reason allowed. The gateway cannot honour an overwrite, because
MQTT 3.1.1 fixes the will at connect, so it rejects and leaves everything untouched. The will contents are never
logged.
- Criteria:
  - Each update message gets its response with Not supported and the session stays usable: publish reaches the
    broker, PINGREQ is answered, the broker connection is not reopened.
  - The will given at CONNECT is still the one the broker publishes when the client is lost.
  - During the will handshake and while the client sleeps the messages are ignored; during a wake-up flush they
    are answered. From an unknown sender they get DISCONNECT, and a malformed one is ignored.
- Tests: TestWillUpdateRejectedNotSupported, TestWillUpdateKeepsSessionAndWill, TestWillUpdateDuringWillHandshakeIgnored, TestWillUpdateAsleepIgnored, TestWillUpdateAnsweredDuringWakeFlush, TestWillUpdateFromUnknownSenderDisconnects, TestWillUpdateMalformedIgnored, TestWillUpdateExactBytes

## Not supported
- Applying will updates (WILLTOPICUPD, WILLMSGUPD): reconnecting the broker connection with a new will, or the
  gateway publishing the will itself. Revisit only with a new intent and ADR.
- Persisting will data across sessions (CleanSession=0); planned with sleep and session resumption.
