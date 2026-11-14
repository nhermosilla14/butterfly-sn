# Will handshake and publication

- Status: Accepted
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
A client that connects with the MQTT-SN Will flag stores a will topic and message in the gateway, and the broker
publishes that will when the client is lost abnormally or when the gateway shuts down. A normal DISCONNECT does
not publish it, and a client can connect without a will by sending an empty will topic.

## Acceptance criteria
- CONNECT with the Will flag does not get CONNACK yet: the gateway sends WILLTOPICREQ, the client answers with
  WILLTOPIC, the gateway sends WILLMSGREQ, and after WILLMSG it opens the broker connection with that will
  topic, message, QoS and retain flag. CONNACK Accepted follows only when the broker accepted the connection.
- An empty WILLTOPIC during the handshake means no will: the gateway skips WILLMSGREQ, connects to the broker
  without a will and answers CONNACK Accepted.
- A will handshake that is not completed within its timeout ends with no broker connection, no session and a
  DISCONNECT to the client.
- A client with a will that misses its keepalive is declared lost and its broker connection is closed without
  MQTT DISCONNECT, so the broker publishes the stored will. An independent subscriber receives the exact will
  topic and payload.
- On gateway shutdown, broker connections that carry a will are closed the same way, so the broker publishes
  the will. Clients still receive DISCONNECT as they do today.
- An explicit client DISCONNECT sends MQTT DISCONNECT to the broker and publishes no will.
- A broker connection lost mid-session publishes the will through the broker, and the client still receives
  DISCONNECT from the existing broker-loss handling.
- A will that asks for QoS 2 is refused with CONNACK Not supported, because QoS 2 is out of scope.
- The will topic and message are never logged.

## Spec changes
- Adds: will / Will handshake
- Adds: will / Will publication
- Changes: transport-and-sessions / Connect and disconnect
- Changes: transport-and-sessions / Broker connection loss
- Changes: transport-and-sessions / Not supported
- Changes: transport-and-sessions / Startup and shutdown

## Out of scope
- WILLTOPICUPD and WILLMSGUPD; a later intent decides how to apply an update through MQTT 3.1.1, which cannot
  change a will in place.
- Persisting will data across sessions (CleanSession=0); sleep and session resumption come later.
- Publishing wills for clients without a will, and will publication on gateway startup failures.
- QoS 2 wills, retransmission, subscriptions and downlink.

## Decisions taken
- The handshake has a fixed 10 second timeout and no new configuration key. A client that cannot finish it is
  treated like an invalid CONNECT.
- Empty WILLTOPIC deletes the will for this connection, as the spec describes, so no WILLMSGREQ follows.
- On client timeout or gateway shutdown, a session with a will closes its broker socket without MQTT
  DISCONNECT so the broker publishes it. This refines the liveness decision only for will-bearing sessions;
  will-less sessions keep the graceful DISCONNECT.
- Will updates remain "not supported" for now: the response carries return code Not supported, as the legacy
  gateway did. The v1 requirement is delivered by the follow-up intent that can reconnect safely.
