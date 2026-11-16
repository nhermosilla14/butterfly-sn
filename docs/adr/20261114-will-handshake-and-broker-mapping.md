# Will handshake before the broker connection

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: will.md, transport-and-sessions.md

## Context
A MQTT-SN client that sets the Will flag expects the gateway to ask for a will topic and a will message before
the connection completes. The broker connection must carry that will from its first CONNECT, because MQTT 3.1.1
has no way to add one later. Today the session dials the broker and answers CONNACK as soon as CONNECT arrives,
and `butterfly-mqtt-client` has no will fields at all.

## Decision
- CONNECT with the Will flag creates the session and its slot but does not dial the broker. The session enters a
  handshake phase owned by the existing session goroutine:
  1. The gateway sends WILLTOPICREQ.
  2. The client answers WILLTOPIC with the will topic, QoS and retain flag.
  3. The gateway sends WILLMSGREQ and the client answers WILLMSG with the payload.
  4. Only then does the gateway dial the broker and send CONNECT with the will.
- The handshake has a fixed 10 second timeout, with no new configuration key. A timeout ends the session with no
  broker connection and a best-effort DISCONNECT to the client, freeing the slot.
- Packets other than the expected will message are ignored and logged during the handshake; DISCONNECT from the
  client ends it. A later packet after a timeout follows the unknown-sender rule.
- WILLTOPIC flags carry QoS and retain. QoS 0 and 1 are accepted; QoS 2 gets CONNACK Not supported, no broker
  connection and no session, since QoS 2 is out of scope.
- An empty WILLTOPIC means the client wants no will for this connection: the gateway skips WILLMSGREQ, dials the
  broker without a will and answers CONNACK Accepted after the broker accepted.
- `butterfly-mqtt-client` gains optional will fields in `ConnectOptions`: topic, payload, QoS (0 or 1) and
  retain. CONNECT sets the Will flag, the QoS bits and the Retain bit, and appends the will topic and message
  after the client id, in MQTT 3.1.1 order. The library validates that an empty topic does not carry will
  fields and that QoS is 0 or 1.
- The session keeps `hasWill`, topic, payload, QoS and retain as its own state; the broker reader and the
  router never see them. The topic and payload are never logged.
- CONNACK is sent only after the broker CONNACK, as today. A broker failure still answers CONNACK Congestion.
- CleanSession=0 remains treated as clean: will data is not persisted across sessions, which belongs to the
  sleep/persistence work.

## Alternatives considered
- Dial the broker first and reconnect to add the will — a reconnect would drop the broker session and publish a
  will on the first socket, which is exactly what we must avoid.
- Accept WILLTOPIC without WILLMSGREQ — the spec's handshake and the old clients expect both prompts.
- Put the will handshake in the router — the router must not own per-session state.
- A configurable handshake timeout — no use case; 10 seconds is well above a LAN round trip.

## Consequences
A will connection takes two extra MQTT-SN round trips before CONNACK, and the slot is held during the
handshake. Missing WILLTOPIC/WILLMSG now fail cleanly instead of hanging forever. The broker client API grows
will options, but stays protocol-only and gateway-free. QoS 2 wills are refused rather than silently downgraded.
The follow-up for WILLTOPICUPD/WILLMSGUPD can reuse the stored will state but must solve the MQTT 3.1.1
limitation separately.
