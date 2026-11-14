# Will updates are rejected explicitly

- Status: Accepted
- Date: 2026-11-14
- Owner: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Depends on: none

## Goal
A client that sends WILLTOPICUPD or WILLMSGUPD gets an answer instead of silence. MQTT 3.1.1 cannot change a will
on a live broker connection, and the project chooses not to emulate it (no reconnect, no gateway-published
wills), so the gateway acknowledges both messages with return code Not supported and leaves the will stored in
the broker untouched. This is the behaviour of the legacy gateway, kept on purpose, and the vision stops listing
will updates as something to deliver.

## Acceptance criteria
- WILLTOPICUPD from a connected, active client gets WILLTOPICRESP with return code Not supported (0x03). The same
  holds for an empty WILLTOPICUPD (the "delete the will" form, exactly two octets).
- WILLMSGUPD gets WILLMSGRESP with return code Not supported.
- The session stays usable: publishing, subscribing and keepalive continue, the broker connection is not touched
  (no reconnect, no MQTT DISCONNECT), and the will given at CONNECT is still the one the broker publishes when
  the client is lost.
- Both messages are answered while the client is awake in a wake-up flush; while the client is asleep they are
  ignored like any other packet that does not wake it (existing sleeping rules).
- A WILLTOPICUPD or WILLMSGUPD from an address with no session gets DISCONNECT, like any other packet from an
  unknown sender.
- Malformed update messages are ignored without ending the session, like other malformed packets.
- The codec decodes and encodes the four messages (WILLTOPICUPD, WILLMSGUPD, WILLTOPICRESP, WILLMSGRESP).
- Rejections are logged with client id and message type, never the will contents.

## Spec changes
- Adds: will / Will update requests
- Changes: will / Not supported (WILLTOPICUPD and WILLMSGUPD become a deliberate non-goal, with the reason)
- Changes: transport-and-sessions / Not supported (remove the will-update line)
- Changes: docs/vision.md (will updates move from "behaviours to deliver" to non-goals)

## Out of scope
- Applying an update: reconnecting the broker connection with a new will, or the gateway publishing the will
  itself. Revisit only with a new intent and ADR.
- Will persistence across sessions.

## Decisions taken
- MQTT-SN 1.2 section 6.4 says the stored will is overwritten and both messages are acknowledged. The gateway
  cannot honour the overwrite, so it acknowledges with the rejection reason the specification allows
  ("accepted, or rejection reason") instead of pretending.
- Return code Not supported (0x03), as in the legacy gateway.
