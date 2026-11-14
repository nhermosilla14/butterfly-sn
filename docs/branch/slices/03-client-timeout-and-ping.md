# Expire silent clients and answer their keepalive

- Intents: 20261114-session-liveness
- ADRs: 20261114-session-liveness-and-broker-loss
- Depends on: 01, 02

## Scope
A client that goes silent beyond 1.5 times the keepalive from its CONNECT is declared lost: the gateway sends
it a best-effort DISCONNECT, sends MQTT DISCONNECT to the broker, closes the connection and frees the slot.
PINGREQ from an active client is answered with PINGRESP and counts as activity. A keepalive of 0 disables the
inactivity timeout.

## Layers touched
- Services: `internal/mqttsn` PINGREQ and PINGRESP codec; the session inactivity timer, reset by every valid
  client packet, and its timeout path; PINGREQ answered locally without forwarding.
- Interface: none; the UDP wire gains the two standard packet types.

## End-to-end test
- PINGREQ from a connected client gets PINGRESP, and the session stays usable afterwards.
- With a 1 s keepalive and `max_clients` 1, a silent client gets a best-effort DISCONNECT after 1.5 s, its
  session is gone (the next REGISTER gets DISCONNECT), and a second client's CONNECT is accepted.
- A client with keepalive 0 is still connected after an idle period longer than any timer, and PINGREQ gets
  PINGRESP.
- Covers criteria: timeout and best-effort DISCONNECT; limit counts live sessions; PINGREQ/PINGRESP; keepalive 0.

## Out of scope
- Gateway-originated PINGREQ towards clients.
- Sleep-duration supervision and the asleep/awake states; DISCONNECT with a duration stays a full disconnect.
- Will publication when a client is declared lost, retransmission, reconnection.
