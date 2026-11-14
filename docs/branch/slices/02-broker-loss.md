# Tell the client when the broker connection is lost

- Status: In progress
- Intents: 20261114-session-liveness
- ADRs: 20261114-session-liveness-and-broker-loss, 20261114-broker-event-reader-api
- Depends on: 01

## Scope
When the broker connection ends for any reason, the client is told and the session disappears. This covers TCP
close or read error, write error, an MQTT DISCONNECT from the broker, and a PINGREQ that the broker never
answers. The client receives MQTT-SN DISCONNECT, the broker socket is closed, the session is removed and its
slot is available to a new client. A packet from that client afterwards follows the existing unknown-sender
rule and gets DISCONNECT.

## Layers touched
- Services: session teardown on broker loss, a single path shared with explicit client DISCONNECT; pending QoS 1
  acknowledgements are abandoned; the unanswered-PINGREQ case uses the broker keepalive timing from slice 01.
- Interface: e2e harness gains a small TCP proxy in front of the shared broker so a test can cut the broker
  connection, and can inject an MQTT DISCONNECT, without stopping the shared container.

## End-to-end test
- Cut the proxied broker connection while the client is connected: the client receives DISCONNECT, and a later
  REGISTER from it receives DISCONNECT instead of REGACK.
- With `max_clients` 1, a second CONNECT gets CONNACK Congestion while the first is connected, and CONNACK
  Accepted after the first session loses its broker connection.
- A broker-sent MQTT DISCONNECT produces the same client-visible DISCONNECT.
- Covers criteria: client notified on broker loss; session removed and slot freed; logging.

## Out of scope
- Client inactivity timeout and MQTT-SN PINGREQ/PINGRESP.
- Will publication on broker loss, reconnection or session resumption.
- Retransmission of packets that were in flight when the broker was lost.
