# Vision

`butterfly-sn` is a transparent MQTT-SN v1.2 gateway written in Go. It replaces a patched build of the
Eclipse Paho MQTT-SN gateway (C++), which is documented in `olddocs/`.

## Why
- One static binary and a small container image; no Cygwin on Windows, native Linux and Win64 builds.
- A codebase that is easier to maintain than the C++ one.
- Robustness: no crashes on malformed input, clean shutdown, bounded resources, useful logs.

## Users
Operators running devices that speak MQTT-SN over UDP (up to ~100 clients, LAN or VPN) against a plain
MQTT broker, typically on a local host or inside a container.

## Success criteria
- The existing devices work against the new gateway without changes.
- Runs unattended: no stuck clients, no slots leaking, restarts and broker outages are recovered from.
- Passes conformance checks for the supported MQTT-SN features against the spec.

## Behaviours to deliver (cut into intents later)
Reference: files in `olddocs/`. "Keep" means same behaviour as the old gateway, "Fix" means the old one
had a defect or quirk, "New" means not in the old gateway.

| Area | Behaviour | Decision |
|---|---|---|
| Transport | UDP IPv4 listener, one session per sender address | Keep |
| Session | CONNECT, CONNACK, DISCONNECT, one broker TCP connection per client | Keep |
| Session | Exact client-id matching, idle slots reclaimed, slot limit applies to live sessions | Fix |
| Session | CONNACK congestion when the broker connection fails; DISCONNECT sent to the client when the broker drops | Fix |
| Session | Keepalive enforcement (1.5x) and sleep-duration expiry | New |
| Patch | DISCONNECT to packets from unknown or non-connected clients | Keep (patch) |
| Patch | WILLTOPIC/WILLMSG accepted from a client in the disconnected state | Keep (patch) |
| Will | WILLTOPICREQ/WILLMSGREQ flow, will published on abnormal loss | Keep |
| Will | WILLTOPICUPD/WILLMSGUPD | New |
| Will | Will published when the gateway shuts down | New |
| Sleep | Asleep/Awake states, buffering, flush on PINGREQ | Keep |
| Sleep | PINGRESP always sent after wake-up flush | Fix |
| Topics | REGISTER/REGACK both directions, registered topic ids, short names | Keep |
| Topics | Wildcard SUBSCRIBE, UNSUBSCRIBE | Keep |
| Topics | Full topic table answered with congestion instead of crashing; one- and zero-character broker topics handled | Fix |
| Pub/sub | PUBLISH and PUBACK, QoS 0 and 1, both directions | Keep |
| Pub/sub | QoS 0 PUBLISH to an unknown topic id is dropped | Fix |
| Pub/sub | Retransmission of REGISTER and PUBLISH towards the client when unanswered | New |
| Discovery | SEARCHGW answered with GWINFO (unicast, random delay) | Change |
| Discovery | Periodic ADVERTISE with its own interval (default 900 s) | Change |
| Config | YAML configuration file | Change |
| Operations | Structured logging to stdout, non-zero exit on fatal errors, SIGINT and SIGTERM shutdown | Fix |
| Delivery | Static binaries for Linux and Win64, container image built with podman | New |

## Non-goals
- Aggregating gateway mode, QoS -1 proxy, forwarder encapsulation.
- DTLS, UDP6, XBee, LoRaLink, RFCOMM transports.
- Predefined topic ids and `predefinedTopic.conf`.
- Client allowlist (`ClientAuthentication`, `clients.conf`): broken in the old gateway.
- Log monitor and shared-memory logging.
- TLS and authentication towards the broker (v1: plain TCP, no credentials).
- QoS 2 (planned for a later release).

## Constraints
- Compatible with MQTT-SN v1.2 as implemented by the clients in use; the spec is `olddocs/MQTT-SN_spec_v1.2.pdf`.
- Implemented from the spec and from behaviour documented in `olddocs/`; no code is copied from Paho (EPL/EDL).
- Licensed MIT.
- Coexistence: the new gateway replaces the old one on a host; both are not expected to run on the same ports.
