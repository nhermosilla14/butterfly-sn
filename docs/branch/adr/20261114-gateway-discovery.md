# Discovery replies and periodic advertisement

- Status: Accepted
- Date: 2026-11-14
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla
- Spec area: discovery.md, configuration.md, transport-and-sessions.md

## Context
MQTT-SN clients discover gateways with SEARCHGW and listen for ADVERTISE. The legacy gateway answered SEARCHGW
with a multicast GWINFO and no delay, and reused the keepalive setting as the ADVERTISE interval, which also
overflowed the 16-bit duration field. The new gateway listens on one unicast UDP socket and must not treat
discovery packets as client traffic.

## Decision
- The MQTT-SN codec gains SEARCHGW (optional radius byte), GWINFO (gateway id plus optional address) and
  ADVERTISE (gateway id plus 16-bit duration).
- The router handles discovery before session lookup: SEARCHGW is answered and returns; GWINFO and ADVERTISE are
  logged and ignored, so they never create a session or get a DISCONNECT.
- SEARCHGW gets exactly one GWINFO, unicast to the sender, after a random delay uniformly in 0–500 ms. The radius
  is ignored. The delayed send uses a timer callback; a SEARCHGW flood is not rate-limited in this intent.
- Configuration gains:
  - `gateway_id`: integer 1–255, default 1.
  - `advertise_interval`: integer seconds 0–65535, default 900; 0 disables advertisement.
  - `advertise_address`: UDP `host:port`; default `255.255.255.255:<listen port>`. It is validated at startup.
- The gateway runs one advertisement ticker at gateway level, started in `Run` and stopped with its context. It
  sends after one full interval, not at startup, with duration equal to `advertise_interval` and the configured
  gateway id.
- Advertisement uses the same UDP socket, so the source port is the listen port. Broadcast destinations require
  the socket to allow broadcast.
- No cross-gateway suppression: a pending GWINFO is sent even if another gateway replies first. The legacy
  gateway also lacked it, and implementing it needs listening for other gateways' replies, which is future work.

## Alternatives considered
- Multicast GWINFO as the legacy did — the requirement is a unicast reply, and multicast does not cross default
  container networks.
- No delay — several gateways would answer a broadcast search at once; a short random delay reduces collisions.
- Fixed advertisement destination — broadcast on the listen port is the sensible default; a configurable address
  covers routed networks and deterministic tests.
- Reusing the client keepalive as the advertisement interval — the legacy bug being fixed.
- A separate discovery socket — more state without a benefit; the existing socket already receives unicast and
  local broadcast.

## Consequences
Discovery no longer creates sessions by accident and works for clients that only listen for advertisements.
Broadcast advertisement is limited to the local segment; operators on routed networks set `advertise_address`.
The 0–500 ms delay adds latency to discovery but is bounded. A flood of SEARCHGW can create many short-lived
timers, which the hardening work can rate-limit later. Container deployments that need broadcast or multicast
discovery still require host networking, as the delivery ADR notes.
