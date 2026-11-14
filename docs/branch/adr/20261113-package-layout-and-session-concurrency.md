# Package layout and per-session concurrency

- Status: Proposed
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Spec area: transport-and-sessions.md, topics-and-pubsub.md

## Context
The gateway must handle about 100 clients over one UDP socket, each with its own TCP connection to the
broker (transparent mode). The old gateway used five threads and global queues, which caused stalls (one
blocking broker connect froze uplink traffic for everyone) and races (see `olddocs/technical/known-issues.md`).

## Decision
Module `github.com/nhermosilla14/butterfly-sn`. Layout:
- `cmd/butterfly-sn`: main, flags (`-config`), signal handling.
- `internal/config`: YAML loading and validation.
- `internal/mqttsn`: MQTT-SN packet codec (encode/decode, no I/O). Pure functions, fuzzable.
- `internal/mqtt`: minimal MQTT 3.1.1 client codec and connection (see ADR on the broker client).
- `internal/gateway`: UDP server and sessions.

Concurrency: one goroutine reads the UDP socket and routes each datagram, by sender address, to the inbox
channel of that client's session. Each session runs one goroutine that owns all its state (state, topic table,
message ids) and processes its inbox, so session state needs no locks. Each session also owns the broker TCP
connection with a reader goroutine that feeds events into the same inbox. Blocking work (dialing the broker)
happens with a timeout inside the session goroutine, so one slow broker connect only delays that client.
Outbound UDP writes go through the shared socket. A `context.Context` cancels everything on shutdown. A
packet from an unknown sender is handled by the router (CONNECT creates the session, anything else gets
DISCONNECT).

## Alternatives considered
- Global event queues and worker pool, as in the old gateway — shared state needs locks and one blocked worker stalls all clients.
- A single goroutine for all sessions — simple, but a slow broker connect blocks every client.
- Locked shared session map with callbacks — more races, harder to test.

## Consequences
Per-client isolation and easy reasoning about session state. Roughly three goroutines per client, which is fine for
about 100 clients. The router needs a concurrency-safe address-to-session map, and sessions must be removed
from it when they end.
