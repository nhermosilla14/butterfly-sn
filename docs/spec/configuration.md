# Configuration

The gateway is configured by a single YAML file passed with `-config`.

## Behaviours

### Minimal YAML configuration
The file accepts `listen`, `broker`, `max_clients`, `log_level`, `gateway_id`, `advertise_interval` and
`advertise_address`. Unknown keys are rejected. `max_clients` defaults to 100 and must be greater than 0;
`log_level` defaults to `info` and accepts `debug`, `info`, `warn` and `error`. `listen` and `broker` are
required. `gateway_id` defaults to 1 and must be 1 to 255; `advertise_interval` defaults to 900 seconds, accepts
0 to 65535 and 0 disables advertisement; `advertise_address` is an optional UDP `host:port` and defaults to the
broadcast address on the listen port (see [discovery.md](discovery.md)).
- Criteria:
  - Absent optional keys take their defaults: 100 clients, `info`, gateway id 1, interval 900.
  - Missing `listen` or `broker`, a non-positive `max_clients`, a `gateway_id` outside 1 to 255, an
    `advertise_interval` outside 0 to 65535, an invalid `advertise_address`, an unknown key or an unknown log
    level makes startup fail with a message naming the problem.
  - `listen` with port 0 is accepted, so the gateway can bind a free port.
- Tests: TestParseDefaults, TestParseErrors, TestParseEmptyDocumentFailsOnRequiredKeys, TestParseListenPortZeroAllowed, TestParseGatewayIDBounds, TestParseAdvertise, TestStartupInvalidConfig, TestStartupInvalidAdvertise

## Not supported
- Hot reload: configuration is read once at startup.
- TLS, authentication and credentials for the broker connection; not planned for the transparent gateway.
- Container image and release builds; separate intent.
- Multicast discovery and per-key discovery settings beyond `gateway_id`, `advertise_interval` and
  `advertise_address`; see [discovery.md](discovery.md).
