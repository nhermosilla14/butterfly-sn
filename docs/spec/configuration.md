# Configuration

The gateway is configured by a single YAML file passed with `-config`.

## Behaviours

### Minimal YAML configuration
The file accepts `listen`, `broker`, `max_clients` and `log_level`. Unknown keys are rejected. `max_clients`
defaults to 100 and must be greater than 0; `log_level` defaults to `info` and accepts `debug`, `info`, `warn`
and `error`. `listen` and `broker` are required.
- Criteria:
  - Absent `max_clients` and `log_level` take the defaults 100 and `info`.
  - Missing `listen` or `broker`, a non-positive `max_clients`, an unknown key or an unknown log level makes
    startup fail with a message naming the problem.
  - `listen` with port 0 is accepted, so the gateway can bind a free port.
- Tests: TestParseDefaults, TestParseErrors, TestParseEmptyDocumentFailsOnRequiredKeys, TestParseListenPortZeroAllowed, TestStartupInvalidConfig

## Not supported
- Hot reload: configuration is read once at startup.
- TLS, authentication and credentials for the broker connection; not planned for the transparent gateway.
- Container image and release builds; separate intent.
