# Changelog

## 1.1.4 - 2026-05-15

### Removed

- `X-Ip-Id` references from README — header and `ipId` field were already removed from the code in 1.1.3

## 1.1.3 - 2026-05-15

### Added

- `hook_queue_outbound` — captures the job ID and `Message-ID` headers at the `queue_outbound` event (outbound queue), replacing `hook_queue` which fired at the inbound `queue` event
- `messageId` — extracted from the `Message-ID` header in `hook_queue_outbound` (angle brackets stripped) and included on all outcome events
- `hook_shutdown` — gracefully closes the AMQP connection on process exit
- Automatic reconnection with exponential back-off when the broker drops the connection; delay capped at `connection.max_reconnect_delay_ms`
- Publish queue: events are buffered in memory (up to `publishing.max_queue_size`) while AMQP is disconnected and flushed on reconnect; previously events were silently dropped when disconnected
- `_extract_hmail_context` helper — centralises extraction of `queueId`, `messageId`, `senderAddress`, `retryCount`, `domain`, and `rcpts` from hmail
- `_build_outcome_event` helper — single authoritative place for building outcome event payloads
- `queueId`, `messageId`, `senderAddress`, `retryCount`, `mxHost`, `outboundIp`, `port`, `protocol` fields on all outcome events
- `mxHost`, `outboundIp`, `port`, `protocol` are always present on bounce and deferred events (as `null`) for a consistent event schema
- `hook_delivered` now emits one event per successfully-delivered recipient (`params[6]`); previously only the first recipient in the delivery batch was reported
- `hook_bounce` and `hook_deferred` now iterate all recipients from `hmail.todo.rcpt_to`; previously only the first was reported
- Deduplication guard (`_amqp_delivered_seen`) on `hook_delivered` — prevents duplicate `outcome.delivered` events when Haraka replays the hook for the same hmail
- Connection URL password is redacted in log output
- Debug log on every successful publish, and on each deliver, bounce, and deferred event
- New config sections and keys (previously only `[main]` existed with `amqp_url` and `exchange`):
  - `[connection]` — `amqp_url`, `max_reconnect_delay_ms`
  - `[publishing]` — `exchange`, `max_queue_size`, `publish_timeout_ms`
  - `[headers]` — `job_id_header` (replaces hard-coded `X-Job-Id`)

### Changed

- `hook_queue` renamed to `hook_queue_outbound`; fires at Haraka's `queue_outbound` event (outbound queue) instead of `queue` (inbound acceptance)
- `register()` no longer calls `register_hook()` explicitly — hooks are auto-discovered by Haraka via the `hook_*` naming convention
- `hook_delivered` signature changed from `(next, hmail, connection, params)` to `(next, hmail, params)` — the `connection` argument at position 3 was not part of the actual Haraka hook signature and was unused
- `hook_deferred` now reads `params.err` (error string) and `params.delay` (seconds) from the params object; previously it read positional string arguments `params[0]`, `params[1]`, `params[2]`
- `next()` is called before publishing in all mail hooks so Haraka processing is never blocked by AMQP I/O
- `_connect_and_setup` extracted from `hook_init_child` and refactored to accept `onConnectError`, `onReady`, `onSetupError` callbacks instead of forwarding `next` directly
- `_setup_channel` signature changed to accept callbacks rather than `next`; triggers `_flushQueue` on successful setup
- `settled` renamed to `ackHandled` in `_publish` for clarity
- `_build_outcome_event` signature changed from `(notes, status, code, message, rcpt, domain)` to `(context, params)` — context carries identity fields, params carries outcome and connection details
- `attemptedAt` is now a Unix millisecond timestamp (`Date.now()`) instead of an ISO 8601 string
- `domain` on outcome events is taken exclusively from `hmail.todo.domain`; the previous fallback to the MX host (`params[0]`) has been removed
- Publish timeout now read from `publishing.publish_timeout_ms`; previously controlled only by the internal `PUBLISH_TIMEOUT` test hook property
- `booleans` config option corrected from `'+enabled'` to `'+main.enabled'` so the plugin-enabled flag is properly parsed from the `[main]` INI section

### Removed

- `ipId` field from outcome events — `queueId` (Haraka's outbound queue UUID) serves the same purpose
- IP-ID tracking: `X-Ip-Id` was previously stripped from the message and stored as `ipId`; this mechanism is no longer supported
- `amqp_url` and `exchange` from the `[main]` config section — moved to `[connection]` and `[publishing]` respectively

## 1.1.2 - 2026-05-09

### Fixed

- `index.js` was missing from the `files` array in `package.json` — the plugin code was not included in the published package

## 1.1.1 - 2026-05-09

- Initial release
