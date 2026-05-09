# haraka-plugin-amqp_reporting

[![npm version](https://img.shields.io/npm/v/haraka-plugin-amqp_reporting.svg)](https://www.npmjs.com/package/haraka-plugin-amqp_reporting)
[![CI](https://github.com/brassnode/haraka-plugin-amqp_reporting/actions/workflows/ci.yml/badge.svg)](https://github.com/brassnode/haraka-plugin-amqp_reporting/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Haraka plugin that publishes outbound delivery outcome events to a RabbitMQ topic exchange.

Reports `delivered`, `bounced`, and `deferred` outcomes after each delivery attempt so downstream services can update job status without polling Haraka directly.

## Features

- Three delivery outcome events (`delivered`, `bounced`, `deferred`) published to a RabbitMQ topic exchange
- Per-message routing keys (`outcome.delivered`, `outcome.bounced`, `outcome.deferred`) for selective consumer binding
- Confirm-channel publishing — the broker acknowledges each message before the plugin considers it sent
- 5-second per-publish timeout prevents a stalled broker from blocking Haraka's delivery pipeline
- One AMQP connection per worker process; connections are isolated and re-established automatically on worker restart
- Stashes `X-Job-Id` and `X-Ip-Id` headers at queue time and strips them from the outbound message
- Compatible with any AMQP 0-9-1 broker via `amqp://` or `amqps://` URLs (RabbitMQ, AWS MQ, CloudAMQP, etc.)

## Requirements

- Node.js >= 22
- Haraka SMTP server
- RabbitMQ broker reachable from each Haraka worker

## Install

Navigate to your Haraka installation directory:

```sh
cd /path/to/local/haraka
```

Install the plugin:

```sh
npm install haraka-plugin-amqp_reporting
```

Register the plugin:

```sh
echo "amqp_reporting" >> config/plugins
```

Restart Haraka:

```sh
service haraka restart
```

## Configuration

Copy the default config into your Haraka config directory:

```sh
cp node_modules/haraka-plugin-amqp_reporting/config/amqp_reporting.ini config/amqp_reporting.ini
```

### amqp_reporting.ini

```ini
[main]
; Set to false to disable the plugin entirely
enabled = true

; AMQP broker URL — supports amqp:// and amqps://
amqp_url = amqp://guest:guest@localhost:5672

; Topic exchange to publish delivery outcome events to
exchange = mailing.delivery.outcomes
```

The plugin declares the exchange as `topic, durable` on startup. The exchange must not already exist with different options.

## How it works

The plugin uses two inbound hooks and three outbound delivery hooks:

| Hook         | Action                                                             |
| ------------ | ------------------------------------------------------------------ |
| `init_child` | Opens one AMQP connection + confirm channel per worker process     |
| `queue`      | Stashes `X-Job-Id`/`X-Ip-Id` into `hmail.todo.notes`; strips both  |
| `delivered`  | Publishes `outcome.delivered` after a 2xx acceptance               |
| `bounce`     | Publishes `outcome.bounced` after a permanent 5xx or exhausted 4xx |
| `deferred`   | Publishes `outcome.deferred` on each temporary 4xx deferral        |

## Event schema

Every message published to RabbitMQ has the following JSON body:

| Field              | Type   | Description                                           |
| ------------------ | ------ | ----------------------------------------------------- |
| `jobId`            | string | Value of `X-Job-Id` header stashed at queue time      |
| `ipId`             | string | Value of `X-Ip-Id` header stashed at queue time       |
| `status`           | string | `"delivered"`, `"bounced"`, or `"deferred"`           |
| `smtpCode`         | number | Numeric SMTP response code (e.g. `250`, `550`, `421`) |
| `smtpMessage`      | string | Full SMTP response text                               |
| `recipientAddress` | string | Recipient email address from `RCPT TO`                |
| `domain`           | string | Destination domain                                    |
| `attemptedAt`      | string | ISO 8601 timestamp of the delivery attempt            |

## Routing keys

| Routing key         | Fires when                                             |
| ------------------- | ------------------------------------------------------ |
| `outcome.delivered` | Destination server accepted the message (2xx)          |
| `outcome.bounced`   | Permanent rejection (5xx) or exhausted 4xx retry queue |
| `outcome.deferred`  | Temporary deferral (4xx), Haraka will retry            |

Bind all three routing keys to the same queue if a single consumer handles all outcomes:

```sh
rabbitmqadmin declare queue name=mailing.delivery.outcomes.q durable=true
rabbitmqadmin declare binding source=mailing.delivery.outcomes \
  destination=mailing.delivery.outcomes.q routing_key="outcome.*"
```

## Reliability notes

- One AMQP connection per worker process; no connection is shared across workers.
- Publishes use confirm channels — the broker acknowledges each message before the plugin considers it sent.
- A 5-second per-publish timeout prevents a stalled broker from blocking Haraka's delivery pipeline. If a publish times out or is nack'd, the error is logged and delivery continues unaffected.
- If the AMQP connection drops, the plugin logs the error and stops publishing. Worker restart re-establishes the connection via `hook_init_child`.

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Contributing

Pull requests are welcome. Please open an issue first to discuss significant changes.

Run tests:

```sh
npm test
```

Check code style:

```sh
npm run lint
```

Auto-fix style:

```sh
npm run format
```

## License

MIT. See [LICENSE](LICENSE).
