'use strict'

const amqplib = require('amqplib/callback_api')

const plugin = exports

// ─── Setup / Configuration ───────────────────────────────────────────────────

plugin.register = function () {
  this._publishQueue = []

  this.load_amqp_reporting_ini()

  if (!this.cfg.main.enabled) {
    this.loginfo('disabled via config')
    return
  }
}

plugin.load_amqp_reporting_ini = function () {
  this.cfg = this.config.get('amqp_reporting.ini', { booleans: ['+main.enabled'] }, () => {
    this.load_amqp_reporting_ini()
  })
  this._sanitizedUrl = this._sanitize_url(this.cfg.connection.amqp_url)
}

// ─── Lifecycle Hooks ─────────────────────────────────────────────────────────

plugin.hook_init_child = function (next) {
  this._reconnectDelay = 1000
  this._connect_and_setup(
    (err) => {
      this.logerror(`AMQP connect failed [${this._sanitizedUrl}]: ${err.message}`)
      this._reconnect()
      next()
    },
    (exchange) => {
      this.loginfo(`AMQP ready - exchange: ${exchange}`)
      next()
    },
    () => next(),
  )
}

plugin.hook_shutdown = function (next) {
  this._shuttingDown = true
  if (this._reconnectTimer) {
    clearTimeout(this._reconnectTimer)
    this._reconnectTimer = null
  }
  if (!this.amqp) return next()
  const { conn } = this.amqp
  this.amqp = null
  conn.close((err) => {
    if (err) this.logerror(`AMQP close error: ${err.message}`)
    next()
  })
}

// ─── AMQP Connection Management ──────────────────────────────────────────────

plugin._connect_and_setup = function (onConnectError, onReady, onSetupError) {
  const url = this.cfg.connection.amqp_url
  this._connect(url, (err, conn) => {
    if (err) return onConnectError(err)
    this._attach_connection_error_handler(conn)
    conn.createConfirmChannel((chErr, ch) =>
      this._setup_channel(conn, chErr, ch, onReady, onSetupError),
    )
  })
}

plugin._connect = function (url, cb) {
  amqplib.connect(url, cb)
}

plugin._setup_channel = function (conn, err, ch, onSuccess, onError) {
  if (err) {
    this.logerror(`AMQP channel failed: ${err.message}`)
    conn.close(() => {})
    return onError()
  }
  const exchange = this.cfg.publishing.exchange
  ch.assertExchange(exchange, 'topic', { durable: true }, (assertErr) => {
    if (assertErr) {
      this.logerror(`AMQP assertExchange failed: ${assertErr.message}`)
      conn.close(() => {})
      return onError()
    }
    this.amqp = { conn, ch, exchange }
    this._flushQueue()
    onSuccess(exchange)
  })
}

plugin._attach_connection_error_handler = function (conn) {
  conn.on('error', (e) => {
    this.logerror(`AMQP connection error: ${e.message}`)
    if (!this.amqp) return
    this.amqp = null
    this._reconnect()
  })
}

plugin._reconnect = function () {
  if (this._shuttingDown) return
  const maxDelay = this.cfg?.connection?.max_reconnect_delay_ms ?? 30000
  const delay = Math.min(this._reconnectDelay, maxDelay)
  this._reconnectDelay = Math.min(this._reconnectDelay * 2, maxDelay)
  this.logdebug(`AMQP reconnecting in ${delay}ms`)
  this._reconnectTimer = setTimeout(() => {
    this._reconnectTimer = null
    this._connect_and_setup(
      (err) => {
        this.logerror(`AMQP reconnect failed [${this._sanitizedUrl}]: ${err.message}`)
        this._reconnect()
      },
      (exchange) => {
        this._reconnectDelay = 1000
        this.loginfo(`AMQP reconnected - exchange: ${exchange}`)
      },
      () => this._reconnect(),
    )
  }, delay)
}

plugin._sanitize_url = function (url) {
  try {
    const u = new URL(url)
    if (u.password) u.password = '***'
    return u.toString()
  } catch {
    return url
  }
}

// ─── Mail Hooks ───────────────────────────────────────────────────────────────

plugin.hook_queue_outbound = function (next, connection) {
  const txn = connection.transaction
  if (!txn) return next()

  const jobIdHeader = this.cfg?.headers?.job_id_header || null
  const rawJob = jobIdHeader ? txn.header.get(jobIdHeader) : null
  txn.notes.amqp_job_id = rawJob ? rawJob.trim() : ''

  const rawMsgId = txn.header.get('Message-ID') || ''
  txn.notes.amqp_message_id = rawMsgId.replace(/^<|>$/g, '').trim()

  if (jobIdHeader) txn.remove_header(jobIdHeader)

  next()
}

plugin.hook_delivered = function (next, hmail, params) {
  if (hmail && hmail._amqp_delivered_seen) return next()
  if (hmail) hmail._amqp_delivered_seen = true

  const context = this._extract_hmail_context(hmail)
  const { notes, domain } = context
  const response = (params && params[2]) || ''
  const okRcpts = (params && params[6]) || []
  const code = this._parse_smtp_code(response, 250)

  const connectionDetails = {
    mxHost: (params && params[0]) || '',
    outboundIp: (params && params[1]) || '',
    port: (params && params[4]) || null,
    protocol: (params && params[5]) || '',
  }

  next()

  for (const rcpt of okRcpts) {
    const address = rcpt.original.slice(1, -1)
    this.logdebug(
      `delivered - jobId: ${notes.amqp_job_id || '(none)'}, rcpt: ${address}, domain: ${domain}, smtpCode: ${code}`,
    )
    const event = this._build_outcome_event(context, {
      status: 'delivered',
      code,
      message: response,
      rcpt: address,
      ...connectionDetails,
    })
    this._publish('outcome.delivered', event)
  }
}

plugin.hook_bounce = function (next, hmail, error) {
  const context = this._extract_hmail_context(hmail)
  const { notes, domain, rcpts } = context
  const code = (error && error.code) || 0
  const msg = (error && error.message) || ''

  next()

  for (const rcptObj of rcpts) {
    const address = rcptObj.original.slice(1, -1)
    this.logdebug(
      `bounce - jobId: ${notes.amqp_job_id || '(none)'}, rcpt: ${address}, domain: ${domain}, smtpCode: ${code}, msg: ${msg}`,
    )
    const event = this._build_outcome_event(context, {
      status: 'bounced',
      code,
      message: msg,
      rcpt: address,
    })
    this._publish('outcome.bounced', event)
  }
}

plugin.hook_deferred = function (next, hmail, params) {
  const context = this._extract_hmail_context(hmail)
  const { notes, domain, rcpts } = context
  const msg = (params && params.err) || ''
  const delay = (params && params.delay) || 0
  const code = this._parse_smtp_code(msg, 421)

  next()

  for (const rcptObj of rcpts) {
    const address = rcptObj.original.slice(1, -1)
    this.logdebug(
      `deferred - jobId: ${notes.amqp_job_id || '(none)'}, rcpt: ${address}, domain: ${domain}, smtpCode: ${code}, delay: ${delay}s, msg: ${msg}`,
    )
    const event = this._build_outcome_event(context, {
      status: 'deferred',
      code,
      message: msg,
      rcpt: address,
    })
    this._publish('outcome.deferred', event)
  }
}

// ─── Event Helpers ────────────────────────────────────────────────────────────

plugin._extract_hmail_context = function (hmail) {
  const todo = (hmail && hmail.todo) || {}
  const notes = todo.notes || {}
  const rcpts = todo.rcpt_to || []
  return {
    notes,
    queueId: todo.uuid || '',
    messageId: notes.amqp_message_id || '',
    senderAddress: todo.mail_from ? todo.mail_from.original.slice(1, -1) : '',
    domain: todo.domain || '',
    rcpts,
    rcpt: rcpts.length ? rcpts[0].original.slice(1, -1) : '',
    retryCount: (hmail && hmail.num_failures) || 0,
  }
}

plugin._build_outcome_event = function (context, params) {
  const {
    status,
    code,
    message,
    rcpt,
    mxHost = null,
    outboundIp = null,
    port = null,
    protocol = null,
  } = params
  const { notes, queueId, messageId, senderAddress, domain, retryCount } = context
  return {
    jobId: notes.amqp_job_id || '',
    queueId,
    messageId,
    senderAddress,
    status,
    smtpCode: code,
    smtpMessage: message,
    recipientAddress: rcpt,
    domain,
    mxHost,
    outboundIp,
    port,
    protocol,
    retryCount,
    attemptedAt: Date.now(),
  }
}

plugin._parse_smtp_code = function (msg, fallback) {
  const m = /^(\d{3})/.exec(msg || '')
  return m ? parseInt(m[1], 10) : fallback
}

// ─── Publishing ───────────────────────────────────────────────────────────────

plugin._publish = function (routingKey, event) {
  if (!this.amqp) {
    const maxQueueSize = this.cfg?.publishing?.max_queue_size ?? 100
    if (this._publishQueue.length < maxQueueSize) {
      this._publishQueue.push({ routingKey, event })
      this.logdebug(
        `AMQP not connected - queued event (${this._publishQueue.length}/${maxQueueSize}): routingKey: ${routingKey}, jobId: ${event.jobId}`,
      )
    } else {
      this.logwarn(
        `AMQP not connected - queue full, dropping event: routingKey: ${routingKey}, jobId: ${event.jobId}`,
      )
    }
    return
  }

  const { ch, exchange } = this.amqp
  const body = Buffer.from(JSON.stringify(event))
  const options = { persistent: true, mandatory: true }
  const timeout = this.cfg?.publishing?.publish_timeout_ms ?? this.PUBLISH_TIMEOUT ?? 5000

  let ackHandled = false

  const timer = setTimeout(() => {
    ackHandled = true
    this.logerror(`AMQP publish timeout - routingKey: ${routingKey}, jobId: ${event.jobId}`)
  }, timeout)

  ch.publish(exchange, routingKey, body, options, (err) => {
    if (ackHandled) return
    ackHandled = true
    clearTimeout(timer)
    if (err) {
      this.logerror(`AMQP publish nack - routingKey: ${routingKey}: ${err.message}`)
    } else {
      this.logdebug(`published - routingKey: ${routingKey}, jobId: ${event.jobId}`)
    }
  })
}

plugin._flushQueue = function () {
  if (!this._publishQueue.length) return
  this.logdebug(`AMQP flushing ${this._publishQueue.length} queued event(s)`)
  for (const { routingKey, event } of this._publishQueue.splice(0)) {
    this._publish(routingKey, event)
  }
}
