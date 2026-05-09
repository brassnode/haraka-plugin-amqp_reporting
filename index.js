'use strict'

const amqplib = require('amqplib/callback_api')

const plugin = exports

plugin.register = function () {
  this.load_amqp_reporting_ini()

  if (!this.cfg.main.enabled) {
    this.loginfo('disabled via config')
    return
  }

  this.register_hook('init_child', 'hook_init_child')
  this.register_hook('queue',      'hook_queue')
  this.register_hook('delivered',  'hook_delivered')
  this.register_hook('bounce',     'hook_bounce')
  this.register_hook('deferred',   'hook_deferred')
}

plugin.load_amqp_reporting_ini = function () {
  this.cfg = this.config.get(
    'amqp_reporting.ini',
    { booleans: ['+enabled'] },
    () => { this.load_amqp_reporting_ini() },
  )
}

plugin.hook_init_child = function (next) {
  const url = this.cfg.main.amqp_url
  this._connect(url, (err, conn) => {
    if (err) {
      this.logerror(`AMQP connect failed: ${err.message}`)
      return next()
    }
    conn.on('error', (e) => {
      this.logerror(`AMQP connection error: ${e.message}`)
      this.amqp = null
    })
    conn.createConfirmChannel((assertErr, ch) => this._setup_channel(next, conn, assertErr, ch))
  })
}

plugin._connect = function (url, cb) {
  amqplib.connect(url, cb)
}

plugin._setup_channel = function (next, conn, err, ch) {
  if (err) {
    this.logerror(`AMQP channel failed: ${err.message}`)
    return next()
  }
  const exchange = this.cfg.main.exchange
  ch.assertExchange(exchange, 'topic', { durable: true }, (assertErr) => {
    if (assertErr) {
      this.logerror(`AMQP assertExchange failed: ${assertErr.message}`)
      return next()
    }
    this.amqp = { conn, ch, exchange }
    this.loginfo(`AMQP ready - exchange: ${exchange}`)
    next()
  })
}

plugin.hook_queue = function (next, connection) {
  const txn = connection.transaction
  if (!txn) return next()

  const jobId = txn.header.get('X-Job-Id') || ''
  const ipId  = txn.header.get('X-Ip-Id')  || ''

  txn.notes.amqp_job_id = jobId.trim()
  txn.notes.amqp_ip_id  = ipId.trim()

  txn.remove_header('X-Job-Id')
  txn.remove_header('X-Ip-Id')

  next()
}

plugin.hook_delivered = function (next, hmail, connection, params) {
  const notes  = (hmail && hmail.todo && hmail.todo.notes) || {}
  const rcpt   = params && params[0] ? params[0].address() : ''
  const msg    = (params && params[1]) || ''
  const domain = (params && params[2]) || ''

  this._publish('outcome.delivered', {
    jobId:            notes.amqp_job_id || '',
    ipId:             notes.amqp_ip_id  || '',
    status:           'delivered',
    smtpCode:         this._parse_smtp_code(msg, 250),
    smtpMessage:      msg,
    recipientAddress: rcpt,
    domain,
    attemptedAt:      new Date().toISOString(),
  })

  next()
}

plugin.hook_bounce = function (next, hmail, error) {
  const notes = (hmail && hmail.todo && hmail.todo.notes) || {}
  const rcpts = (hmail && hmail.todo && hmail.todo.rcpt_to) || []
  const code  = (error && error.code) || 0
  const msg   = (error && error.message) || ''

  this._publish('outcome.bounced', {
    jobId:            notes.amqp_job_id || '',
    ipId:             notes.amqp_ip_id  || '',
    status:           'bounced',
    smtpCode:         code,
    smtpMessage:      msg,
    recipientAddress: rcpts.length ? rcpts[0].address() : '',
    domain:           (hmail && hmail.todo && hmail.todo.domain) || '',
    attemptedAt:      new Date().toISOString(),
  })

  next()
}

plugin.hook_deferred = function (next, hmail, params) {
  const notes  = (hmail && hmail.todo && hmail.todo.notes) || {}
  const domain = (params && params[0]) || ''
  const msg    = (params && params[1]) || ''
  const rcpt   = params && params[2] ? params[2].address() : ''

  this._publish('outcome.deferred', {
    jobId:            notes.amqp_job_id || '',
    ipId:             notes.amqp_ip_id  || '',
    status:           'deferred',
    smtpCode:         this._parse_smtp_code(msg, 421),
    smtpMessage:      msg,
    recipientAddress: rcpt,
    domain,
    attemptedAt:      new Date().toISOString(),
  })

  next()
}

plugin._publish = function (routingKey, event) {
  if (!this.amqp) return

  const { ch, exchange } = this.amqp
  const body    = Buffer.from(JSON.stringify(event))
  const options = { persistent: true, mandatory: true }
  const timeout = this.PUBLISH_TIMEOUT !== undefined ? this.PUBLISH_TIMEOUT : 5000

  let settled = false

  const timer = setTimeout(() => {
    settled = true
    this.logerror(`AMQP publish timeout - routingKey: ${routingKey}, jobId: ${event.jobId}`)
  }, timeout)

  ch.publish(exchange, routingKey, body, options, (err) => {
    if (settled) return
    settled = true
    clearTimeout(timer)
    if (err) this.logerror(`AMQP publish nack - routingKey: ${routingKey}: ${err.message}`)
  })
}

plugin._parse_smtp_code = function (msg, fallback) {
  const m = /^(\d{3})/.exec(msg || '')
  return m ? parseInt(m[1], 10) : fallback
}
