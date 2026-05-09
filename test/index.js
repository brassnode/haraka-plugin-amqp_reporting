'use strict'

const assert = require('node:assert/strict')
const { beforeEach, describe, it } = require('node:test')
const fixtures = require('haraka-test-fixtures')

beforeEach(() => {
  this.plugin = new fixtures.plugin('amqp_reporting')
  this.plugin.amqp = null

  if (process.env.HARAKA_COVERAGE) {
    const mod = require('../index.js')
    Object.assign(this.plugin, mod)
  }
})

function makeHmail(notes = {}, domain = 'example.com', rcpts = []) {
  return { todo: { notes, domain, rcpt_to: rcpts } }
}

function addr(email) {
  return { address: () => email }
}

describe('plugin', () => {
  it('loads', () => {
    assert.ok(this.plugin)
  })

  it('has a register function', () => {
    assert.equal(typeof this.plugin.register, 'function')
  })
})

describe('register', () => {
  it('registers all hooks when enabled', () => {
    const registered = []
    this.plugin.register_hook = (e) => registered.push(e)
    this.plugin.register()
    assert.deepStrictEqual(registered, ['init_child', 'queue', 'delivered', 'bounce', 'deferred'])
  })

  it('returns early and logs when disabled', () => {
    this.plugin.load_amqp_reporting_ini = function () {
      this.cfg = { main: { enabled: false } }
    }
    let hooksCalled = false
    let logged = false
    this.plugin.register_hook = () => { hooksCalled = true }
    this.plugin.loginfo = () => { logged = true }
    this.plugin.register()
    assert.strictEqual(hooksCalled, false)
    assert.strictEqual(logged, true)
  })
})

describe('load_amqp_reporting_ini', () => {
  it('sets this.cfg', () => {
    this.plugin.load_amqp_reporting_ini()
    assert.equal(typeof this.plugin.cfg, 'object')
  })

  it('cfg.main.enabled defaults to true', () => {
    this.plugin.load_amqp_reporting_ini()
    assert.strictEqual(this.plugin.cfg.main.enabled, true)
  })
})

describe('hook_init_child', () => {
  beforeEach(() => {
    this.plugin.load_amqp_reporting_ini()
    this.plugin.loginfo  = () => {}
    this.plugin.logerror = () => {}
  })

  it('calls next on connect error', () => {
    this.plugin._connect = (url, cb) => cb(new Error('refused'))
    let called = false
    this.plugin.hook_init_child(() => { called = true })
    assert.ok(called)
    assert.strictEqual(this.plugin.amqp, null)
  })

  it('calls next on createConfirmChannel error', () => {
    this.plugin._connect = (url, cb) => {
      cb(null, {
        on: () => {},
        createConfirmChannel: (cb2) => cb2(new Error('channel fail')),
      })
    }
    let called = false
    this.plugin.hook_init_child(() => { called = true })
    assert.ok(called)
    assert.strictEqual(this.plugin.amqp, null)
  })

  it('calls next on assertExchange error', () => {
    this.plugin._connect = (url, cb) => {
      cb(null, {
        on: () => {},
        createConfirmChannel: (cb2) => cb2(null, {
          assertExchange: (ex, type, opts, cb3) => cb3(new Error('assert fail')),
        }),
      })
    }
    let called = false
    this.plugin.hook_init_child(() => { called = true })
    assert.ok(called)
    assert.strictEqual(this.plugin.amqp, null)
  })

  it('sets this.amqp and calls next on success', () => {
    const fakeCh   = { assertExchange: (ex, type, opts, cb) => cb(null) }
    const fakeConn = {
      on: () => {},
      createConfirmChannel: (cb) => cb(null, fakeCh),
    }
    this.plugin._connect = (url, cb) => cb(null, fakeConn)
    let called = false
    this.plugin.hook_init_child(() => { called = true })
    assert.ok(called)
    assert.ok(this.plugin.amqp)
    assert.strictEqual(this.plugin.amqp.ch, fakeCh)
    assert.strictEqual(this.plugin.amqp.conn, fakeConn)
  })

  it('sets this.amqp to null on connection error event', () => {
    let errorHandler
    const fakeConn = {
      on: (event, handler) => { if (event === 'error') errorHandler = handler },
      createConfirmChannel: (cb) => cb(null, {
        assertExchange: (ex, type, opts, cb2) => cb2(null),
      }),
    }
    this.plugin._connect = (url, cb) => cb(null, fakeConn)
    this.plugin.hook_init_child(() => {})
    assert.ok(this.plugin.amqp)
    errorHandler(new Error('connection dropped'))
    assert.strictEqual(this.plugin.amqp, null)
  })
})

describe('hook_queue', () => {
  let conn

  beforeEach(() => {
    conn = fixtures.connection.createConnection({})
    conn.init_transaction()
    conn.transaction.remove_header = () => {}
  })

  it('stashes job_id from X-Job-Id header', () => {
    conn.transaction.header.add('X-Job-Id', 'job-abc')
    this.plugin.hook_queue(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_job_id, 'job-abc')
  })

  it('stashes ip_id from X-Ip-Id header', () => {
    conn.transaction.header.add('X-Ip-Id', 'ip-123')
    this.plugin.hook_queue(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_ip_id, 'ip-123')
  })

  it('stashes empty strings when headers are absent', () => {
    this.plugin.hook_queue(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_job_id, '')
    assert.strictEqual(conn.transaction.notes.amqp_ip_id, '')
  })

  it('trims whitespace from header values', () => {
    conn.transaction.header.add('X-Job-Id', '  job-padded  ')
    this.plugin.hook_queue(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_job_id, 'job-padded')
  })

  it('calls remove_header for X-Job-Id', () => {
    const removed = []
    conn.transaction.remove_header = (n) => removed.push(n)
    this.plugin.hook_queue(() => {}, conn)
    assert.ok(removed.includes('X-Job-Id'))
  })

  it('calls remove_header for X-Ip-Id', () => {
    const removed = []
    conn.transaction.remove_header = (n) => removed.push(n)
    this.plugin.hook_queue(() => {}, conn)
    assert.ok(removed.includes('X-Ip-Id'))
  })

  it('calls next()', () => {
    let called = false
    this.plugin.hook_queue(() => { called = true }, conn)
    assert.ok(called)
  })

  it('calls next() without error when transaction is null', () => {
    conn.transaction = null
    let called = false
    assert.doesNotThrow(() => this.plugin.hook_queue(() => { called = true }, conn))
    assert.ok(called)
  })
})

// ─── _connect ────────────────────────────────────────────────────────────────

describe('_connect', () => {
  it('delegates to amqplib.connect', () => {
    const amqp = require('amqplib/callback_api')
    const calls = []
    const orig = amqp.connect
    amqp.connect = (url, cb) => calls.push({ url, cb })
    const cb = () => {}
    this.plugin._connect('amqp://test', cb)
    amqp.connect = orig
    assert.strictEqual(calls[0].url, 'amqp://test')
    assert.strictEqual(calls[0].cb, cb)
  })
})

// ─── hook_delivered ──────────────────────────────────────────────────────────

describe('hook_delivered', () => {
  let published

  beforeEach(() => {
    published = []
    this.plugin._publish = (k, e) => published.push({ k, e })
  })

  it('calls next()', () => {
    let called = false
    this.plugin.hook_delivered(() => { called = true }, makeHmail(), null, null)
    assert.ok(called)
  })

  it('publishes with routing key outcome.delivered', () => {
    this.plugin.hook_delivered(() => {}, makeHmail(), null, [])
    assert.strictEqual(published[0].k, 'outcome.delivered')
  })

  it('sets status to delivered', () => {
    this.plugin.hook_delivered(() => {}, makeHmail(), null, [])
    assert.strictEqual(published[0].e.status, 'delivered')
  })

  it('includes jobId from hmail.todo.notes', () => {
    this.plugin.hook_delivered(() => {}, makeHmail({ amqp_job_id: 'job-42' }), null, [])
    assert.strictEqual(published[0].e.jobId, 'job-42')
  })

  it('includes ipId from hmail.todo.notes', () => {
    this.plugin.hook_delivered(() => {}, makeHmail({ amqp_ip_id: 'ip-7' }), null, [])
    assert.strictEqual(published[0].e.ipId, 'ip-7')
  })

  it('parses smtpCode from params[1]', () => {
    this.plugin.hook_delivered(() => {}, makeHmail(), null, [null, '250 2.0.0 OK', 'example.com'])
    assert.strictEqual(published[0].e.smtpCode, 250)
  })

  it('uses recipientAddress from params[0]', () => {
    this.plugin.hook_delivered(() => {}, makeHmail(), null, [addr('user@example.com'), '250 OK', 'example.com'])
    assert.strictEqual(published[0].e.recipientAddress, 'user@example.com')
  })

  it('uses domain from params[2]', () => {
    this.plugin.hook_delivered(() => {}, makeHmail(), null, [null, '250 OK', 'gmail.com'])
    assert.strictEqual(published[0].e.domain, 'gmail.com')
  })

  it('sets attemptedAt to an ISO 8601 string', () => {
    this.plugin.hook_delivered(() => {}, makeHmail(), null, [])
    const { attemptedAt } = published[0].e
    assert.strictEqual(new Date(attemptedAt).toISOString(), attemptedAt)
  })

  it('handles null params gracefully', () => {
    assert.doesNotThrow(() =>
      this.plugin.hook_delivered(() => {}, makeHmail(), null, null)
    )
    assert.strictEqual(published[0].e.recipientAddress, '')
    assert.strictEqual(published[0].e.domain, '')
  })

  it('handles null hmail gracefully', () => {
    assert.doesNotThrow(() =>
      this.plugin.hook_delivered(() => {}, null, null, null)
    )
    assert.strictEqual(published[0].e.jobId, '')
    assert.strictEqual(published[0].e.ipId, '')
  })
})

describe('hook_bounce', () => {
  let published

  beforeEach(() => {
    published = []
    this.plugin._publish = (k, e) => published.push({ k, e })
  })

  it('calls next()', () => {
    let called = false
    this.plugin.hook_bounce(() => { called = true }, makeHmail(), null)
    assert.ok(called)
  })

  it('publishes with routing key outcome.bounced', () => {
    this.plugin.hook_bounce(() => {}, makeHmail(), null)
    assert.strictEqual(published[0].k, 'outcome.bounced')
  })

  it('sets status to bounced', () => {
    this.plugin.hook_bounce(() => {}, makeHmail(), null)
    assert.strictEqual(published[0].e.status, 'bounced')
  })

  it('includes jobId from hmail.todo.notes', () => {
    this.plugin.hook_bounce(() => {}, makeHmail({ amqp_job_id: 'job-99' }), null)
    assert.strictEqual(published[0].e.jobId, 'job-99')
  })

  it('uses smtpCode from error.code', () => {
    this.plugin.hook_bounce(() => {}, makeHmail(), { code: 550, message: '550 5.1.1 User unknown' })
    assert.strictEqual(published[0].e.smtpCode, 550)
  })

  it('uses smtpMessage from error.message', () => {
    this.plugin.hook_bounce(() => {}, makeHmail(), { code: 550, message: '550 5.1.1 User unknown' })
    assert.strictEqual(published[0].e.smtpMessage, '550 5.1.1 User unknown')
  })

  it('uses first rcpt_to address', () => {
    const hmail = makeHmail({}, 'example.com', [addr('a@b.com'), addr('c@d.com')])
    this.plugin.hook_bounce(() => {}, hmail, null)
    assert.strictEqual(published[0].e.recipientAddress, 'a@b.com')
  })

  it('uses domain from hmail.todo.domain', () => {
    this.plugin.hook_bounce(() => {}, makeHmail({}, 'outlook.com'), null)
    assert.strictEqual(published[0].e.domain, 'outlook.com')
  })

  it('defaults smtpCode to 0 when error is null', () => {
    this.plugin.hook_bounce(() => {}, makeHmail(), null)
    assert.strictEqual(published[0].e.smtpCode, 0)
  })

  it('handles null error gracefully', () => {
    assert.doesNotThrow(() =>
      this.plugin.hook_bounce(() => {}, makeHmail(), null)
    )
  })

  it('handles null hmail gracefully', () => {
    assert.doesNotThrow(() =>
      this.plugin.hook_bounce(() => {}, null, null)
    )
    assert.strictEqual(published[0].e.jobId, '')
    assert.strictEqual(published[0].e.domain, '')
  })
})

describe('hook_deferred', () => {
  let published

  beforeEach(() => {
    published = []
    this.plugin._publish = (k, e) => published.push({ k, e })
  })

  it('calls next()', () => {
    let called = false
    this.plugin.hook_deferred(() => { called = true }, makeHmail(), null)
    assert.ok(called)
  })

  it('publishes with routing key outcome.deferred', () => {
    this.plugin.hook_deferred(() => {}, makeHmail(), [])
    assert.strictEqual(published[0].k, 'outcome.deferred')
  })

  it('sets status to deferred', () => {
    this.plugin.hook_deferred(() => {}, makeHmail(), [])
    assert.strictEqual(published[0].e.status, 'deferred')
  })

  it('includes jobId from hmail.todo.notes', () => {
    this.plugin.hook_deferred(() => {}, makeHmail({ amqp_job_id: 'job-55' }), [])
    assert.strictEqual(published[0].e.jobId, 'job-55')
  })

  it('parses smtpCode from params[1]', () => {
    this.plugin.hook_deferred(() => {}, makeHmail(), ['example.com', '421 4.7.0 Try again later', addr('r@example.com')])
    assert.strictEqual(published[0].e.smtpCode, 421)
  })

  it('falls back to 421 when params[1] has no leading digits', () => {
    this.plugin.hook_deferred(() => {}, makeHmail(), ['example.com', 'connection timeout'])
    assert.strictEqual(published[0].e.smtpCode, 421)
  })

  it('uses domain from params[0]', () => {
    this.plugin.hook_deferred(() => {}, makeHmail(), ['yahoo.com', ''])
    assert.strictEqual(published[0].e.domain, 'yahoo.com')
  })

  it('uses recipientAddress from params[2]', () => {
    this.plugin.hook_deferred(() => {}, makeHmail(), ['example.com', '', addr('r@example.com')])
    assert.strictEqual(published[0].e.recipientAddress, 'r@example.com')
  })

  it('handles null params gracefully', () => {
    assert.doesNotThrow(() =>
      this.plugin.hook_deferred(() => {}, makeHmail(), null)
    )
    assert.strictEqual(published[0].e.domain, '')
    assert.strictEqual(published[0].e.recipientAddress, '')
  })

  it('handles null hmail gracefully', () => {
    assert.doesNotThrow(() =>
      this.plugin.hook_deferred(() => {}, null, null)
    )
    assert.strictEqual(published[0].e.jobId, '')
  })
})

describe('_publish', () => {
  it('does nothing when this.amqp is null', () => {
    this.plugin.amqp = null
    assert.doesNotThrow(() => this.plugin._publish('outcome.delivered', { jobId: 'x' }))
  })

  it('calls ch.publish with persistent and mandatory options', () => {
    const calls = []
    this.plugin.amqp = {
      ch: { publish: (ex, key, body, opts) => calls.push({ opts }) },
      exchange: 'mailing.delivery.outcomes',
    }
    this.plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.strictEqual(calls[0].opts.persistent, true)
    assert.strictEqual(calls[0].opts.mandatory, true)
  })

  it('publishes to the configured exchange', () => {
    const exchanges = []
    this.plugin.amqp = {
      ch: { publish: (ex) => exchanges.push(ex) },
      exchange: 'mailing.delivery.outcomes',
    }
    this.plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.strictEqual(exchanges[0], 'mailing.delivery.outcomes')
  })

  it('serialises the event as JSON', () => {
    let captured
    this.plugin.amqp = {
      ch: { publish: (ex, key, body) => { captured = body } },
      exchange: 'x',
    }
    this.plugin._publish('outcome.delivered', { jobId: 'job-1', status: 'delivered' })
    assert.strictEqual(JSON.parse(captured.toString()).jobId, 'job-1')
  })

  it('logs error on nack callback', () => {
    const errors = []
    this.plugin.logerror = (m) => errors.push(m)
    this.plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => cb(new Error('nacked')) },
      exchange: 'x',
    }
    this.plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.ok(errors.some((m) => m.includes('nack')))
  })

  it('does not log on successful ack', () => {
    const errors = []
    this.plugin.logerror = (m) => errors.push(m)
    this.plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => cb(null) },
      exchange: 'x',
    }
    this.plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.strictEqual(errors.length, 0)
  })

  it('logs error on publish timeout', async () => {
    const errors = []
    this.plugin.logerror = (m) => errors.push(m)
    this.plugin.PUBLISH_TIMEOUT = 10
    this.plugin.amqp = {
      ch: { publish: () => {} }, // never calls back
      exchange: 'x',
    }
    this.plugin._publish('outcome.delivered', { jobId: 'job-timeout' })
    await new Promise((r) => setTimeout(r, 50))
    assert.ok(errors.some((m) => m.includes('timeout')))
  })

  it('does not double-log when callback settles before timer fires', async () => {
    const errors = []
    this.plugin.logerror = (m) => errors.push(m)
    this.plugin.PUBLISH_TIMEOUT = 20
    this.plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => cb(new Error('nacked')) },
      exchange: 'x',
    }
    this.plugin._publish('outcome.delivered', { jobId: 'x' })
    await new Promise((r) => setTimeout(r, 60))
    assert.strictEqual(errors.filter((m) => m.includes('nack')).length, 1)
    assert.strictEqual(errors.filter((m) => m.includes('timeout')).length, 0)
  })

  it('does not double-log when timer settles before callback fires', async () => {
    const errors = []
    this.plugin.logerror = (m) => errors.push(m)
    this.plugin.PUBLISH_TIMEOUT = 10
    let lateCb
    this.plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => { lateCb = cb } },
      exchange: 'x',
    }
    this.plugin._publish('outcome.delivered', { jobId: 'x' })
    await new Promise((r) => setTimeout(r, 50))
    assert.ok(errors.some((m) => m.includes('timeout')))
    lateCb(new Error('late nack'))
    assert.strictEqual(errors.filter((m) => m.includes('nack')).length, 0)
  })
})

describe('_parse_smtp_code', () => {
  it('extracts code from leading digits', () => {
    assert.strictEqual(this.plugin._parse_smtp_code('550 5.1.1 Nope', 0), 550)
  })

  it('extracts 250 from delivery response', () => {
    assert.strictEqual(this.plugin._parse_smtp_code('250 2.0.0 OK', 0), 250)
  })

  it('extracts 421 from deferral response', () => {
    assert.strictEqual(this.plugin._parse_smtp_code('421 4.7.0 Try again later', 0), 421)
  })

  it('returns fallback when message is empty string', () => {
    assert.strictEqual(this.plugin._parse_smtp_code('', 421), 421)
  })

  it('returns fallback when message is null', () => {
    assert.strictEqual(this.plugin._parse_smtp_code(null, 250), 250)
  })

  it('returns fallback when message has no leading digits', () => {
    assert.strictEqual(this.plugin._parse_smtp_code('connection timeout', 421), 421)
  })
})
