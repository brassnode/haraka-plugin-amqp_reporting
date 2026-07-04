'use strict'

const assert = require('node:assert/strict')
const { afterEach, beforeEach, describe, it } = require('node:test')
const fixtures = require('haraka-test-fixtures')

let plugin

beforeEach(() => {
  plugin = new fixtures.plugin('amqp_reporting')
  plugin.amqp = null
  plugin._publishQueue = []

  if (process.env.HARAKA_COVERAGE) {
    const mod = require('../index.js')
    Object.assign(plugin, mod)
  }
})

function makeHmail(notes = {}, domain = 'example.com', rcpts = []) {
  return { todo: { notes, domain, rcpt_to: rcpts } }
}

function addr(email) {
  return { original: `<${email}>` }
}

// Standard delivered params with one recipient — used when the specific recipient doesn't matter
function deliveredParams(rcpt = 'rcpt@example.com', response = '250 2.0.0 OK') {
  return [null, null, response, 0, 25, 'smtp', [addr(rcpt)]]
}

// ─── plugin ──────────────────────────────────────────────────────────────────

describe('plugin', () => {
  it('loads', () => {
    assert.ok(plugin)
  })

  it('has a register function', () => {
    assert.equal(typeof plugin.register, 'function')
  })
})

// ─── register ────────────────────────────────────────────────────────────────

describe('register', () => {
  it('makes no explicit register_hook calls when enabled', () => {
    const registered = []
    plugin.register_hook = (e) => registered.push(e)
    plugin.register()
    assert.deepStrictEqual(registered, [])
  })

  it('returns early and logs when disabled', () => {
    plugin.load_amqp_reporting_ini = function () {
      this.cfg = { main: { enabled: false } }
    }
    let hooksCalled = false
    let logged = false
    plugin.register_hook = () => { hooksCalled = true }
    plugin.loginfo = () => { logged = true }
    plugin.register()
    assert.strictEqual(hooksCalled, false)
    assert.strictEqual(logged, true)
  })
})

// ─── load_amqp_reporting_ini ─────────────────────────────────────────────────

describe('load_amqp_reporting_ini', () => {
  it('sets this.cfg', () => {
    plugin.load_amqp_reporting_ini()
    assert.equal(typeof plugin.cfg, 'object')
  })

  it('cfg.main.enabled defaults to true', () => {
    plugin.load_amqp_reporting_ini()
    assert.strictEqual(plugin.cfg.main.enabled, true)
  })

  it('calls itself when config reloads', () => {
    let reloadCb
    const stubCfg = {
      main: { enabled: true },
      connection: { amqp_url: 'amqp://localhost' },
      publishing: { exchange: 'x' },
      headers: {},
    }
    plugin.config.get = (file, opts, cb) => { reloadCb = cb; return stubCfg }
    plugin._sanitize_url = () => 'amqp://localhost'
    plugin.load_amqp_reporting_ini()

    let reloaded = false
    plugin.load_amqp_reporting_ini = () => { reloaded = true }
    reloadCb()
    assert.ok(reloaded)
  })
})

// ─── hook_init_child ─────────────────────────────────────────────────────────

describe('hook_init_child', () => {
  beforeEach(() => {
    plugin.load_amqp_reporting_ini()
    plugin.loginfo  = () => {}
    plugin.logerror = () => {}
    plugin.logdebug = () => {}
  })

  afterEach(() => {
    if (plugin._reconnectTimer) clearTimeout(plugin._reconnectTimer)
  })

  it('calls next on connect error', () => {
    plugin._connect = (url, cb) => cb(new Error('refused'))
    let called = false
    plugin.hook_init_child(() => { called = true })
    assert.ok(called)
    assert.strictEqual(plugin.amqp, null)
  })

  it('calls next on createConfirmChannel error', () => {
    plugin._connect = (url, cb) => {
      cb(null, {
        on: () => {},
        close: (cb2) => cb2 && cb2(),
        createConfirmChannel: (cb2) => cb2(new Error('channel fail')),
      })
    }
    let called = false
    plugin.hook_init_child(() => { called = true })
    assert.ok(called)
    assert.strictEqual(plugin.amqp, null)
  })

  it('calls next on assertExchange error', () => {
    plugin._connect = (url, cb) => {
      cb(null, {
        on: () => {},
        close: (cb2) => cb2 && cb2(),
        createConfirmChannel: (cb2) => cb2(null, {
          assertExchange: (ex, type, opts, cb3) => cb3(new Error('assert fail')),
        }),
      })
    }
    let called = false
    plugin.hook_init_child(() => { called = true })
    assert.ok(called)
    assert.strictEqual(plugin.amqp, null)
  })

  it('sets this.amqp and calls next on success', () => {
    const fakeCh   = { assertExchange: (ex, type, opts, cb) => cb(null) }
    const fakeConn = {
      on: () => {},
      createConfirmChannel: (cb) => cb(null, fakeCh),
    }
    plugin._connect = (url, cb) => cb(null, fakeConn)
    let called = false
    plugin.hook_init_child(() => { called = true })
    assert.ok(called)
    assert.ok(plugin.amqp)
    assert.strictEqual(plugin.amqp.ch, fakeCh)
    assert.strictEqual(plugin.amqp.conn, fakeConn)
  })

  it('sets this.amqp to null on connection error event', () => {
    let errorHandler
    const fakeConn = {
      on: (event, handler) => { if (event === 'error') errorHandler = handler },
      createConfirmChannel: (cb) => cb(null, {
        assertExchange: (ex, type, opts, cb2) => cb2(null),
      }),
    }
    plugin._connect = (url, cb) => cb(null, fakeConn)
    plugin.hook_init_child(() => {})
    assert.ok(plugin.amqp)
    errorHandler(new Error('connection dropped'))
    assert.strictEqual(plugin.amqp, null)
  })

  it('does nothing when connection error fires after amqp is already cleared', () => {
    let errorHandler
    const fakeConn = {
      on: (event, handler) => { if (event === 'error') errorHandler = handler },
      createConfirmChannel: (cb) => cb(null, {
        assertExchange: (ex, type, opts, cb2) => cb2(null),
      }),
    }
    plugin._connect = (url, cb) => cb(null, fakeConn)
    plugin.hook_init_child(() => {})
    plugin.amqp = null
    assert.doesNotThrow(() => errorHandler(new Error('late error')))
    assert.strictEqual(plugin.amqp, null)
  })
})

// ─── hook_shutdown ───────────────────────────────────────────────────────────

describe('hook_shutdown', () => {
  it('calls next immediately when amqp is null', () => {
    plugin.amqp = null
    let called = false
    plugin.hook_shutdown(() => { called = true })
    assert.ok(called)
  })

  it('closes the connection and calls next', () => {
    let closed = false
    plugin.amqp = { conn: { close: (cb) => { closed = true; cb(null) } } }
    let called = false
    plugin.hook_shutdown(() => { called = true })
    assert.ok(closed)
    assert.ok(called)
  })

  it('calls next even if conn.close errors', () => {
    const errors = []
    plugin.logerror = (m) => errors.push(m)
    plugin.amqp = { conn: { close: (cb) => cb(new Error('close failed')) } }
    let called = false
    plugin.hook_shutdown(() => { called = true })
    assert.ok(called)
    assert.ok(errors.some((m) => m.includes('close failed')))
  })

  it('sets amqp to null', () => {
    plugin.amqp = { conn: { close: (cb) => cb(null) } }
    plugin.hook_shutdown(() => {})
    assert.strictEqual(plugin.amqp, null)
  })

  it('cancels a pending reconnect timer', () => {
    plugin._reconnectTimer = setTimeout(() => {}, 60000)
    plugin.amqp = null
    plugin.hook_shutdown(() => {})
    assert.strictEqual(plugin._reconnectTimer, null)
  })
})

// ─── hook_queue_outbound ──────────────────────────────────────────────────────────────

describe('hook_queue_outbound', () => {
  let conn

  beforeEach(() => {
    plugin.load_amqp_reporting_ini()
    conn = fixtures.connection.createConnection({})
    conn.init_transaction()
    conn.transaction.remove_header = () => {}
  })

  it('stashes job_id from X-Job-Id header', () => {
    conn.transaction.header.add('X-Job-Id', 'job-abc')
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_job_id, 'job-abc')
  })

  it('stashes empty string when job header is absent', () => {
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_job_id, '')
  })

  it('stashes message_id from Message-ID header stripping angle brackets', () => {
    conn.transaction.header.add('Message-ID', '<abc-123@mail.example.com>')
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_message_id, 'abc-123@mail.example.com')
  })

  it('stashes empty string when Message-ID header is absent', () => {
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_message_id, '')
  })

  it('trims whitespace from header values', () => {
    conn.transaction.header.add('X-Job-Id', '  job-padded  ')
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_job_id, 'job-padded')
  })

  it('stashes the bare address from a From header with a display name', () => {
    conn.transaction.header.add('From', '"Acme Support" <no-reply@acme.test>')
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_from, 'no-reply@acme.test')
  })

  it('stashes empty string when From header is absent', () => {
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_from, '')
  })

  it('stashes the first address from a multi-address From header', () => {
    conn.transaction.header.add('From', 'Acme <a@acme.test>, ops@acme.test')
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_from, 'a@acme.test')
  })

  it('stashes empty string when the From header is unparseable', () => {
    conn.transaction.header.add('From', 'not-an-address')
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_from, '')
  })

  it('calls remove_header for X-Job-Id', () => {
    const removed = []
    conn.transaction.remove_header = (n) => removed.push(n)
    plugin.hook_queue_outbound(() => {}, conn)
    assert.ok(removed.includes('X-Job-Id'))
  })


  it('calls next()', () => {
    let called = false
    plugin.hook_queue_outbound(() => { called = true }, conn)
    assert.ok(called)
  })

  it('calls next() without error when transaction is null', () => {
    conn.transaction = null
    let called = false
    assert.doesNotThrow(() => plugin.hook_queue_outbound(() => { called = true }, conn))
    assert.ok(called)
  })

  it('skips header extraction when job_id_header is empty', () => {
    plugin.cfg.headers.job_id_header = ''
    conn.transaction.header.add('X-Job-Id', 'job-abc')
    plugin.hook_queue_outbound(() => {}, conn)
    assert.strictEqual(conn.transaction.notes.amqp_job_id, '')
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
    plugin._connect('amqp://test', cb)
    amqp.connect = orig
    assert.strictEqual(calls[0].url, 'amqp://test')
    assert.strictEqual(calls[0].cb, cb)
  })
})

// ─── _sanitize_url ───────────────────────────────────────────────────────────

describe('_sanitize_url', () => {
  it('redacts the password', () => {
    const result = plugin._sanitize_url('amqp://user:secret@host:5672/vhost')
    assert.ok(!result.includes('secret'))
    assert.ok(result.includes('***'))
  })

  it('leaves a URL without a password unchanged', () => {
    const url = 'amqp://host:5672'
    assert.strictEqual(plugin._sanitize_url(url), url)
  })

  it('returns the raw string when the URL is malformed', () => {
    assert.strictEqual(plugin._sanitize_url('not-a-url'), 'not-a-url')
  })
})

// ─── hook_delivered ──────────────────────────────────────────────────────────

// Haraka calls: plugins.run_hooks('delivered', hmail, [host, ip, response, delay, port, mode, ok_recips, secured, authenticated])
// Hook receives: (next, hmail, params) where params = [host, ip, response, delay, port, mode, ok_recips, ...]
describe('hook_delivered', () => {
  let published

  beforeEach(() => {
    published = []
    plugin._publish = (k, e) => published.push({ k, e })
  })

  it('calls next()', () => {
    let called = false
    plugin.hook_delivered(() => { called = true }, makeHmail(), null)
    assert.ok(called)
  })

  it('publishes only once when called twice with the same hmail', () => {
    const hmail = makeHmail({}, 'example.com', [addr('r@example.com')])
    plugin.hook_delivered(() => {}, hmail, deliveredParams())
    plugin.hook_delivered(() => {}, hmail, deliveredParams())
    assert.strictEqual(published.length, 1)
  })

  it('publishes with routing key outcome.delivered', () => {
    plugin.hook_delivered(() => {}, makeHmail(), deliveredParams())
    assert.strictEqual(published[0].k, 'outcome.delivered')
  })

  it('sets status to delivered', () => {
    plugin.hook_delivered(() => {}, makeHmail(), deliveredParams())
    assert.strictEqual(published[0].e.status, 'delivered')
  })

  it('includes jobId from hmail.todo.notes', () => {
    plugin.hook_delivered(() => {}, makeHmail({ amqp_job_id: 'job-42' }), deliveredParams())
    assert.strictEqual(published[0].e.jobId, 'job-42')
  })

  it('parses smtpCode from params[2] (SMTP response)', () => {
    plugin.hook_delivered(() => {}, makeHmail(), deliveredParams('r@example.com', '250 2.0.0 OK'))
    assert.strictEqual(published[0].e.smtpCode, 250)
  })

  it('uses recipientAddress from params[6] ok_recips', () => {
    plugin.hook_delivered(() => {}, makeHmail(), [null, null, '250 OK', 0, 25, 'smtp', [addr('user@example.com')]])
    assert.strictEqual(published[0].e.recipientAddress, 'user@example.com')
  })

  it('publishes one event per recipient', () => {
    plugin.hook_delivered(() => {}, makeHmail(), [null, null, '250 OK', 0, 25, 'smtp', [addr('a@b.com'), addr('c@d.com')]])
    assert.strictEqual(published.length, 2)
    assert.strictEqual(published[1].e.recipientAddress, 'c@d.com')
  })

  it('uses domain from hmail.todo.domain', () => {
    plugin.hook_delivered(() => {}, makeHmail({}, 'gmail.com'), deliveredParams())
    assert.strictEqual(published[0].e.domain, 'gmail.com')
  })

  it('includes senderAddress from hmail.todo.mail_from', () => {
    const hmail = {
      todo: { notes: {}, domain: 'example.com', rcpt_to: [], mail_from: { original: '<sender@example.com>' } },
    }
    plugin.hook_delivered(() => {}, hmail, deliveredParams())
    assert.strictEqual(published[0].e.senderAddress, 'sender@example.com')
  })

  it('uses empty string for domain when hmail.todo.domain is absent', () => {
    plugin.hook_delivered(
      () => {},
      { todo: { notes: {}, rcpt_to: [] } },
      ['mail.gmail.com', null, '250 OK', 0, 25, 'smtp', [addr('r@gmail.com')]],
    )
    assert.strictEqual(published[0].e.domain, '')
  })

  it('sets attemptedAt to a Unix ms timestamp', () => {
    plugin.hook_delivered(() => {}, makeHmail(), deliveredParams())
    const { attemptedAt } = published[0].e
    assert.strictEqual(typeof attemptedAt, 'number')
    assert.ok(attemptedAt > 0)
  })

  it('publishes no events when params is null', () => {
    assert.doesNotThrow(() => plugin.hook_delivered(() => {}, makeHmail(), null))
    assert.strictEqual(published.length, 0)
  })

  it('publishes no events when hmail is null', () => {
    assert.doesNotThrow(() => plugin.hook_delivered(() => {}, null, null))
    assert.strictEqual(published.length, 0)
  })
})

// ─── hook_bounce ─────────────────────────────────────────────────────────────

describe('hook_bounce', () => {
  let published

  beforeEach(() => {
    published = []
    plugin._publish = (k, e) => published.push({ k, e })
  })

  it('calls next()', () => {
    let called = false
    plugin.hook_bounce(() => { called = true }, makeHmail(), null)
    assert.ok(called)
  })

  it('publishes with routing key outcome.bounced', () => {
    plugin.hook_bounce(() => {}, makeHmail({}, 'example.com', [addr('a@b.com')]), null)
    assert.strictEqual(published[0].k, 'outcome.bounced')
  })

  it('sets status to bounced', () => {
    plugin.hook_bounce(() => {}, makeHmail({}, 'example.com', [addr('a@b.com')]), null)
    assert.strictEqual(published[0].e.status, 'bounced')
  })

  it('includes jobId from hmail.todo.notes', () => {
    plugin.hook_bounce(() => {}, makeHmail({ amqp_job_id: 'job-99' }, 'example.com', [addr('a@b.com')]), null)
    assert.strictEqual(published[0].e.jobId, 'job-99')
  })

  it('uses smtpCode from error.code', () => {
    plugin.hook_bounce(() => {}, makeHmail({}, 'example.com', [addr('a@b.com')]), { code: 550, message: '550 5.1.1 User unknown' })
    assert.strictEqual(published[0].e.smtpCode, 550)
  })

  it('uses smtpMessage from error.message', () => {
    plugin.hook_bounce(() => {}, makeHmail({}, 'example.com', [addr('a@b.com')]), { code: 550, message: '550 5.1.1 User unknown' })
    assert.strictEqual(published[0].e.smtpMessage, '550 5.1.1 User unknown')
  })

  it('publishes one event per recipient', () => {
    const hmail = makeHmail({}, 'example.com', [addr('a@b.com'), addr('c@d.com')])
    plugin.hook_bounce(() => {}, hmail, null)
    assert.strictEqual(published.length, 2)
    assert.strictEqual(published[0].e.recipientAddress, 'a@b.com')
    assert.strictEqual(published[1].e.recipientAddress, 'c@d.com')
  })

  it('uses domain from hmail.todo.domain', () => {
    plugin.hook_bounce(() => {}, makeHmail({}, 'outlook.com', [addr('a@outlook.com')]), null)
    assert.strictEqual(published[0].e.domain, 'outlook.com')
  })

  it('defaults smtpCode to 0 when error is null', () => {
    plugin.hook_bounce(() => {}, makeHmail({}, 'example.com', [addr('a@b.com')]), null)
    assert.strictEqual(published[0].e.smtpCode, 0)
  })

  it('publishes no events when rcpt_to is empty', () => {
    assert.doesNotThrow(() => plugin.hook_bounce(() => {}, makeHmail(), null))
    assert.strictEqual(published.length, 0)
  })

  it('handles null hmail gracefully', () => {
    assert.doesNotThrow(() => plugin.hook_bounce(() => {}, null, null))
    assert.strictEqual(published.length, 0)
  })
})

// ─── hook_deferred ───────────────────────────────────────────────────────────

// Haraka calls: plugins.run_hooks('deferred', hmail, { delay, err, ...extra })
// Hook receives: (next, hmail, params) where params = { delay, err }
describe('hook_deferred', () => {
  let published

  beforeEach(() => {
    published = []
    plugin._publish = (k, e) => published.push({ k, e })
  })

  it('calls next()', () => {
    let called = false
    plugin.hook_deferred(() => { called = true }, makeHmail(), null)
    assert.ok(called)
  })

  it('publishes with routing key outcome.deferred', () => {
    plugin.hook_deferred(() => {}, makeHmail({}, 'example.com', [addr('r@example.com')]), {})
    assert.strictEqual(published[0].k, 'outcome.deferred')
  })

  it('sets status to deferred', () => {
    plugin.hook_deferred(() => {}, makeHmail({}, 'example.com', [addr('r@example.com')]), {})
    assert.strictEqual(published[0].e.status, 'deferred')
  })

  it('includes jobId from hmail.todo.notes', () => {
    plugin.hook_deferred(() => {}, makeHmail({ amqp_job_id: 'job-55' }, 'example.com', [addr('r@example.com')]), {})
    assert.strictEqual(published[0].e.jobId, 'job-55')
  })

  it('parses smtpCode from params.err', () => {
    plugin.hook_deferred(() => {}, makeHmail({}, 'example.com', [addr('r@example.com')]), { delay: 300, err: '421 4.7.0 Try again later' })
    assert.strictEqual(published[0].e.smtpCode, 421)
  })

  it('falls back to 421 when params.err has no leading digits', () => {
    plugin.hook_deferred(() => {}, makeHmail({}, 'example.com', [addr('r@example.com')]), { delay: 300, err: 'connection timeout' })
    assert.strictEqual(published[0].e.smtpCode, 421)
  })

  it('uses domain from hmail.todo.domain', () => {
    plugin.hook_deferred(() => {}, makeHmail({}, 'yahoo.com', [addr('r@yahoo.com')]), { delay: 300, err: '' })
    assert.strictEqual(published[0].e.domain, 'yahoo.com')
  })

  it('uses recipientAddress from hmail.todo.rcpt_to', () => {
    plugin.hook_deferred(() => {}, makeHmail({}, 'example.com', [addr('r@example.com')]), { delay: 300 })
    assert.strictEqual(published[0].e.recipientAddress, 'r@example.com')
  })

  it('publishes one event per recipient', () => {
    const hmail = makeHmail({}, 'example.com', [addr('a@b.com'), addr('c@d.com')])
    plugin.hook_deferred(() => {}, hmail, { err: '421 Try later' })
    assert.strictEqual(published.length, 2)
    assert.strictEqual(published[1].e.recipientAddress, 'c@d.com')
  })

  it('publishes no events when rcpt_to is empty', () => {
    assert.doesNotThrow(() => plugin.hook_deferred(() => {}, makeHmail(), null))
    assert.strictEqual(published.length, 0)
  })

  it('handles null hmail gracefully', () => {
    assert.doesNotThrow(() => plugin.hook_deferred(() => {}, null, null))
    assert.strictEqual(published.length, 0)
  })
})

// ─── address extraction (queue-shaped todo) ──────────────────────────────────

// Regression: outbound todos are deserialized from the queue file as plain
// objects with `.original` but no `.address()` method. These must not throw and
// must yield the bare address (angle brackets stripped).
describe('address extraction from queue-shaped objects', () => {
  let published

  beforeEach(() => {
    published = []
    plugin._publish = (k, e) => published.push({ k, e })
    plugin.logdebug = () => {}
  })

  it('does not throw when mail_from/rcpt_to lack an .address() method', () => {
    const hmail = {
      todo: {
        notes: {},
        domain: 'recipient1.test',
        mail_from: { original: '<bounces+MJCHEduq.XXX.test=recipient1.test@brd.spf.dopasend.com>' },
        rcpt_to: [addr('test@recipient1.test')],
      },
    }
    assert.doesNotThrow(() => plugin.hook_delivered(() => {}, hmail, deliveredParams('test@recipient1.test')))
  })

  it('reports the real From header as senderAddress, not the VERP envelope', () => {
    const verp = 'bounces+MJCHEduq.XXX.test=recipient1.test@brd.spf.dopasend.com'
    const hmail = {
      todo: {
        notes: { amqp_from: 'no-reply@acme.test' },
        domain: 'recipient1.test',
        mail_from: { original: `<${verp}>` },
        rcpt_to: [],
      },
    }
    const ctx = plugin._extract_hmail_context(hmail)
    assert.strictEqual(ctx.senderAddress, 'no-reply@acme.test')
  })

  it('falls back to the envelope MAIL FROM when no From note was captured', () => {
    const verp = 'bounces+MJCHEduq.XXX.test=recipient1.test@brd.spf.dopasend.com'
    const hmail = {
      todo: { notes: {}, domain: 'recipient1.test', mail_from: { original: `<${verp}>` }, rcpt_to: [] },
    }
    const ctx = plugin._extract_hmail_context(hmail)
    assert.strictEqual(ctx.senderAddress, verp)
  })

  it('returns empty string for the null sender <>', () => {
    const hmail = { todo: { notes: {}, mail_from: { original: '<>' }, rcpt_to: [] } }
    assert.strictEqual(plugin._extract_hmail_context(hmail).senderAddress, '')
  })

  it('returns empty string when mail_from is absent', () => {
    assert.strictEqual(plugin._extract_hmail_context({ todo: { notes: {} } }).senderAddress, '')
  })
})

// ─── _publish ────────────────────────────────────────────────────────────────

describe('_publish', () => {
  it('does not throw when this.amqp is null', () => {
    plugin.amqp = null
    plugin.logdebug = () => {}
    assert.doesNotThrow(() => plugin._publish('outcome.delivered', { jobId: 'x' }))
  })

  it('queues the event when not connected', () => {
    plugin.amqp = null
    plugin.logdebug = () => {}
    plugin._publishQueue = []
    plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.strictEqual(plugin._publishQueue.length, 1)
    assert.strictEqual(plugin._publishQueue[0].routingKey, 'outcome.delivered')
  })

  it('drops event and warns when queue is full', () => {
    const warnings = []
    plugin.logwarn  = (m) => warnings.push(m)
    plugin.logdebug = () => {}
    plugin.amqp = null
    plugin.cfg = { publishing: { max_queue_size: 2, publish_timeout_ms: 5000 } }
    plugin._publishQueue = [{ routingKey: 'x', event: {} }, { routingKey: 'x', event: {} }]
    plugin._publish('outcome.delivered', { jobId: 'drop-me' })
    assert.strictEqual(plugin._publishQueue.length, 2)
    assert.ok(warnings.some((m) => m.includes('queue full')))
  })

  it('calls ch.publish with persistent and mandatory options', () => {
    const calls = []
    plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => { calls.push({ opts }); cb(null) } },
      exchange: 'mailing.delivery.outcomes',
    }
    plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.strictEqual(calls[0].opts.persistent, true)
    assert.strictEqual(calls[0].opts.mandatory, true)
  })

  it('publishes to the configured exchange', () => {
    const exchanges = []
    plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => { exchanges.push(ex); cb(null) } },
      exchange: 'mailing.delivery.outcomes',
    }
    plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.strictEqual(exchanges[0], 'mailing.delivery.outcomes')
  })

  it('serialises the event as JSON', () => {
    let captured
    plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => { captured = body; cb(null) } },
      exchange: 'x',
    }
    plugin._publish('outcome.delivered', { jobId: 'job-1', status: 'delivered' })
    assert.strictEqual(JSON.parse(captured.toString()).jobId, 'job-1')
  })

  it('logs error on nack callback', () => {
    const errors = []
    plugin.logerror = (m) => errors.push(m)
    plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => cb(new Error('nacked')) },
      exchange: 'x',
    }
    plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.ok(errors.some((m) => m.includes('nack')))
  })

  it('does not log on successful ack', () => {
    const errors = []
    plugin.logerror = (m) => errors.push(m)
    plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => cb(null) },
      exchange: 'x',
    }
    plugin._publish('outcome.delivered', { jobId: 'x' })
    assert.strictEqual(errors.length, 0)
  })

  it('logs error on publish timeout', async () => {
    const errors = []
    plugin.logerror = (m) => errors.push(m)
    plugin.PUBLISH_TIMEOUT = 10
    plugin.amqp = {
      ch: { publish: () => {} }, // never calls back
      exchange: 'x',
    }
    plugin._publish('outcome.delivered', { jobId: 'job-timeout' })
    await new Promise((r) => setTimeout(r, 50))
    assert.ok(errors.some((m) => m.includes('timeout')))
  })

  it('uses publish_timeout_ms from config when set', async () => {
    const errors = []
    plugin.logerror = (m) => errors.push(m)
    plugin.load_amqp_reporting_ini()
    plugin.cfg.publishing.publish_timeout_ms = 10
    plugin.amqp = {
      ch: { publish: () => {} }, // never calls back
      exchange: 'x',
    }
    plugin._publish('outcome.delivered', { jobId: 'cfg-timeout' })
    await new Promise((r) => setTimeout(r, 50))
    assert.ok(errors.some((m) => m.includes('timeout')))
  })

  it('does not double-log when callback settles before timer fires', async () => {
    const errors = []
    plugin.logerror = (m) => errors.push(m)
    plugin.PUBLISH_TIMEOUT = 20
    plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => cb(new Error('nacked')) },
      exchange: 'x',
    }
    plugin._publish('outcome.delivered', { jobId: 'x' })
    await new Promise((r) => setTimeout(r, 60))
    assert.strictEqual(errors.filter((m) => m.includes('nack')).length, 1)
    assert.strictEqual(errors.filter((m) => m.includes('timeout')).length, 0)
  })

  it('does not double-log when timer settles before callback fires', async () => {
    const errors = []
    plugin.logerror = (m) => errors.push(m)
    plugin.PUBLISH_TIMEOUT = 10
    let lateCb
    plugin.amqp = {
      ch: { publish: (ex, key, body, opts, cb) => { lateCb = cb } },
      exchange: 'x',
    }
    plugin._publish('outcome.delivered', { jobId: 'x' })
    await new Promise((r) => setTimeout(r, 50))
    assert.ok(errors.some((m) => m.includes('timeout')))
    lateCb(new Error('late nack'))
    assert.strictEqual(errors.filter((m) => m.includes('nack')).length, 0)
  })
})

// ─── _flushQueue ─────────────────────────────────────────────────────────────

describe('_flushQueue', () => {
  beforeEach(() => {
    plugin.logdebug = () => {}
  })

  it('does nothing when queue is empty', () => {
    plugin._publishQueue = []
    assert.doesNotThrow(() => plugin._flushQueue())
  })

  it('publishes all queued events', () => {
    const published = []
    plugin._publish = (k, e) => published.push({ k, e })
    plugin._publishQueue = [
      { routingKey: 'outcome.delivered', event: { jobId: 'a' } },
      { routingKey: 'outcome.bounced',   event: { jobId: 'b' } },
    ]
    plugin._flushQueue()
    assert.strictEqual(published.length, 2)
    assert.strictEqual(published[0].k, 'outcome.delivered')
    assert.strictEqual(published[1].k, 'outcome.bounced')
  })

  it('empties the queue after flushing', () => {
    plugin._publish = () => {}
    plugin._publishQueue = [{ routingKey: 'x', event: {} }]
    plugin._flushQueue()
    assert.strictEqual(plugin._publishQueue.length, 0)
  })
})

// ─── _reconnect ──────────────────────────────────────────────────────────────

describe('_reconnect', () => {
  beforeEach(() => {
    plugin.load_amqp_reporting_ini()
    plugin.logdebug = () => {}
    plugin.loginfo  = () => {}
    plugin.logerror = () => {}
    plugin._reconnectDelay = 1000
    plugin._shuttingDown = false
    plugin._connect_and_setup = () => {}
  })

  afterEach(() => {
    if (plugin._reconnectTimer) clearTimeout(plugin._reconnectTimer)
  })

  it('does nothing when shutting down', () => {
    plugin._shuttingDown = true
    plugin._reconnect()
    assert.strictEqual(plugin._reconnectTimer, undefined)
  })

  it('stores the timer handle', () => {
    plugin._reconnect()
    assert.ok(plugin._reconnectTimer)
  })

  it('doubles _reconnectDelay', () => {
    plugin._reconnectDelay = 1000
    plugin._reconnect()
    assert.strictEqual(plugin._reconnectDelay, 2000)
  })

  it('caps _reconnectDelay at max_reconnect_delay_ms', () => {
    plugin.cfg.connection.max_reconnect_delay_ms = 2000
    plugin._reconnectDelay = 2000
    plugin._reconnect()
    assert.strictEqual(plugin._reconnectDelay, 2000)
  })

  it('clears the timer handle when it fires', async () => {
    plugin.cfg.connection.max_reconnect_delay_ms = 30000
    plugin._reconnectDelay = 10
    plugin._reconnect()
    await new Promise((r) => setTimeout(r, 50))
    assert.strictEqual(plugin._reconnectTimer, null)
  })

  it('logs error and schedules retry when reconnect attempt fails', async () => {
    const errors = []
    plugin.logerror = (m) => errors.push(m)
    plugin._reconnectDelay = 10
    plugin._connect_and_setup = (onErr) => {
      plugin._shuttingDown = true  // prevent the retry from looping
      onErr(new Error('refused'))
    }
    plugin._reconnect()
    await new Promise((r) => setTimeout(r, 50))
    assert.ok(errors.some((m) => m.includes('reconnect failed')))
  })

  it('resets _reconnectDelay to 1000 on successful reconnect', async () => {
    plugin._reconnectDelay = 10
    plugin._connect_and_setup = (onErr, onReady) => onReady('test-exchange')
    plugin._reconnect()
    await new Promise((r) => setTimeout(r, 50))
    assert.strictEqual(plugin._reconnectDelay, 1000)
  })

  it('schedules retry when channel setup fails during reconnect', async () => {
    plugin._reconnectDelay = 10
    let retried = false
    plugin._connect_and_setup = (onErr, onReady, onSetupError) => {
      plugin._shuttingDown = true  // stop the next retry from looping
      retried = true
      onSetupError()
    }
    plugin._reconnect()
    await new Promise((r) => setTimeout(r, 50))
    assert.ok(retried)
  })

  it('uses 30000ms cap when max_reconnect_delay_ms is not configured', () => {
    plugin.cfg = {}
    plugin._reconnectDelay = 40000
    plugin._reconnect()
    assert.ok(plugin._reconnectDelay <= 30000)
  })
})

// ─── _parse_smtp_code ────────────────────────────────────────────────────────

describe('_parse_smtp_code', () => {
  it('extracts code from leading digits', () => {
    assert.strictEqual(plugin._parse_smtp_code('550 5.1.1 Nope', 0), 550)
  })

  it('extracts 250 from delivery response', () => {
    assert.strictEqual(plugin._parse_smtp_code('250 2.0.0 OK', 0), 250)
  })

  it('extracts 421 from deferral response', () => {
    assert.strictEqual(plugin._parse_smtp_code('421 4.7.0 Try again later', 0), 421)
  })

  it('returns fallback when message is empty string', () => {
    assert.strictEqual(plugin._parse_smtp_code('', 421), 421)
  })

  it('returns fallback when message is null', () => {
    assert.strictEqual(plugin._parse_smtp_code(null, 250), 250)
  })

  it('returns fallback when message has no leading digits', () => {
    assert.strictEqual(plugin._parse_smtp_code('connection timeout', 421), 421)
  })
})
