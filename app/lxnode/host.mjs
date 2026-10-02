/**
 * fnmusic-flow · lx-music 自定义源宿主（Node）
 * ==========================================
 *
 * 作用：让「lx-music 自定义源脚本」（.js）在服务端原样运行，不再需要用户桌面端
 * 开着 lx-music。脚本按 lx-music 官方文档约定的 globalThis.lx API 与宿主通信，
 * 本宿主完整实现这套 API：
 *
 *   lx.version / lx.env / lx.currentScriptInfo
 *   lx.EVENT_NAMES = { request, inited, updateAlert }
 *   lx.on(eventName, handler)      —— 注册回调（request 回调必须返回 Promise）
 *   lx.send(eventName, data)       —— 脚本向宿主上报（inited / updateAlert）
 *   lx.request(url, options, cb)   —— 不受跨域限制的 HTTP 请求，cb(err, resp, body)
 *   lx.utils.buffer / crypto / zlib
 *
 * 与 lx-music 客户端的行为对齐点：
 *   - resp.body：返回体是 JSON 时解析为对象，否则为字符串；同时提供 resp.raw 原始文本，
 *     并给解析出的对象挂一个隐藏的 toString（返回原始文本），兼容 JSON.parse(body) 写法。
 *   - options.body 传对象时自动 JSON 序列化并补 Content-Type；form / formData / timeout /
 *     follow_max / headers 均支持。
 *   - 初始化必须发送 inited 事件，未发送视为初始化失败；发送前抛错同样视为失败。
 *
 * 对 Python 侧暴露的本地 HTTP 接口（默认只监听 127.0.0.1）：
 *   GET    /                      宿主自描述
 *   GET    /health                宿主与已加载脚本的状态
 *   POST   /script                { id, hash, name, code }  加载 / 更新脚本（同 id 同 hash 幂等）
 *   DELETE /script?id=xxx         卸载脚本
 *   POST   /resolve               { id, source, songId, quality, musicInfo } -> { ok, url }
 *
 * 只用 Node 标准库，无第三方依赖。
 */

import http from 'node:http'
import https from 'node:https'
import vm from 'node:vm'
import crypto from 'node:crypto'
import zlib from 'node:zlib'
import { Buffer } from 'node:buffer'

const BIND = process.env.LX_HOST_BIND || '127.0.0.1'
const PORT = Number(process.env.LX_HOST_PORT || 18432)
const DEFAULT_TIMEOUT = Number(process.env.LX_HOST_TIMEOUT || 20000)
const LOAD_TIMEOUT = Number(process.env.LX_HOST_LOAD_TIMEOUT || 8000)
const RESOLVE_TIMEOUT = Number(process.env.LX_HOST_RESOLVE_TIMEOUT || 25000)
const MAX_BODY = 4 * 1024 * 1024

const EVENT_NAMES = Object.freeze({
  request: 'request',
  inited: 'inited',
  updateAlert: 'updateAlert',
})

/** 已加载脚本：id -> record */
const scripts = new Map()

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function safeJson(value, limit = 400) {
  try {
    const text = typeof value === 'string' ? value : JSON.stringify(value)
    if (text == null) return String(value)
    return text.length > limit ? `${text.slice(0, limit)}…` : text
  } catch {
    return String(value)
  }
}

/* ------------------------------------------------------------------ HTTP 客户端 */

function headerGet(headers, name) {
  const target = name.toLowerCase()
  for (const key of Object.keys(headers)) {
    if (key.toLowerCase() === target) return headers[key]
  }
  return undefined
}

function headerDel(headers, name) {
  const target = name.toLowerCase()
  for (const key of Object.keys(headers)) {
    if (key.toLowerCase() === target) delete headers[key]
  }
}

function buildPayload(options, headers) {
  if (options.body != null) {
    if (Buffer.isBuffer(options.body)) return options.body
    if (typeof options.body === 'string') return Buffer.from(options.body, 'utf8')
    if (!headerGet(headers, 'content-type')) headers['Content-Type'] = 'application/json'
    return Buffer.from(JSON.stringify(options.body), 'utf8')
  }
  if (options.form) {
    const params = new URLSearchParams()
    for (const [key, value] of Object.entries(options.form)) {
      if (value != null) params.append(key, String(value))
    }
    if (!headerGet(headers, 'content-type')) {
      headers['Content-Type'] = 'application/x-www-form-urlencoded'
    }
    return Buffer.from(params.toString(), 'utf8')
  }
  if (options.formData) {
    const boundary = `----lxhost${crypto.randomBytes(12).toString('hex')}`
    const chunks = []
    for (const [key, value] of Object.entries(options.formData)) {
      if (value == null) continue
      chunks.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${key}"\r\n\r\n`, 'utf8'))
      chunks.push(Buffer.isBuffer(value) ? value : Buffer.from(String(value), 'utf8'))
      chunks.push(Buffer.from('\r\n', 'utf8'))
    }
    chunks.push(Buffer.from(`--${boundary}--\r\n`, 'utf8'))
    if (!headerGet(headers, 'content-type')) {
      headers['Content-Type'] = `multipart/form-data; boundary=${boundary}`
    }
    return Buffer.concat(chunks)
  }
  return null
}

function decompress(buffer, encoding) {
  const value = String(encoding || '').toLowerCase()
  try {
    if (value.includes('gzip')) return zlib.gunzipSync(buffer)
    if (value.includes('br')) return zlib.brotliDecompressSync(buffer)
    if (value.includes('deflate')) return zlib.inflateSync(buffer)
  } catch {
    /* 压缩头与实际不符时按原样返回 */
  }
  return buffer
}

/** 给 JSON 对象挂一个隐藏的 toString，让 JSON.parse(body) 这类老写法也能工作。 */
function attachRawToString(value, raw) {
  if (!value || typeof value !== 'object') return value
  try {
    Object.defineProperty(value, 'toString', {
      value: () => raw,
      enumerable: false,
      configurable: true,
      writable: true,
    })
  } catch {
    /* 冻结对象等场景忽略 */
  }
  return value
}

function parseBody(text, contentType) {
  const trimmed = text.trim()
  if (!trimmed) return text
  const looksJson = /json/i.test(String(contentType || '')) || /^[[{]/.test(trimmed)
  if (!looksJson) return text
  try {
    return attachRawToString(JSON.parse(trimmed), text)
  } catch {
    return text
  }
}

/**
 * lx.request 的实现：返回一个「取消请求」的函数。
 * callback(err, resp, body) —— resp = { statusCode, statusMessage, headers, body, raw }
 */
function lxRequest(url, options, callback) {
  const opt = options || {}
  const cb = typeof callback === 'function' ? callback : () => {}
  const method = String(opt.method || 'GET').toUpperCase()
  const headers = { ...(opt.headers || {}) }
  const followMax = Number.isFinite(Number(opt.follow_max)) ? Number(opt.follow_max) : 5
  const timeout = Number(opt.timeout) > 0 ? Number(opt.timeout) : DEFAULT_TIMEOUT

  let settled = false
  let current = null

  const done = (err, resp, body) => {
    if (settled) return
    settled = true
    cb(err, resp, body)
  }

  let payload = null
  try {
    payload = buildPayload(opt, headers)
  } catch (err) {
    setTimeout(() => done(err), 0)
    return () => {}
  }
  headerDel(headers, 'content-length')

  const send = (target, redirectsLeft) => {
    if (settled) return
    let parsed
    try {
      parsed = new URL(String(target))
    } catch {
      done(new Error(`无效的请求地址：${safeJson(target, 120)}`))
      return
    }
    if (!/^https?:$/.test(parsed.protocol)) {
      done(new Error('仅支持 http / https 协议'))
      return
    }
    const transport = parsed.protocol === 'https:' ? https : http
    const requestHeaders = { ...headers }
    if (payload) requestHeaders['Content-Length'] = String(payload.length)

    const req = transport.request(parsed, { method, headers: requestHeaders, timeout }, (res) => {
      const status = res.statusCode || 0
      const location = res.headers.location
      if (location && redirectsLeft > 0 && [301, 302, 303, 307, 308].includes(status)) {
        res.resume()
        let nextTarget
        try {
          nextTarget = new URL(location, parsed).toString()
        } catch {
          done(new Error(`重定向地址无效：${safeJson(location, 120)}`))
          return
        }
        send(nextTarget, redirectsLeft - 1)
        return
      }
      const chunks = []
      res.on('data', (chunk) => chunks.push(chunk))
      res.on('end', () => {
        const raw = decompress(Buffer.concat(chunks), res.headers['content-encoding']).toString('utf8')
        const body = parseBody(raw, res.headers['content-type'])
        const resp = {
          statusCode: status,
          statusMessage: res.statusMessage || '',
          headers: res.headers,
          body,
          raw,
        }
        done(null, resp, body)
      })
      res.on('error', (err) => done(err))
    })
    current = req
    req.on('timeout', () => req.destroy(new Error('请求超时')))
    req.on('error', (err) => done(err))
    if (payload) req.write(payload)
    req.end()
  }

  send(url, followMax)
  return () => {
    if (settled) return
    settled = true
    try {
      current?.destroy(new Error('请求已取消'))
    } catch {
      /* ignore */
    }
  }
}

/* ------------------------------------------------------------------ lx 工具方法 */

const utils = {
  buffer: {
    from: (...args) => Buffer.from(...args),
    bufToString: (buffer, format) => Buffer.from(buffer).toString(format),
  },
  crypto: {
    md5: (value) => crypto.createHash('md5').update(value == null ? '' : String(value)).digest('hex'),
    randomBytes: (size) => crypto.randomBytes(Number(size) || 0).toString('hex'),
    aesEncrypt: (buffer, mode, key, iv) => {
      const cipher = crypto.createCipheriv(mode, key, iv)
      const input = Buffer.isBuffer(buffer) ? buffer : Buffer.from(String(buffer), 'utf8')
      return Buffer.concat([cipher.update(input), cipher.final()]).toString('base64')
    },
    rsaEncrypt: (buffer, key) => {
      const input = Buffer.isBuffer(buffer) ? buffer : Buffer.from(String(buffer), 'utf8')
      return crypto.publicEncrypt(key, input).toString('base64')
    },
  },
  zlib: {
    inflate: (buffer) => new Promise((resolve, reject) => {
      zlib.inflate(Buffer.from(buffer), (err, out) => (err ? reject(err) : resolve(out)))
    }),
    deflate: (buffer) => new Promise((resolve, reject) => {
      zlib.deflate(Buffer.from(buffer), (err, out) => (err ? reject(err) : resolve(out)))
    }),
  },
}

/** 解析脚本头部注释里的 @name / @version / @author / @homepage / @description */
function parseMeta(code) {
  const pick = (key) => {
    const match = new RegExp(`@${key}\\s+([^\\r\\n*]+)`).exec(code)
    return match ? match[1].trim() : undefined
  }
  return {
    name: pick('name'),
    version: pick('version'),
    author: pick('author'),
    homepage: pick('homepage'),
    description: pick('description'),
  }
}

function sanitizeSources(raw) {
  const sources = {}
  if (!raw || typeof raw !== 'object') return sources
  for (const [key, value] of Object.entries(raw)) {
    if (!value || typeof value !== 'object') continue
    const qualitys = Array.isArray(value.qualitys)
      ? value.qualitys.filter((item) => typeof item === 'string')
      : []
    sources[key] = {
      name: typeof value.name === 'string' ? value.name : key,
      type: typeof value.type === 'string' ? value.type : 'music',
      actions: Array.isArray(value.actions) ? value.actions.filter((item) => typeof item === 'string') : [],
      qualitys,
    }
  }
  return sources
}

/* ------------------------------------------------------------------ 脚本装载 */

function createConsole(record) {
  const push = (level, args) => {
    const line = args.map((item) => (typeof item === 'string' ? item : safeJson(item, 300))).join(' ')
    record.logs.push(`[${level}] ${line}`)
    if (record.logs.length > 200) record.logs.splice(0, record.logs.length - 200)
  }
  return {
    log: (...args) => push('log', args),
    info: (...args) => push('info', args),
    warn: (...args) => push('warn', args),
    error: (...args) => push('error', args),
    debug: (...args) => push('debug', args),
    trace: (...args) => push('trace', args),
    dir: (...args) => push('dir', args),
    table: (...args) => push('table', args),
    group: (...args) => push('group', args),
    groupCollapsed: (...args) => push('group', args),
    groupEnd: () => {},
    time: () => {},
    timeEnd: () => {},
    assert: () => {},
  }
}

function buildContext(record, meta) {
  const context = vm.createContext(
    {
      console: createConsole(record),
      setTimeout,
      clearTimeout,
      setInterval,
      clearInterval,
      queueMicrotask,
      URL,
      URLSearchParams,
      TextEncoder,
      TextDecoder,
      Buffer,
      atob,
      btoa,
    },
    { name: `lx:${record.name}` },
  )
  const lx = {
    version: '2.0.0',
    env: 'desktop',
    EVENT_NAMES,
    currentScriptInfo: {
      name: meta.name || record.name,
      description: meta.description,
      version: meta.version,
      author: meta.author,
      homepage: meta.homepage,
      rawScript: record.code,
    },
    on(eventName, handler) {
      if (typeof handler === 'function') record.handlers.set(String(eventName), handler)
    },
    send(eventName, data) {
      if (eventName === EVENT_NAMES.inited) {
        record.inited = data && typeof data === 'object' ? data : {}
        record.sources = sanitizeSources(record.inited.sources)
        record.ready = record.inited.status !== false
        if (record.inited.openDevTools && !record.devToolsLogged) {
          record.devToolsLogged = true
          record.logs.push('[host] 脚本请求打开 DevTools（服务端忽略）')
        }
      } else if (eventName === EVENT_NAMES.updateAlert) {
        record.updateAlert = {
          log: data?.log ? String(data.log).slice(0, 1024) : '',
          updateUrl: data?.updateUrl ? String(data.updateUrl).slice(0, 1024) : '',
        }
        record.logs.push(`[update] ${safeJson(record.updateAlert.log, 200)}`)
      }
    },
    request: (url, options, callback) => lxRequest(url, options, callback),
    utils,
  }
  context.lx = lx
  return context
}

async function loadScript({ id, hash, name, code }) {
  const record = {
    id: String(id),
    name: String(name || ''),
    code: String(code || ''),
    hash: String(hash || ''),
    meta: {},
    ready: false,
    error: null,
    inited: null,
    updateAlert: null,
    sources: {},
    logs: [],
    handlers: new Map(),
    loadedAt: new Date().toISOString(),
  }
  await unloadScript(record.id)

  const meta = parseMeta(record.code)
  record.meta = meta
  if (!record.name) record.name = meta.name || record.id
  record.displayName = meta.name || record.name
  record.version = meta.version || 'unknown'

  try {
    const context = buildContext(record, meta)
    const script = new vm.Script(record.code, {
      filename: `${record.id}.js`,
      displayErrors: true,
    })
    // 同步执行，超时保护：脚本死循环不会拖住宿主
    script.runInContext(context, { timeout: 15000 })
  } catch (err) {
    record.error = `脚本执行失败：${err && err.message ? err.message : String(err)}`
    scripts.set(record.id, record)
    return record
  }

  // 等待 inited 事件（部分脚本在异步逻辑里才发送）
  const deadline = Date.now() + LOAD_TIMEOUT
  while (!record.inited && !record.error && Date.now() < deadline) {
    await sleep(50)
  }
  if (!record.inited && !record.error) {
    record.error = `脚本未在 ${Math.round(LOAD_TIMEOUT / 1000)} 秒内发送 inited 事件，可能是不兼容的脚本`
  }
  if (record.inited && !Object.keys(record.sources).length) {
    record.error = '脚本初始化成功，但未声明任何可用音乐源'
    record.ready = false
  }
  scripts.set(record.id, record)
  return record
}

async function unloadScript(id) {
  const existed = scripts.delete(String(id))
  return existed
}

function summarize(record, withLogs = false) {
  const summary = {
    id: record.id,
    name: record.displayName || record.name || record.id,
    version: record.version || 'unknown',
    author: record.meta?.author || null,
    homepage: record.meta?.homepage || null,
    description: record.meta?.description || null,
    ready: Boolean(record.ready) && !record.error,
    error: record.error,
    sources: record.sources,
    platforms: Object.keys(record.sources || {}),
    update: record.updateAlert,
    loadedAt: record.loadedAt,
  }
  if (withLogs) summary.logs = record.logs.slice(-30)
  return summary
}

function buildMusicInfo(source, songId, extra) {
  const id = String(songId ?? '')
  const base = {
    id,
    songmid: id,
    songId: id,
    hash: id,
    mid: id,
    source,
  }
  if (extra && typeof extra === 'object') {
    for (const [key, value] of Object.entries(extra)) {
      if (value != null && value !== '') base[key] = value
    }
  }
  return base
}

async function resolveWith(record, { source, songId, quality, musicInfo }) {
  const handler = record.handlers.get(EVENT_NAMES.request)
  if (!handler) throw new Error('该脚本未注册 request 事件，无法解析播放地址')
  if (record.error) throw new Error(record.error)
  if (!record.inited) throw new Error('脚本尚未初始化完成')

  const info = {
    type: quality || '320k',
    musicInfo: musicInfo && typeof musicInfo === 'object' ? musicInfo : buildMusicInfo(source, songId),
  }
  let timer = null
  let result
  try {
    result = await Promise.race([
      Promise.resolve().then(() => handler({ source, action: 'musicUrl', info })),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('解析超时')), RESOLVE_TIMEOUT)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }

  let url = null
  if (typeof result === 'string') url = result
  else if (Buffer.isBuffer(result)) url = result.toString('utf8')
  else if (result && typeof result === 'object') {
    url = result.url || (result.data && typeof result.data === 'object' ? result.data.url : null)
  }
  if (!url || typeof url !== 'string' || !url.trim()) {
    throw new Error('脚本未返回有效的播放地址')
  }
  return url.trim()
}

/* ------------------------------------------------------------------ HTTP 服务 */

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    req.on('data', (chunk) => {
      size += chunk.length
      if (size > MAX_BODY) {
        reject(new Error('请求体过大'))
        req.destroy()
        return
      }
      chunks.push(chunk)
    })
    req.on('end', () => {
      const text = Buffer.concat(chunks).toString('utf8').trim()
      if (!text) return resolve({})
      try {
        resolve(JSON.parse(text))
      } catch (err) {
        reject(new Error('请求体不是有效 JSON'))
      }
    })
    req.on('error', reject)
  })
}

function sendJson(res, status, payload) {
  const body = Buffer.from(JSON.stringify(payload, null, 2), 'utf8')
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': String(body.length),
    'Cache-Control': 'no-store',
  })
  res.end(body)
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || '127.0.0.1'}`)
  const route = url.pathname.replace(/\/+$/, '') || '/'

  try {
    if (req.method === 'GET' && route === '/') {
      return sendJson(res, 200, {
        service: 'fnmusic-flow lx-music 自定义源宿主',
        version: 1,
        node: process.version,
        event: Object.keys(EVENT_NAMES),
        scripts: [...scripts.values()].map((record) => summarize(record)),
      })
    }

    if (req.method === 'GET' && route === '/health') {
      const rows = [...scripts.values()].map((record) => summarize(record, true))
      return sendJson(res, 200, {
        ok: true,
        node: process.version,
        count: rows.length,
        ready: rows.filter((row) => row.ready).length,
        scripts: rows,
      })
    }

    if (req.method === 'POST' && route === '/script') {
      const payload = await readJson(req)
      const id = String(payload.id || '').trim()
      if (!id) return sendJson(res, 200, { ok: false, error: '缺少脚本 id' })
      if (!String(payload.code || '').trim()) return sendJson(res, 200, { ok: false, error: '缺少脚本内容' })

      const existing = scripts.get(id)
      if (existing && payload.hash && existing.hash === String(payload.hash) && existing.ready && !existing.error) {
        return sendJson(res, 200, { ok: true, reused: true, script: summarize(existing), logs: existing.logs.slice(-30) })
      }
      const record = await loadScript({
        id,
        hash: payload.hash,
        name: payload.name,
        code: payload.code,
      })
      return sendJson(res, 200, {
        ok: !record.error && record.ready,
        error: record.error,
        reused: false,
        script: summarize(record),
        logs: record.logs.slice(-30),
      })
    }

    if (req.method === 'DELETE' && (route === '/script' || route.startsWith('/script/'))) {
      const id = route.startsWith('/script/')
        ? decodeURIComponent(route.slice('/script/'.length))
        : String(url.searchParams.get('id') || '')
      if (!id) return sendJson(res, 200, { ok: false, error: '缺少脚本 id' })
      return sendJson(res, 200, { ok: true, removed: await unloadScript(id) })
    }

    if (req.method === 'POST' && route === '/resolve') {
      const payload = await readJson(req)
      const id = String(payload.id || '').trim()
      const record = scripts.get(id)
      if (!record) return sendJson(res, 200, { ok: false, error: '脚本未加载到宿主', code: 'not_loaded' })
      if (record.error) return sendJson(res, 200, { ok: false, error: record.error, code: 'init_failed' })
      if (!record.ready) return sendJson(res, 200, { ok: false, error: '脚本尚未就绪', code: 'not_ready' })
      const source = String(payload.source || 'wy')
      if (!record.sources[source]) {
        return sendJson(res, 200, {
          ok: false,
          code: 'unsupported_source',
          error: `脚本不支持该平台（${source}），它声明的是：${Object.keys(record.sources).join('、') || '无'}`,
        })
      }
      try {
        const value = await resolveWith(record, {
          source,
          songId: payload.songId,
          quality: payload.quality,
          musicInfo: payload.musicInfo,
        })
        return sendJson(res, 200, { ok: true, url: value, source, quality: payload.quality || '320k' })
      } catch (err) {
        const message = err && err.message ? err.message : String(err)
        record.logs.push(`[resolve-error] ${message}`)
        return sendJson(res, 200, { ok: false, error: message, code: 'resolve_failed' })
      }
    }

    return sendJson(res, 404, { ok: false, error: `未知路由：${req.method} ${route}` })
  } catch (err) {
    return sendJson(res, 500, { ok: false, error: err && err.message ? err.message : String(err) })
  }
})

process.on('unhandledRejection', (err) => {
  console.error('[lx-host] 未处理的 Promise 异常：', err && err.message ? err.message : err)
})
process.on('uncaughtException', (err) => {
  console.error('[lx-host] 未捕获异常：', err && err.stack ? err.stack : err)
})

server.listen(PORT, BIND, () => {
  console.log(`[lx-host] lx-music 自定义源宿主已启动：http://${BIND}:${PORT}（node ${process.version}）`)
})

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    server.close(() => process.exit(0))
    setTimeout(() => process.exit(0), 1500).unref()
  })
}
