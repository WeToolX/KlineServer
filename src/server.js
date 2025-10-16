'use strict'

const express = require('express')
const cors = require('cors')
const Database = require('better-sqlite3')
const fs = require('fs')
const path = require('path')

let fetchFn = typeof globalThis.fetch === 'function' ? globalThis.fetch : null
if (!fetchFn) {
  try {
    const fetchModule = require('node-fetch')
    fetchFn = typeof fetchModule === 'function' ? fetchModule : fetchModule.default
  } catch (error) {
    console.error('The optional dependency "node-fetch" is required on Node.js 16 environments.')
    throw error
  }
}

if (typeof fetchFn !== 'function') {
  throw new Error('A compatible fetch implementation could not be resolved.')
}

const CRYPTO_SYMBOLS = ['btcusdt', 'ethusdt', 'xrpusdt', 'ltcusdt', 'eosusdt', 'bchusdt']

const POLL_INTERVAL_MS = Number(process.env.KLINE_POLL_INTERVAL_MS || 1000)
const DB_DIR = path.resolve(__dirname, '..', 'data')
const DB_PATH = path.join(DB_DIR, 'kline.db')
const parsedRetention = Number(process.env.KLINE_RETENTION_DAYS || 7)
const RETENTION_DAYS = Number.isFinite(parsedRetention) && parsedRetention > 0 ? parsedRetention : 7
const RETENTION_MS = RETENTION_DAYS * 24 * 60 * 60 * 1000
const CLEAN_INTERVAL_MS = 60 * 60 * 1000

if (!fs.existsSync(DB_DIR)) {
  fs.mkdirSync(DB_DIR, { recursive: true })
}

const db = new Database(DB_PATH)

db.exec(`
  CREATE TABLE IF NOT EXISTS kline_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    open REAL,
    close REAL,
    high REAL,
    low REAL,
    volume REAL,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_kline_symbol_time ON kline_snapshots (symbol, timestamp_ms);

  CREATE TABLE IF NOT EXISTS latest_quotes (
    symbol TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    price REAL,
    open REAL,
    close REAL,
    high REAL,
    low REAL,
    volume REAL,
    amount REAL,
    update_time INTEGER NOT NULL
  );
`)

const insertSnapshotStmt = db.prepare(`
  INSERT INTO kline_snapshots
    (symbol, timestamp_ms, open, close, high, low, volume, created_at)
  VALUES
    (@symbol, @timestamp, @open, @close, @high, @low, @volume, @createdAt)
`)

const upsertQuoteStmt = db.prepare(`
  INSERT INTO latest_quotes
    (symbol, type, price, open, close, high, low, volume, amount, update_time)
  VALUES
    (@symbol, @type, @price, @open, @close, @high, @low, @volume, @amount, @updateTime)
  ON CONFLICT(symbol) DO UPDATE SET
    type = excluded.type,
    price = excluded.price,
    open = excluded.open,
    close = excluded.close,
    high = excluded.high,
    low = excluded.low,
    volume = excluded.volume,
    amount = excluded.amount,
    update_time = excluded.update_time
`)

const selectLatestQuotesStmt = db.prepare(`
  SELECT symbol, type, price, open, close, high, low, volume, amount, update_time
  FROM latest_quotes
  ORDER BY symbol ASC
`)

const selectLatestQuoteStmt = db.prepare(`
  SELECT symbol, type, price, open, close, high, low, volume, amount, update_time
  FROM latest_quotes
  WHERE symbol = ?
`)

const selectSnapshotsStmt = db.prepare(`
  SELECT symbol, timestamp_ms AS timestamp, open, close, high, low, volume
  FROM kline_snapshots
  WHERE symbol = ? AND timestamp_ms >= ?
  ORDER BY timestamp_ms ASC
`)

const deleteOlderThanStmt = db.prepare(`
  DELETE FROM kline_snapshots WHERE timestamp_ms < ?
`)

const app = express()
app.use(cors())
app.use(express.json())

app.get('/health', function (_req, res) {
  res.json({ status: 'ok' })
})

app.get('/api/market/quotes', function (_req, res) {
  try {
    const rows = selectLatestQuotesStmt.all()
    res.json(rows.map(mapQuoteRow))
  } catch (error) {
    console.error('Failed to query latest quotes:', error)
    res.status(500).json({ error: 'failed to query latest quotes' })
  }
})

app.get('/api/market/quote', function (req, res) {
  const symbol = (req.query.symbol || '').toString().toLowerCase()
  if (!symbol) {
    return res.status(400).json({ error: 'symbol is required' })
  }

  try {
    const row = selectLatestQuoteStmt.get(symbol)
    if (!row) {
      return res.status(404).json({ error: 'quote not found' })
    }
    res.json(mapQuoteRow(row))
  } catch (error) {
    console.error('Failed to query quote:', error)
    res.status(500).json({ error: 'failed to query quote' })
  }
})

app.post('/api/market/kline/snapshot', function (_req, res) {
  res.status(410).json({ error: 'snapshot ingestion is handled automatically by the server' })
})

app.get('/api/market/kline', function (req, res) {
  const symbol = (req.query.symbol || '').toString().toLowerCase()
  if (!symbol) {
    return res.status(400).json({ error: 'symbol is required' })
  }

  const intervalMinutes = parseInterval(req.query.interval || '5m')
  const intervalMs = intervalMinutes * 60 * 1000
  const limit = clamp(Number(req.query.limit) || 200, 50, 500)
  const lookbackMs = intervalMs * limit * 2
  const minTimestamp = Date.now() - lookbackMs

  try {
    const rows = selectSnapshotsStmt.all(symbol, minTimestamp)
    if (!rows.length) {
      return res.json({ symbol: symbol, interval: intervalMinutes + 'm', candles: [] })
    }

    const candles = groupCandles(rows, intervalMs).slice(-limit)
    res.json({ symbol: symbol, interval: intervalMinutes + 'm', candles: candles })
  } catch (error) {
    console.error('Failed to query kline:', error)
    res.status(500).json({ error: 'failed to query kline data' })
  }
})

setInterval(function () {
  const cutoff = Date.now() - RETENTION_MS
  try {
    const info = deleteOlderThanStmt.run(cutoff)
    if (info.changes > 0) {
      console.log('[KLine] Cleaned ' + info.changes + ' rows older than ' + new Date(cutoff).toISOString())
    }
  } catch (error) {
    console.error('Failed to clean old snapshots:', error)
  }
}, CLEAN_INTERVAL_MS)

const jobConfigs = CRYPTO_SYMBOLS.map(function (symbol) {
  return { type: 'crypto', symbol: symbol }
})

let isPolling = false
const lastSnapshotMap = new Map()

function pollQuotes() {
  if (isPolling) return
  isPolling = true

  const tasks = jobConfigs.map(function (job) {
    return fetchQuoteFromUpstream(job.type, job.symbol)
      .then(function (data) {
        return { status: 'fulfilled', job: job, data: data }
      })
      .catch(function (error) {
        return { status: 'rejected', job: job, error: error }
      })
  })

  Promise.all(tasks)
    .then(function (results) {
      results.forEach(function (result) {
        if (result.status === 'fulfilled') {
          if (!result.data) {
            console.warn('[KLine] No data returned for ' + result.job.symbol + ', skipping')
            return
          }
          const stored = storeQuote(result.data)
          if (!stored) {
            console.warn('[KLine] Skipped storing quote for ' + result.job.symbol)
          }
        } else {
          const message = result.error && result.error.message ? result.error.message : result.error
          console.warn('[KLine] Failed to fetch ' + result.job.symbol + ':', message)
        }
      })
    })
    .catch(function (error) {
      console.error('Unexpected polling error:', error)
    })
    .then(function () {
      isPolling = false
    })
}

pollQuotes()
setInterval(pollQuotes, POLL_INTERVAL_MS)

const PORT = Number(process.env.KLINE_PORT || 4000)
app.listen(PORT, function () {
  console.log('📈 KLine server running on http://localhost:' + PORT)
})

function fetchQuoteFromUpstream(_type, symbol) {
  return fetchCryptoQuote(symbol)
}

function fetchCryptoQuote(symbol) {
  const url = 'https://api.huobi.pro/market/detail/merged?symbol=' + encodeURIComponent(symbol)
  return fetchFn(url, { headers: { 'Cache-Control': 'no-cache' } }).then(function (response) {
    if (!response.ok) {
      throw new Error('failed to fetch crypto ' + symbol)
    }
    return response.json()
  }).then(function (payload) {
    const tick = payload && payload.tick
    if (!tick || typeof tick.close !== 'number') {
      return null
    }

    return {
      type: 'crypto',
      symbol: symbol,
      price: tick.close,
      open: tick.open,
      close: tick.close,
      high: tick.high,
      low: tick.low,
      volume: tick.amount != null ? tick.amount : tick.vol != null ? tick.vol : null,
      amount: tick.vol != null ? tick.vol : null,
      updateTime: payload && payload.ts ? payload.ts : Date.now()
    }
  })
}

function storeQuote(raw) {
  if (!raw || !raw.symbol) return false

  const now = Date.now()
  const normalized = {
    symbol: raw.symbol.toLowerCase(),
    type: raw.type || 'crypto',
    price: toSafeNumber(raw.price),
    open: toSafeNumber(raw.open),
    close: toSafeNumber(raw.close != null ? raw.close : raw.price),
    high: toSafeNumber(raw.high),
    low: toSafeNumber(raw.low),
    volume: toSafeNumber(raw.volume),
    amount: toSafeNumber(
      raw.amount != null
        ? raw.amount
        : Number.isFinite(Number(raw.price)) && Number.isFinite(Number(raw.volume))
          ? Number(raw.price) * Number(raw.volume)
          : null
    ),
    updateTime: Math.trunc(toSafeNumber(raw.updateTime) || now)
  }

  try {
    upsertQuoteStmt.run(normalized)
  } catch (error) {
    console.error('Failed to upsert quote:', error)
    return false
  }

  const lastTs = lastSnapshotMap.get(normalized.symbol)
  if (normalized.updateTime !== lastTs) {
    try {
      insertSnapshotStmt.run({
        symbol: normalized.symbol,
        timestamp: normalized.updateTime,
        open: normalized.open,
        close: normalized.close,
        high: normalized.high,
        low: normalized.low,
        volume: normalized.volume,
        createdAt: now
      })
      lastSnapshotMap.set(normalized.symbol, normalized.updateTime)
    } catch (error) {
      console.error('Failed to insert snapshot:', error)
    }
  }

  return true
}

function mapQuoteRow(row) {
  return {
    type: row.type,
    symbol: row.symbol,
    price: toSafeNumber(row.price),
    open: toSafeNumber(row.open),
    close: toSafeNumber(row.close),
    high: toSafeNumber(row.high),
    low: toSafeNumber(row.low),
    volume: toSafeNumber(row.volume),
    amount: toSafeNumber(row.amount),
    updateTime: row.update_time
  }
}

function toSafeNumber(value) {
  if (value === null || value === undefined) return null
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function clamp(value, min, max) {
  if (Number.isNaN(value)) return min
  return Math.min(Math.max(value, min), max)
}

function parseInterval(raw) {
  if (!raw) return 5
  if (typeof raw === 'number' && Number.isFinite(raw)) return raw
  const text = raw.toString().trim().toLowerCase()
  const match = text.match(/^(\d+)([mhd])$/)
  if (!match) {
    const fallback = Number(text)
    return Number.isFinite(fallback) ? fallback : 5
  }
  const value = Number(match[1])
  const unit = match[2]
  switch (unit) {
    case 'h':
      return value * 60
    case 'd':
      return value * 60 * 24
    default:
      return value
  }
}

function groupCandles(rows, intervalMs) {
  const buckets = new Map()

  rows.forEach(function (row) {
    if (!Number.isFinite(row.timestamp)) return
    const bucketKey = Math.floor(row.timestamp / intervalMs) * intervalMs
    const safeOpen = row.open != null ? row.open : row.close != null ? row.close : 0
    const safeClose = row.close != null ? row.close : row.open != null ? row.open : safeOpen
    const safeHigh = row.high != null ? row.high : Math.max(safeOpen, safeClose)
    const safeLow = row.low != null ? row.low : Math.min(safeOpen, safeClose)
    const safeVolume = row.volume != null ? row.volume : 0

    const bucket = buckets.get(bucketKey)

    const base = {
      time: bucketKey,
      open: safeOpen,
      close: safeClose,
      high: safeHigh,
      low: safeLow,
      volume: safeVolume,
      firstTimestamp: row.timestamp,
      lastTimestamp: row.timestamp
    }

    if (!bucket) {
      buckets.set(bucketKey, base)
      return
    }

    if (row.timestamp < bucket.firstTimestamp) {
      bucket.open = safeOpen
      bucket.firstTimestamp = row.timestamp
    }

    if (row.timestamp > bucket.lastTimestamp) {
      bucket.close = safeClose
      bucket.lastTimestamp = row.timestamp
    }

    bucket.high = Math.max(bucket.high, safeHigh)
    bucket.low = Math.min(bucket.low, safeLow)

    bucket.volume += safeVolume
  })

  return Array.from(buckets.entries())
    .sort(function (a, b) {
      return a[0] - b[0]
    })
    .map(function (entry) {
      const item = entry[1]
      return {
        time: item.time,
        open: round(item.open),
        close: round(item.close),
        high: round(item.high),
        low: round(item.low),
        volume: round(item.volume, 6)
      }
    })
}

function round(value, digits) {
  if (!Number.isFinite(value)) return null
  var precision = typeof digits === 'number' ? digits : 4
  var factor = Math.pow(10, precision)
  return Math.round(value * factor) / factor
}

module.exports = app
