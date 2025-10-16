'use strict'

const fs = require('fs')
const path = require('path')

const SAVE_DEBOUNCE_MS = 250

function createStorage(filePath) {
  return new JsonStorage(filePath)
}

class JsonStorage {
  constructor(filePath) {
    this.filePath = filePath
    this.tmpPath = filePath + '.tmp'
    this.snapshots = []
    this.quotes = new Map()
    this.latestSnapshotTs = new Map()
    this._saveTimer = null

    ensureDir(path.dirname(this.filePath))
    this.loadFromDisk()
  }

  loadFromDisk() {
    if (!fs.existsSync(this.filePath)) {
      return
    }

    try {
      const raw = fs.readFileSync(this.filePath, 'utf8')
      if (!raw) return
      const data = JSON.parse(raw)
      if (Array.isArray(data.snapshots)) {
        this.snapshots = data.snapshots
          .filter(isValidSnapshot)
          .map(normalizeSnapshot)
        this.snapshots.sort(function (a, b) {
          return a.timestamp - b.timestamp
        })
      }

      if (data.quotes && typeof data.quotes === 'object') {
        Object.keys(data.quotes).forEach(function (key) {
          const value = normalizeQuote(data.quotes[key])
          if (value) {
            this.quotes.set(key, value)
          }
        }, this)
      }

      this.rebuildSnapshotIndex()
    } catch (error) {
      console.error('Failed to load storage file, starting with empty store:', error)
      this.snapshots = []
      this.quotes.clear()
      this.latestSnapshotTs.clear()
    }
  }

  rebuildSnapshotIndex() {
    this.latestSnapshotTs.clear()
    this.snapshots.forEach(function (snapshot) {
      const prev = this.latestSnapshotTs.get(snapshot.symbol)
      if (!prev || snapshot.timestamp > prev) {
        this.latestSnapshotTs.set(snapshot.symbol, snapshot.timestamp)
      }
    }, this)
  }

  getAllQuotes() {
    return Array.from(this.quotes.values())
      .slice()
      .sort(function (a, b) {
        return a.symbol.localeCompare(b.symbol)
      })
      .map(cloneQuote)
  }

  getQuote(symbol) {
    const value = this.quotes.get(symbol)
    return value ? cloneQuote(value) : null
  }

  upsertQuote(quote) {
    const normalized = normalizeQuote(quote)
    if (!normalized) return false

    this.quotes.set(normalized.symbol, normalized)
    this.scheduleSave()
    return true
  }

  insertSnapshot(snapshot) {
    const normalized = normalizeSnapshot(snapshot)
    if (!normalized) return false

    this.snapshots.push(normalized)
    const lastTs = this.latestSnapshotTs.get(normalized.symbol)
    if (!lastTs || normalized.timestamp > lastTs) {
      this.latestSnapshotTs.set(normalized.symbol, normalized.timestamp)
    }

    this.scheduleSave()
    return true
  }

  getSnapshots(symbol, minTimestamp) {
    const threshold = Number(minTimestamp) || 0
    return this.snapshots
      .filter(function (snapshot) {
        return snapshot.symbol === symbol && snapshot.timestamp >= threshold
      })
      .sort(function (a, b) {
        return a.timestamp - b.timestamp
      })
      .map(cloneSnapshot)
  }

  deleteSnapshotsBefore(timestamp) {
    const cutoff = Number(timestamp) || 0
    if (!this.snapshots.length) return 0

    const nextSnapshots = []
    for (let i = 0; i < this.snapshots.length; i++) {
      const snapshot = this.snapshots[i]
      if (snapshot.timestamp >= cutoff) {
        nextSnapshots.push(snapshot)
      }
    }

    const removed = this.snapshots.length - nextSnapshots.length
    if (removed > 0) {
      this.snapshots = nextSnapshots
      this.rebuildSnapshotIndex()
      this.scheduleSave()
    }

    return removed
  }

  getLastSnapshotTimestamp(symbol) {
    return this.latestSnapshotTs.get(symbol) || null
  }

  flushSync() {
    if (this._saveTimer) {
      clearTimeout(this._saveTimer)
      this._saveTimer = null
    }
    this.saveToDisk()
  }

  scheduleSave() {
    if (this._saveTimer) {
      return
    }
    this._saveTimer = setTimeout(() => {
      this._saveTimer = null
      this.saveToDisk()
    }, SAVE_DEBOUNCE_MS)

    if (typeof this._saveTimer.unref === 'function') {
      this._saveTimer.unref()
    }
  }

  saveToDisk() {
    try {
      const payload = {
        snapshots: this.snapshots.map(cloneSnapshot),
        quotes: Object.fromEntries(
          Array.from(this.quotes.entries()).map(function (entry) {
            return [entry[0], cloneQuote(entry[1])]
          })
        )
      }
      const data = JSON.stringify(payload)
      fs.writeFileSync(this.tmpPath, data)
      fs.renameSync(this.tmpPath, this.filePath)
    } catch (error) {
      console.error('Failed to persist storage file:', error)
      try {
        if (fs.existsSync(this.tmpPath)) {
          fs.unlinkSync(this.tmpPath)
        }
      } catch (_) {
        // ignore cleanup errors
      }
    }
  }
}

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }
}

function isValidSnapshot(snapshot) {
  return snapshot && typeof snapshot.symbol === 'string' && Number.isFinite(Number(snapshot.timestamp))
}

function normalizeSnapshot(snapshot) {
  if (!snapshot) return null
  const symbol = snapshot.symbol && snapshot.symbol.toString().toLowerCase()
  const timestamp = Number(snapshot.timestamp)
  if (!symbol || !Number.isFinite(timestamp)) {
    return null
  }

  return {
    symbol: symbol,
    timestamp: timestamp,
    open: toNullableNumber(snapshot.open),
    close: toNullableNumber(snapshot.close),
    high: toNullableNumber(snapshot.high),
    low: toNullableNumber(snapshot.low),
    volume: toNullableNumber(snapshot.volume),
    createdAt: Number.isFinite(Number(snapshot.createdAt)) ? Number(snapshot.createdAt) : Date.now()
  }
}

function cloneSnapshot(snapshot) {
  return {
    symbol: snapshot.symbol,
    timestamp: snapshot.timestamp,
    open: snapshot.open,
    close: snapshot.close,
    high: snapshot.high,
    low: snapshot.low,
    volume: snapshot.volume,
    createdAt: snapshot.createdAt
  }
}

function normalizeQuote(value) {
  if (!value || !value.symbol) return null
  const symbol = value.symbol.toString().toLowerCase()
  return {
    symbol: symbol,
    type: typeof value.type === 'string' ? value.type : 'crypto',
    price: toNullableNumber(value.price),
    open: toNullableNumber(value.open),
    close: toNullableNumber(value.close),
    high: toNullableNumber(value.high),
    low: toNullableNumber(value.low),
    volume: toNullableNumber(value.volume),
    amount: toNullableNumber(value.amount),
    updateTime: Number.isFinite(Number(value.updateTime)) ? Number(value.updateTime) : Date.now()
  }
}

function cloneQuote(quote) {
  return {
    symbol: quote.symbol,
    type: quote.type,
    price: quote.price,
    open: quote.open,
    close: quote.close,
    high: quote.high,
    low: quote.low,
    volume: quote.volume,
    amount: quote.amount,
    updateTime: quote.updateTime
  }
}

function toNullableNumber(value) {
  if (value === null || value === undefined) return null
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

module.exports = {
  createStorage
}

