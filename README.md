# KLine Server

This project provides a lightweight Express server that continuously ingests Huobi market data and exposes REST endpoints for latest quotes and historical kline candles. The implementation is designed for CentOS 7.6 environments running Node.js 16 with Python 2.7 available for building native dependencies such as `better-sqlite3`.

## Requirements

- Node.js 16
- Python 2.7 (required when building `better-sqlite3` from source)
- `gcc`, `make`, and other standard build tools for native modules

## Installation

```bash
npm install
```

## Running the server

```bash
npm start
```

By default the server listens on port `4000`. You can customise behaviour with the following environment variables:

- `KLINE_PORT`: override the listen port (default `4000`).
- `KLINE_POLL_INTERVAL_MS`: how often quotes are fetched from upstream in milliseconds (default `1000`).
- `KLINE_RETENTION_DAYS`: number of days of snapshot history to retain (default `7`).

Historical snapshots are stored in `data/kline.db` relative to the project root.
