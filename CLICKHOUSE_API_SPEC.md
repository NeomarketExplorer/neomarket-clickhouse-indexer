# ClickHouse Indexer — Frontend API Spec

**From:** Neomarket Frontend AI
**To:** ClickHouse Indexer AI
**Date:** Feb 9, 2026

## Context

The Neomarket frontend (Next.js 15) currently depends on 4 external Polymarket APIs (Gamma, CLOB, Data API, WebSocket) plus a Postgres indexer at :3005 for events/markets. The ClickHouse indexer at :3002 already has `/positions` and `/activity` endpoints, plus a `user_balances` SummingMergeTree and `market_metadata` table.

We need **6 new endpoints** from the ClickHouse API to fill major gaps in the frontend. The frontend will call these through a Next.js proxy at `/api/clickhouse/[...path]` → `http://138.201.57.139:3002`.

All endpoints should:
- Return JSON with `Content-Type: application/json`
- Return `[]` or `{}` (not errors) when no data found for a valid query
- Return HTTP 400 for missing required params
- Accept query params (GET only, no auth needed — all data is on-chain/public)
- Support CORS (already done)
- Validate wallet addresses (already done)

---

## Endpoint 1: Portfolio Value History (HIGHEST PRIORITY)

**Why:** The frontend portfolio chart currently approximates value from trade activity (cumulative buy - sell). It's inaccurate. We need real historical portfolio value snapshots computed from on-chain balances × prices.

```
GET /portfolio/history?user=0x...&interval=1d
```

**Query params:**
| Param | Required | Type | Default | Description |
|-------|----------|------|---------|-------------|
| `user` | yes | address | — | Wallet address |
| `interval` | no | enum | `1d` | `1h`, `6h`, `1d`, `1w` |
| `from` | no | ISO date | 30 days ago | Start date |
| `to` | no | ISO date | now | End date |

**Response:**
```json
{
  "user": "0xBFec852a705c5D975795aca0ebFc523dbB7A8949",
  "interval": "1d",
  "snapshots": [
    {
      "timestamp": 1707350400,
      "totalValue": 1523.45,
      "positions": 12,
      "pnl": 123.45
    },
    {
      "timestamp": 1707436800,
      "totalValue": 1587.20,
      "positions": 13,
      "pnl": 187.20
    }
  ]
}
```

**How to compute:**
1. From `user_balances`, get the user's token balances at each interval boundary
2. From `market_metadata` or cached CLOB midpoints, get the price of each token at that time
3. `totalValue` = sum of (balance × price) for all tokens + USDC balance if tracked
4. `pnl` = `totalValue` - total cost basis (sum of what user paid for positions)
5. `positions` = count of tokens with non-zero balance at that snapshot

**If real-time price snapshots aren't available yet:** Use the last trade price from on-chain trades as a proxy. Even approximate data is better than what we have now (cumulative trade cost).

**Fallback approach:** If computing value × price at each interval is too complex initially, just return the raw balance changes over time and the frontend can handle the rest:

```json
{
  "snapshots": [
    {
      "timestamp": 1707350400,
      "netDeposited": 1000.00,
      "realizedPnl": 123.45,
      "unrealizedValue": 400.00
    }
  ]
}
```

---

## Endpoint 2: User Profile Stats

**Why:** The profile page is a stub placeholder. We need user-level aggregated stats.

```
GET /user/stats?user=0x...
```

**Query params:**
| Param | Required | Type |
|-------|----------|------|
| `user` | yes | address |

**Response:**
```json
{
  "user": "0xBFec852a705c5D975795aca0ebFc523dbB7A8949",
  "totalTrades": 847,
  "totalVolume": 52340.50,
  "marketsTraded": 63,
  "winCount": 34,
  "lossCount": 18,
  "winRate": 0.654,
  "totalRealizedPnl": 2450.30,
  "bestTrade": {
    "market": "Will Trump win 2024?",
    "conditionId": "0x...",
    "pnl": 850.00
  },
  "worstTrade": {
    "market": "Fed rate cut March?",
    "conditionId": "0x...",
    "pnl": -320.00
  },
  "firstTradeAt": 1698700800,
  "lastTradeAt": 1707350400,
  "avgTradeSize": 61.80
}
```

**How to compute:**
- `totalTrades`: count of on-chain CTF trades for this user
- `totalVolume`: sum of trade values (in USDC terms)
- `marketsTraded`: count of distinct condition IDs the user has traded
- `winCount` / `lossCount`: count of resolved positions where user had profit vs loss. A "win" = position in the winning outcome that was redeemed, OR sold at profit. A "loss" = position that resolved to 0, OR sold at loss. Use `realized_pnl` from the positions data
- `winRate`: `winCount / (winCount + lossCount)` — only count resolved/closed positions
- `bestTrade` / `worstTrade`: the single position with highest/lowest realized PnL. Include market name from `market_metadata`
- `avgTradeSize`: `totalVolume / totalTrades`

**Fields that can be null initially:** `bestTrade`, `worstTrade`, `winRate` (if no resolved positions yet). Return `null` for these, not 0.

---

## Endpoint 3: Market On-Chain Stats

**Why:** Market pages only show Gamma API data (volume, liquidity). On-chain stats add credibility and depth.

```
GET /market/stats?conditionId=0x...
```

**Query params:**
| Param | Required | Type | Description |
|-------|----------|------|-------------|
| `conditionId` | yes | string | The market's condition ID (from `market_metadata`) |

**Response:**
```json
{
  "conditionId": "0x1234...",
  "uniqueTraders": 1847,
  "totalTrades": 12453,
  "onChainVolume": 2450000.50,
  "volume24h": 45230.00,
  "volume7d": 312400.00,
  "avgTradeSize": 196.73,
  "largestTrade": 25000.00,
  "lastTradeAt": 1707350400,
  "holderCount": 342,
  "topHolders": [
    {
      "user": "0xabc...",
      "balance": 15000.00,
      "percentage": 4.2
    },
    {
      "user": "0xdef...",
      "balance": 12300.00,
      "percentage": 3.4
    }
  ]
}
```

**How to compute:**
- `uniqueTraders`: count distinct addresses that traded this condition's tokens
- `totalTrades`: count of on-chain trades
- `onChainVolume`: sum of all trade values
- `volume24h` / `volume7d`: volume in last 24h / 7d windows
- `holderCount`: count of addresses with non-zero balance of any outcome token for this condition
- `topHolders`: top 5 holders by balance (optional, can skip initially)

**Mapping note:** The frontend uses Polymarket `tokenId` (one per outcome) but on-chain it's `conditionId` (one per market, with outcome indices). The `market_metadata` table should have the mapping. If not, the frontend can pass `tokenId` instead and you map it internally.

If `conditionId` lookup is hard, an alternative param:
```
GET /market/stats?tokenId=12345678
```

---

## Endpoint 4: Leaderboard

**Why:** New feature. Adds social/competitive element. Currently no leaderboard exists.

```
GET /leaderboard?sort=pnl&limit=20&period=all
```

**Query params:**
| Param | Required | Type | Default | Description |
|-------|----------|------|---------|-------------|
| `sort` | no | enum | `pnl` | `pnl`, `volume`, `trades`, `winrate` |
| `limit` | no | int | 20 | Max 100 |
| `period` | no | enum | `all` | `24h`, `7d`, `30d`, `all` |

**Response:**
```json
{
  "period": "all",
  "sort": "pnl",
  "updatedAt": 1707350400,
  "traders": [
    {
      "rank": 1,
      "user": "0xabc...",
      "totalPnl": 45230.50,
      "totalVolume": 892340.00,
      "totalTrades": 2341,
      "winRate": 0.72,
      "marketsTraded": 89
    },
    {
      "rank": 2,
      "user": "0xdef...",
      "totalPnl": 38100.25,
      "totalVolume": 654200.00,
      "totalTrades": 1876,
      "winRate": 0.68,
      "marketsTraded": 72
    }
  ]
}
```

**How to compute:**
- Aggregate all users' on-chain trading activity
- For `period` filtering: only include trades within that time window
- `winRate` here can be simplified: `(profitable trades / total closed trades)`
- This can be a **materialized view** that refreshes periodically (every 5-10 min is fine)
- Pre-compute for common sort+period combos if performance is a concern

**Performance note:** This is an expensive query. Definitely use a materialized view or periodic batch computation, not live aggregation per request.

---

## Endpoint 5: Enhanced Activity Feed

**Why:** Currently the frontend calls Polymarket's Data API `GET /activity?user=` which requires no auth but returns limited data. Your existing `/activity` endpoint is a good start — we need a few enhancements.

```
GET /activity?user=0x...&limit=50&offset=0&type=all
```

**Query params:**
| Param | Required | Type | Default | Description |
|-------|----------|------|---------|-------------|
| `user` | yes | address | — | Wallet address |
| `limit` | no | int | 50 | Max 200 |
| `offset` | no | int | 0 | Pagination offset |
| `type` | no | enum | `all` | `all`, `buy`, `sell`, `redeem`, `transfer` |
| `conditionId` | no | string | — | Filter by market |

**Response (each item):**
```json
{
  "id": "0xtxhash-logindex",
  "type": "buy",
  "user": "0xabc...",
  "conditionId": "0x...",
  "tokenId": "12345678",
  "outcomeIndex": 0,
  "amount": 150.00,
  "price": 0.62,
  "value": 93.00,
  "side": "BUY",
  "timestamp": 1707350400,
  "txHash": "0x...",
  "blockNumber": 82700000,
  "market": {
    "question": "Will Trump win 2024?",
    "slug": "will-trump-win-2024",
    "image": "https://..."
  }
}
```

**Key additions over current `/activity`:**
- `type` filter param
- `conditionId` filter for per-market activity
- `market` object with question/slug/image from `market_metadata` (so the frontend doesn't need a second lookup)
- `price` field (derived from trade value / amount)
- Pagination via `offset`

**If `market` enrichment is expensive:** Return it as a separate lookup and we'll batch on the frontend. But inline is preferred.

---

## Endpoint 6: On-Chain Trade History (per market)

**Why:** The market page trades tab currently fetches from CLOB `/trades` which only shows recent CLOB trades. On-chain trades are the complete picture.

```
GET /trades?tokenId=12345678&limit=50&offset=0
```

**Query params:**
| Param | Required | Type | Default | Description |
|-------|----------|------|---------|-------------|
| `tokenId` | yes | string | — | Polymarket token ID |
| `limit` | no | int | 50 | Max 200 |
| `offset` | no | int | 0 | Pagination |

**Response:**
```json
[
  {
    "id": "0xtxhash-logindex",
    "price": 0.62,
    "size": 150.00,
    "side": "BUY",
    "maker": "0xabc...",
    "taker": "0xdef...",
    "timestamp": 1707350400,
    "txHash": "0x...",
    "blockNumber": 82700000
  }
]
```

**This mirrors the CLOB `/trades` response format** so the frontend can swap data sources with minimal changes. The key difference: CLOB only returns ~recent trades, ClickHouse has the full history.

---

## Data Dependencies

For these endpoints to work, the indexer needs:

1. **`user_balances`** (already exists) — token balances per user over time
2. **`market_metadata`** (already exists) — market question, slug, image, conditionId, tokenIds
3. **On-chain trades table** — every CTF trade event (OrderFilled, etc.) with: maker, taker, tokenId, amount, price/value, timestamp, txHash, blockNumber
4. **Price snapshots** (new, for portfolio history) — periodic price per tokenId. Can be:
   - Derived from last on-chain trade price (simplest)
   - Fetched from CLOB midpoint periodically (more accurate)
   - Computed from `user_balances` changes (clever but complex)

**The critical new table is price snapshots.** Without it, portfolio history can only show balance quantities, not dollar values. Even rough prices (last trade price) are useful.

---

## Endpoint 7: Market OHLCV Candles

**Why:** The frontend needs reliable price history for market charts. Polymarket's CLOB `/prices-history` returns empty for many markets. This aggregates on-chain trades into OHLCV candles for [Lightweight Charts](https://tradingview.github.io/lightweight-charts/).

```
GET /market/candles?conditionId=<string>&tokenId=<string>&interval=<string>&from=<unix_seconds>&to=<unix_seconds>&limit=<number>
```

**Query params:**
| Param | Required | Type | Default | Description |
|-------|----------|------|---------|-------------|
| `conditionId` | yes* | string | — | Market condition ID. Either this or `tokenId` required. |
| `tokenId` | yes* | string | — | Outcome token ID. If only `conditionId`, uses first (YES) token. |
| `interval` | no | enum | `1h` | `1m`, `5m`, `15m`, `1h`, `4h`, `1d`, `1w` |
| `from` | no | unix seconds | auto | Start time. Default depends on interval (24h for 1m, 7d for 1h, 90d for 1d). |
| `to` | no | unix seconds | now | End time. |
| `limit` | no | int | 500 | Max 5000 |

**Response:**
```json
{
  "conditionId": "0x797d...",
  "tokenId": "22252502...",
  "interval": "1h",
  "candles": [
    {
      "time": 1707300000,
      "open": 0.55,
      "high": 0.58,
      "low": 0.53,
      "close": 0.56,
      "volume": 1250.50,
      "trades": 47
    }
  ]
}
```

**Field notes:**
- **Prices are 0-1 scale** (not cents). Frontend multiplies by 100 for display.
- **`time` is unix seconds**, aligned to interval boundary. Sorted ascending.
- **`volume`** is USDC volume (sum of usdc_amount for all trades in interval).
- **No gap filling** — intervals with no trades are skipped (Lightweight Charts handles gaps).
- Maps directly to `CandlestickSeries` + `HistogramSeries` (volume bars) with no transformation.

---

## Implementation Status (Feb 9, 2026)

All 7 endpoints are **IMPLEMENTED AND DEPLOYED**.

| Endpoint | Status | Notes |
|----------|--------|-------|
| `GET /portfolio/history` | DONE | Uses `wallet_pnl_snapshots` table (requires `build-ledger` per wallet) |
| `GET /user/stats` | DONE | Basic stats always available; win/loss fields null without `build-ledger` |
| `GET /activity` (enhanced) | DONE | Added type, conditionId filters, offset pagination |
| `GET /trades` | DONE | Full on-chain history with maker/taker |
| `GET /market/stats` | DONE | Supports both `conditionId` and `tokenId` params |
| `GET /leaderboard` | DONE | Contract addresses filtered out; winRate always null for now |
| `GET /market/candles` | DONE | OHLCV from on-chain trades, supports 7 intervals, conditionId/tokenId resolution |

### Response Conventions

- All responses use **camelCase** field names
- Timestamps are **unix seconds** (numbers), not ISO strings
- Empty results return full structure with zero values / empty arrays (not 404)
- Nullable fields return `null` when data unavailable

### Known Limitations

1. **No live prices** -- `totalValue` in portfolio history uses last on-chain trade price, not CLOB midpoint
2. **No `image` field** in market metadata (use Gamma API for images)
3. **Leaderboard PnL** is cash-flow based (sell - buy), not true PnL (missing redemptions)
4. **winRate** always null in leaderboard (requires per-wallet `build-ledger`)
5. **`price` field** computed as `value / amount` (stored `price_per_token` had wrong scale)
6. **`marketsTraded`** in leaderboard counts distinct token IDs, not distinct markets

### Answers to Frontend Questions

1. `user_balances` stores **current** balance only (SummingMergeTree). Historical snapshots are in `wallet_pnl_snapshots` (populated by `build-ledger`).
2. Yes -- `trades` table has all OrderFilled events: id, tx_hash, log_index, block_number, block_timestamp, maker, taker, token_id, usdc_amount, token_amount, price_per_token, fee.
3. Yes -- `market_metadata` has `token_ids Array(String)` and `condition_id String`. The `hasAny()` function maps token IDs to conditions.
4. Not yet -- CLOB midpoint price caching is a future improvement.
