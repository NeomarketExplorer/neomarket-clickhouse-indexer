# ClickHouse Indexer -- Status

**Last updated:** Feb 9, 2026

## Current Status: ALL ENDPOINTS DEPLOYED

Deployed on Coolify (Hetzner 138.201.57.139). Indexer synced, API live with 12 endpoints, metadata sync active.

### Completed

| Task | Status |
|------|--------|
| SQL injection fixes (parameterized queries) | Done |
| `/positions` and `/activity` endpoints (enriched with market metadata) | Done |
| CORS support | Done |
| Address validation | Done |
| `user_balances` SummingMergeTree + MV | Done |
| `market_metadata` table + Gamma API sync | Done |
| Docker security (localhost ports, password, memory limits) | Done |
| Pushed to GitHub | Done |
| Connected to Coolify, deployed all services | Done |
| ClickHouse init schema (20 tables/views) | Done |
| Docker networking (external `subsquid_poli_default`) | Done |
| Replaced ethers with viem | Done |
| Removed pg dependency, metadata sync uses Gamma API directly | Done |
| Fix ledger engine: FINAL -> LIMIT 1 BY id (OOM fix) | Done |
| Fix token scale: toUsdcNumber (1e6) instead of toTokenNumber (1e18) | Done |
| Fix NegRisk transfer double-counting | Done |
| `/portfolio/history` endpoint | Done |
| `/user/stats` endpoint | Done |
| Enhanced `/activity` (type filter, conditionId, pagination) | Done |
| `/trades` endpoint (on-chain per token) | Done |
| `/market/stats` endpoint | Done |
| `/leaderboard` endpoint | Done |
| Fix price computation (value/amount instead of stored price_per_token) | Done |
| Filter contract addresses from leaderboard | Done |
| Return full structure for empty market/stats | Done |
| `/market/candles` OHLCV endpoint for price charts | Done |
| `/market/candles` performance: `candles_1m` AggregatingMergeTree + MV | Done |

### Repo

**https://github.com/NeomarketExplorer/neomarket-clickhouse-indexer**

---

## What's Running on Hetzner

| Container | Purpose | Port |
|-----------|---------|------|
| `i4ow40sksocwgoc4wko8s0g0` | ClickHouse database | 8123 (internal) |
| `indexer-o8g00ow48gkwo44o0s4wscok-*` | Subsquid block indexer | -- |
| `api-o8g00ow48gkwo44o0s4wscok-*` | API server | :3002 |
| `metadata-sync-o8g00ow48gkwo44o0s4wscok-*` | Gamma API metadata sync | -- |
| `web-*` | Neomarket frontend | :3000 |
| `indexer-tswcc4sko4sg8s00sgs8gwos` | Postgres-based indexer (events/markets) | :3005 |

---

## Future Improvements

1. **RPC stability** -- Replace `polygon-rpc.com` with `polygon.drpc.org` (current RPC returns garbage on rate limit, causing indexer crash loop)
2. **CLOB price caching** -- Periodically fetch midpoint prices for more accurate portfolio valuation
2. **Automatic build-ledger** -- Run build-ledger for active wallets automatically (currently manual per-wallet)
3. **Leaderboard materialized view** -- Pre-compute leaderboard data for faster queries on `all` period
4. **winRate in leaderboard** -- Requires wallet_ledger data per user
5. **Add `image` to market_metadata** -- Sync from Gamma API for inline market images
6. **Add blockNumber to activity** -- Requires updating wallet_trades SQL view
