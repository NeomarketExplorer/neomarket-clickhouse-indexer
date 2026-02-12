# Neomarket ClickHouse Indexer

Subsquid + ClickHouse pipeline for Polymarket PnL and position tracking.

**Repo:** https://github.com/NeomarketExplorer/neomarket-clickhouse-indexer

## Status (Feb 9, 2026)

- **Deployed on Coolify** (Hetzner 138.201.57.139)
- **Indexer synced** -- processing live blocks (~82.7M+)
- **API live** on port 3002 -- 12 endpoints including 7 new frontend endpoints
- **Candles optimized** -- `candles_1m` AggregatingMergeTree + MV for <300ms OHLCV queries (was ~6s)
- **Metadata sync** running -- 27k+ markets from Gamma API
- **Uses viem** (not ethers) for all on-chain utils

## Services

| Service | Command | Port | Memory |
|---------|---------|------|--------|
| indexer | `npm run start` | -- | 4G |
| api | `npm run api` | 3002 | 2G |
| metadata-sync | `npm run sync:metadata -- --loop` | -- | 1G |
| snapshotter | `npm run snapshot:scheduler` | -- | 4G (optional, `worker` profile) |

## API Endpoints (`src/api.ts`)

### Core Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /positions?user=ADDRESS` | Open positions with market metadata |
| `GET /pnl/:wallet?startTs=&endTs=` | PnL delta between timestamps (requires build-ledger) |
| `GET /snapshots/:wallet?fromTs=&toTs=&limit=` | Historical PnL snapshots (requires build-ledger) |
| `GET /ledger/:wallet?fromTs=&toTs=&limit=` | Detailed trade ledger (requires build-ledger) |

### Frontend Endpoints (Feb 9, 2026)

| Endpoint | Description |
|----------|-------------|
| `GET /portfolio/history?user=&interval=&from=&to=` | Portfolio value snapshots by interval (requires build-ledger) |
| `GET /user/stats?user=` | User stats: trades, volume, win/loss, best/worst trade |
| `GET /activity?user=&limit=&offset=&type=&conditionId=` | Activity feed with type filter, conditionId filter, pagination |
| `GET /trades?tokenId=&limit=&offset=` | On-chain trade history per token |
| `GET /market/stats?conditionId=` or `?tokenId=` | Market analytics: traders, volume, holders |
| `GET /market/candles?conditionId=&tokenId=&interval=&from=&to=&limit=` | OHLCV candles for price charts |
| `GET /leaderboard?sort=&limit=&period=` | Trader rankings by **net cashflow**, **realized PnL** (ledger-backed), volume, or trades |
| `GET /leaderboard/explain?user=&period=&limit=&metric=` | Audit breakdown (with tx hashes) for `metric=netCashflow` or `metric=pnl` |

### Data Availability

- **Always available**: `/activity`, `/user/stats` (basic fields), `/trades`, `/market/stats`, `/market/candles`, `/leaderboard`, `/positions`
- **Requires build-ledger** (per-wallet batch): `/portfolio/history`, `/pnl`, `/snapshots`, `/ledger`, and nullable fields in `/user/stats`

### Response Conventions

- All frontend endpoints use **camelCase** field names
- Timestamps are **unix seconds** (numbers), not ISO strings
- Empty results return full structure with zero values / empty arrays (not 404)
- Nullable fields (`winRate`, `winCount`, `bestTrade`, etc.) return `null` when no ledger data
- `/leaderboard?sort=netCashflow` ranks by net cashflow (`sum(sell_usdc) - sum(buy_usdc)`)
- `/leaderboard?sort=pnl` ranks by realized PnL from `wallet_ledger` (coverage depends on ledger/snapshot jobs)
- Leaderboard rows now include both `netCashflowUsd` and `realizedPnlUsd` (when available)
- Legacy leaderboard field `totalPnl` is retained for compatibility and equals `netCashflowUsd`

## Environment Variables

Set in Coolify as **Runtime only** (not Build Time):

```env
CLICKHOUSE_URL=http://i4ow40sksocwgoc4wko8s0g0:8123
CLICKHOUSE_DATABASE=polymarket
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=<coolify-generated>
SQD_NETWORK_GATEWAY=https://v2.archive.subsquid.io/network/polygon-mainnet
RPC_ENDPOINT=<primary-polygon-rpc>
RPC_ENDPOINTS=https://polygon-rpc.com,https://polygon.drpc.org,https://polygon-bor-rpc.publicnode.com
RPC_HEALTHCHECK_TIMEOUT_MS=2500
RPC_RATE_LIMIT=10
RPC_CAPACITY=10
RPC_REQUEST_TIMEOUT_MS=30000
RPC_MAX_BATCH_CALL_SIZE=100
FINALITY_CONFIRMATION=75
HOT_BLOCKS_DEPTH=50
RPC_HEAD_POLL_INTERVAL_MS=1000
RPC_NEW_HEAD_TIMEOUT_MS=60000
PROCESSOR_ID=polymarket-pnl
START_BLOCK=4023686
PORT=3002
GAMMA_API_URL=https://gamma-api.polymarket.com
```

- `RPC_ENDPOINTS` is a comma-separated fallback list. The indexer probes endpoints at startup and picks the first healthy one.
- `run-with-restart.sh` rotates `RPC_ENDPOINT` across `RPC_ENDPOINTS` on each retry attempt.
- For stricter accuracy over speed, raise `FINALITY_CONFIRMATION` and `HOT_BLOCKS_DEPTH` (for example `128`).

## Docker Networking

- ClickHouse is a separate Coolify resource (container `i4ow40sksocwgoc4wko8s0g0`)
- All services connect via external network `subsquid_poli_default`

## Scripts

| Script | Purpose |
|--------|---------|
| `npm run start` | Run indexer |
| `./run-with-restart.sh` | Run indexer with auto-restart and RPC endpoint rotation |
| `npm run api` | HTTP API on :3002 |
| `npm run sync:metadata` | Sync market metadata from Gamma API |
| `npm run sync:metadata -- --loop` | Continuous sync every 5 min |
| `npm run ledger -- <wallet> 1d` | Build ledger for single wallet |
| `npm run backfill -- --wallets-file wallets.txt` | Batch ledger build |
| `npm run snapshot:scheduler` | Periodic snapshot updates |
| `npm run reconcile -- <wallet>` | Compare ledger vs on-chain balances |
| `npm run audit:leaderboard -- --local-base http://localhost:3002 --strict` | Compare local leaderboard vs official Polymarket leaderboard |

## Leaderboard Audit Agent

Run a parity check against official Polymarket APIs:

```bash
npm run audit:leaderboard -- --local-base http://localhost:3002 --local-period all --local-sort netCashflow --pm-timeframe ALL --pm-sort PNL --limit 100 --compare-top 50 --min-overlap 0.20 --timeout-ms 30000 --strict
```

What it checks:
- Upstream health: `data-api.polymarket.com`, `gamma-api.polymarket.com`, `clob.polymarket.com`
- Wallet overlap/rank drift between local `/leaderboard` and Polymarket official leaderboard
- JSON report output (use `--report-file /path/report.json`)
- Request timeout control (use `--timeout-ms`)

## Leaderboard MV Backfill (Existing Deployments)

`/leaderboard` now uses pre-aggregated tables:
- `wallet_leaderboard_stats_1h`
- `wallet_leaderboard_stats_all`

For an existing ClickHouse instance with historical data, run a one-time backfill:
1. Temporarily stop the indexer container to avoid overlap while backfilling.
2. Run the two `INSERT ... SELECT` statements below.
3. Start the indexer again.

```sql
INSERT INTO polymarket.wallet_leaderboard_stats_1h
SELECT
  toStartOfHour(block_timestamp) AS bucket,
  tupleElement(participant, 1) AS wallet,
  countState() AS trades_state,
  sumState(usdc_value) AS volume_state,
  sumState(if(tupleElement(participant, 2) = 'sell', usdc_value, -usdc_value)) AS pnl_state,
  uniqExactState(token_id) AS markets_state
FROM (
  SELECT
    block_timestamp,
    token_id,
    toFloat64(usdc_amount) / 1000000 AS usdc_value,
    arrayJoin([
      tuple(maker, if(is_maker_buy, 'buy', 'sell')),
      tuple(taker, if(is_taker_buy, 'buy', 'sell'))
    ]) AS participant
  FROM polymarket.trades
)
GROUP BY bucket, wallet;

INSERT INTO polymarket.wallet_leaderboard_stats_all
SELECT
  tupleElement(participant, 1) AS wallet,
  countState() AS trades_state,
  sumState(usdc_value) AS volume_state,
  sumState(if(tupleElement(participant, 2) = 'sell', usdc_value, -usdc_value)) AS pnl_state,
  uniqExactState(token_id) AS markets_state
FROM (
  SELECT
    token_id,
    toFloat64(usdc_amount) / 1000000 AS usdc_value,
    arrayJoin([
      tuple(maker, if(is_maker_buy, 'buy', 'sell')),
      tuple(taker, if(is_taker_buy, 'buy', 'sell'))
    ]) AS participant
  FROM polymarket.trades
)
GROUP BY wallet;
```

## Architecture

```
Polygon RPC --> Subsquid Processor --> ClickHouse
                                           |
                                    +------+------+
                                    |      |      |
                              API (:3002)  |  Metadata Sync
                                    |      |      |
                                    |      |   Gamma API
                                    v      v
                              Neomarket Frontend
```

## Tech Stack

- **viem** -- on-chain utils (encodePacked, keccak256)
- **@clickhouse/client** -- ClickHouse queries
- **@subsquid/evm-processor** -- Polygon block indexing
- **Gamma API** -- market metadata (questions, outcomes, token IDs)
