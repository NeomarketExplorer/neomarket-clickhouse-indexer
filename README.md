# Neomarket ClickHouse Indexer

Subsquid + ClickHouse pipeline for Polymarket PnL and position tracking.

**Repo:** https://github.com/NeomarketExplorer/neomarket-clickhouse-indexer

## Status (Feb 9, 2026)

- **Deployed on Coolify** (Hetzner 138.201.57.139)
- **Indexer synced** -- processing live blocks (~82.7M+)
- **API live** on port 3002 -- 12 endpoints including 7 new frontend endpoints
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
| `GET /leaderboard?sort=&limit=&period=` | Trader rankings by PnL, volume, or trades |

### Data Availability

- **Always available**: `/activity`, `/user/stats` (basic fields), `/trades`, `/market/stats`, `/market/candles`, `/leaderboard`, `/positions`
- **Requires build-ledger** (per-wallet batch): `/portfolio/history`, `/pnl`, `/snapshots`, `/ledger`, and nullable fields in `/user/stats`

### Response Conventions

- All frontend endpoints use **camelCase** field names
- Timestamps are **unix seconds** (numbers), not ISO strings
- Empty results return full structure with zero values / empty arrays (not 404)
- Nullable fields (`winRate`, `winCount`, `bestTrade`, etc.) return `null` when no ledger data

## Environment Variables

Set in Coolify as **Runtime only** (not Build Time):

```env
CLICKHOUSE_URL=http://i4ow40sksocwgoc4wko8s0g0:8123
CLICKHOUSE_DATABASE=polymarket
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=<coolify-generated>
SQD_NETWORK_GATEWAY=https://v2.archive.subsquid.io/network/polygon-mainnet
RPC_ENDPOINT=<your-polygon-rpc>
PROCESSOR_ID=polymarket-pnl
START_BLOCK=4023686
PORT=3002
GAMMA_API_URL=https://gamma-api.polymarket.com
```

## Docker Networking

- ClickHouse is a separate Coolify resource (container `i4ow40sksocwgoc4wko8s0g0`)
- All services connect via external network `subsquid_poli_default`

## Scripts

| Script | Purpose |
|--------|---------|
| `npm run start` | Run indexer |
| `npm run api` | HTTP API on :3002 |
| `npm run sync:metadata` | Sync market metadata from Gamma API |
| `npm run sync:metadata -- --loop` | Continuous sync every 5 min |
| `npm run ledger -- <wallet> 1d` | Build ledger for single wallet |
| `npm run backfill -- --wallets-file wallets.txt` | Batch ledger build |
| `npm run snapshot:scheduler` | Periodic snapshot updates |
| `npm run reconcile -- <wallet>` | Compare ledger vs on-chain balances |

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
