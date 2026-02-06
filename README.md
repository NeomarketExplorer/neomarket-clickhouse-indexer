# Neomarket ClickHouse Indexer

Subsquid + ClickHouse pipeline for Polymarket PnL and position tracking.

**Repo:** https://github.com/NeomarketExplorer/neomarket-clickhouse-indexer

## Status (Feb 2026)

- **Old Hetzner setup nuked** — containers and 249GB volume removed
- **Code ready** — SQL injection fixed, new endpoints added, Docker hardened
- **Needs deployment** — connect repo to Coolify + get paid RPC

## What's Implemented

### API Endpoints (`src/api.ts`)
| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /positions?user=ADDRESS` | User's open positions with avg buy price |
| `GET /activity?user=ADDRESS&limit=50` | Recent trade activity |
| `GET /pnl/:wallet?startTs=&endTs=` | PnL delta between timestamps |
| `GET /snapshots/:wallet?fromTs=&toTs=&limit=` | Historical PnL snapshots |
| `GET /ledger/:wallet?fromTs=&toTs=&limit=` | Detailed trade ledger |

### Security Fixes
- All queries use ClickHouse parameterized queries (no SQL injection)
- Wallet address validation (returns 400 for invalid)
- CORS headers for browser access
- Docker: localhost-only ports, password, memory limits

### Schema Additions (`clickhouse/init/001_create_tables.sql`)
- `user_balances` — SummingMergeTree for position tracking
- `user_balances_mv` — Materialized view auto-populating from transfers
- `market_metadata` — For Gamma API sync (token→market mapping)

## Quickstart (Local Dev)

```bash
npm install
cp .env.example .env  # Edit with your values
docker-compose up -d  # Starts ClickHouse
npm run start         # Run indexer
npm run api           # API on :3002
```

## Environment Variables

```env
# ClickHouse
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_DATABASE=polymarket
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=neomarket_ch_2024

# Subsquid
SQD_NETWORK_GATEWAY=https://v2.archive.subsquid.io/network/polygon-mainnet

# RPC (NEEDS PAID PROVIDER)
RPC_ENDPOINT=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY

# Indexer
PROCESSOR_ID=polymarket-pnl
START_BLOCK=4023686
```

## Deployment (Coolify)

### Next Steps for Handoff

1. **Get paid RPC** — Alchemy or QuickNode (free tier may work)
   - Need HTTP endpoint for Polygon mainnet
   - ~12-48 hours to reindex 78M blocks

2. **Connect to Coolify:**
   - Add GitHub repo: `NeomarketExplorer/neomarket-clickhouse-indexer`
   - Use `docker-compose.coolify.yml`
   - Set env vars in Coolify UI (don't commit secrets)

3. **Deploy services:**
   - ClickHouse (separate service, persistent volume)
   - API (port 3002)
   - Indexer (connects to RPC + ClickHouse)

4. **After reindex completes:**
   - Test `/positions?user=0x...` and `/activity?user=0x...`
   - Wire up frontend to call these endpoints

## Scripts

| Script | Purpose |
|--------|---------|
| `npm run start` | Run indexer |
| `npm run api` | HTTP API on :3002 |
| `npm run ledger -- <wallet> 1d` | Build ledger for single wallet |
| `npm run backfill -- --wallets-file wallets.txt` | Batch ledger build |
| `npm run snapshot:scheduler` | Periodic snapshot updates |

## Architecture

```
Polygon RPC ──→ Subsquid Processor ──→ ClickHouse
                                           │
                                           ↓
                                     API (:3002)
                                           │
                                           ↓
                                   Neomarket Frontend
```
