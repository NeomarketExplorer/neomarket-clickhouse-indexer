# Polymarket Subsquid Indexer

This folder contains the Subsquid + ClickHouse pipeline used to build wallet-level PnL.

Quickstart

1) `npm install`
2) Copy `.env.example` to `.env` and set:
   - `CLICKHOUSE_*`, `SQD_NETWORK_GATEWAY`, `RPC_ENDPOINT`, `START_BLOCK`
3) `npm run db:reset`
4) `npm run start` (indexer; run until synced)
5) Build ledger + snapshots:
   - `npm run ledger -- <wallet> 1d`
   - or `npm run backfill -- --wallets-file wallets.txt --interval 1d`

Useful scripts

- `npm run pnl -- <wallet> <mode> [startTs] [endTs]`
- `npm run pnl:snapshots -- <wallet> <startTs> <endTs>`
- `npm run reconcile -- <wallet> --rpc <url>`
- `npm run api` (HTTP API on port 3002)

Production notes

- Production runs via Coolify (Docker Compose service) on Debian 12.
- ClickHouse is managed as a separate Coolify service; the app uses `CLICKHOUSE_URL=http://clickhouse:8123`.
- A reference Compose file for Coolify is in `docker-compose.coolify.yml` (indexer + api + optional snapshot worker).

More details: `SIMPLE_POLI/SUBSQUID_POLI_NOTES.md`
