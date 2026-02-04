# ClickHouse Indexer — Review & Plan

## What Already Exists

There's a Subsquid-based indexer (`polymarket-pnl-indexer`) already running on Hetzner with ClickHouse. It has **all the raw on-chain data we need** — the schema is comprehensive and the backfill is done. The problem is that the derived data pipeline is broken, the API doesn't serve what the frontend needs, and there are security/ops issues.

### Current State (as of 2026-02-02)

| Component | Status |
|-----------|--------|
| ClickHouse `clickhouse/clickhouse-server:24.8` | Running, healthy, 2 days uptime |
| Raw event indexer (Subsquid processor) | Working — synced to block 82,427,615 |
| PnL indexer (`indexer-cs0oss000o8wo0c8ssswk0co`) | **Crash-looping** — restarts every ~60s |
| API server (`:3002`) | Running but **useless** — serves from empty tables |

### Data Volume

| Table | Rows | Disk |
|-------|------|------|
| `transfers` (ERC-1155) | 593M | 67 GB |
| `trades` (OrderFilled) | 440M | 118 GB |
| `splits` | 178M | 25 GB |
| `fee_refunds` | 60M | 9 GB |
| `redemptions` | 46M | 7 GB |
| `adapter_splits` | 39M | 6 GB |
| `merges` | 28M | 4 GB |
| `adapter_merges` | 15M | 2 GB |
| `adapter_redemptions` | 7M | 1 GB |
| `adapter_conversions` | 1.5M | 0.2 GB |
| `conditions` | 479K | 0.07 GB |
| `wallet_ledger` | **0** | 0 |
| `wallet_pnl_snapshots` | **0** | 0 |
| **Total** | | **~240 GB** |

Backfilled from block 4,023,686 (~Polymarket genesis) to present. Full history.

---

## Problems Found

### 1. PnL Indexer Crash-Looping (CRITICAL)

The `indexer` container restarts every ~60 seconds with:

```
RpcProtocolError: Response for a batch request should be an array
    at HttpConnection.batchCall
```

**Cause:** The RPC endpoint is `https://polygon-rpc.com` — a free public aggregator. It doesn't reliably support batch JSON-RPC requests. The Subsquid processor uses batch calls by default and crashes when the response isn't an array.

**Also:** On startup it logs:
```
Could not discover hot-supported tables from src/db/tables/hot-supported: ENOENT
```
The build is missing the `hot-supported` directory. This disables the hot/cold reorg handling.

**Result:** `wallet_ledger` and `wallet_pnl_snapshots` stay at 0 rows. The entire P&L pipeline never runs.

### 2. API Doesn't Serve Positions (CRITICAL)

The API server (`src/api.ts`) only has three endpoints:

```
GET /pnl/:wallet       → reads from wallet_pnl_snapshots (empty)
GET /snapshots/:wallet → reads from wallet_pnl_snapshots (empty)
GET /ledger/:wallet    → reads from wallet_ledger (empty)
```

There is **no `/positions` endpoint**. Even if the tables were populated, the API doesn't return what the frontend needs: current token balances with market metadata.

The frontend needs:
```
GET /positions?user=ADDRESS     → [{condition_id, outcome_index, size, avg_price, cur_price, pnl, ...}]
GET /activity?user=ADDRESS      → [{type, side, price, size, timestamp, tx_hash, ...}]
```

### 3. No Materialized View for Balances (PERFORMANCE)

To compute a user's current positions, you'd scan the full 593M-row `transfers` table:

```sql
SELECT token_id,
       sum(if(to = '0xUSER', toInt256(value), -toInt256(value))) as balance
FROM transfers
WHERE to = '0xUSER' OR from = '0xUSER'
GROUP BY token_id
HAVING balance > 0
```

This is slow without a materialized view. Need a `SummingMergeTree` MV that pre-aggregates per (wallet, token_id).

### 4. SQL Injection (SECURITY)

The API interpolates user input directly into SQL:

```typescript
// src/api.ts
`WHERE wallet = '${wallet.toLowerCase()}'`
```

A request to `/ledger/'; DROP TABLE trades; --` would execute arbitrary SQL. ClickHouse parameterized queries exist (`{wallet:String}`) and must be used.

### 5. ClickHouse Exposed to Internet with No Password (SECURITY)

```
Ports: 8123 (HTTP), 9000 (native) → bound to 0.0.0.0
CLICKHOUSE_PASSWORD=""
```

Anyone on the internet can connect to `138.201.57.139:8123` and read/write/drop all 240GB of data. This needs to be fixed immediately — either bind to localhost only, or set a password, or both.

### 6. No Resource Limits (OPS)

```
Memory limit: 0 (unlimited)
CPU limit: 0 (unlimited)
```

ClickHouse with 240GB of data on a 61GB RAM server can OOM-kill other services during large queries. Should set `max_memory_usage` in ClickHouse config and Docker memory limits.

### 7. Disk 66% Full (OPS)

270 GB used of 437 GB. ClickHouse alone is 240 GB. At current growth (Polymarket volume is increasing), disk fills within months. Need monitoring or a retention policy for old partitions.

---

## What Needs to Change

### Fix 1: Replace the RPC endpoint

Switch from `https://polygon-rpc.com` to a paid provider that supports batch requests reliably. Alchemy or QuickNode with Polygon archive access. Set the `RPC_ENDPOINT` env var in the indexer container.

This alone should stop the crash loop and let `wallet_ledger` start populating.

### Fix 2: Add a `user_balances` materialized view

```sql
CREATE TABLE polymarket.user_balances (
    wallet        LowCardinality(String),
    token_id      String,
    balance       Int256
) ENGINE = SummingMergeTree()
ORDER BY (wallet, token_id);

-- Populate from existing transfers data
INSERT INTO polymarket.user_balances
SELECT to AS wallet, token_id, toInt256(value) AS balance
FROM polymarket.transfers
WHERE to != '0x0000000000000000000000000000000000000000'
UNION ALL
SELECT `from` AS wallet, token_id, -toInt256(value) AS balance
FROM polymarket.transfers
WHERE `from` != '0x0000000000000000000000000000000000000000';

-- Then create a MV so new inserts auto-update
CREATE MATERIALIZED VIEW polymarket.user_balances_mv
TO polymarket.user_balances
AS
SELECT to AS wallet, token_id, toInt256(value) AS balance
FROM polymarket.transfers
WHERE to != '0x0000000000000000000000000000000000000000'
UNION ALL
SELECT `from` AS wallet, token_id, -toInt256(value) AS balance
FROM polymarket.transfers
WHERE `from` != '0x0000000000000000000000000000000000000000';
```

Query (instant, <10ms):
```sql
SELECT token_id, sum(balance) AS size
FROM polymarket.user_balances
WHERE wallet = {wallet:String}
GROUP BY token_id
HAVING size > 0
```

### Fix 3: Add `/positions` and `/activity` endpoints to the API

New endpoints the frontend needs:

**GET /positions?user=ADDRESS**

```json
[
  {
    "asset": "21742633143463906290569050155826241533067272736897614950488156847949938836455",
    "condition_id": "0xabc...",
    "outcome_index": 0,
    "size": 150.5,
    "avg_price": 0.62,
    "cur_price": 0.71,
    "current_value": 106.855,
    "pnl": 13.505,
    "pnl_percent": 14.5,
    "initial_value": 93.31
  }
]
```

Implementation:
1. Query `user_balances` for token_ids with balance > 0
2. Join with `conditions` table to get condition_id + outcome_index from token_id
3. Join with a market_metadata table (synced from Gamma) for question/slug
4. Calculate avg_price from `wallet_trades` view (already exists)
5. Get current price from CLOB midpoint or cache
6. Derive current_value, initial_value, pnl, pnl_percent

**GET /activity?user=ADDRESS&limit=50**

```json
[
  {
    "type": "trade",
    "timestamp": "2025-01-15T10:30:00Z",
    "condition_id": "0xabc...",
    "side": "BUY",
    "price": 0.62,
    "size": 50.0,
    "value": 31.0,
    "fee": 0.31,
    "transaction_hash": "0x..."
  }
]
```

Implementation: Query `wallet_trades` view (already exists) ordered by block_timestamp desc.

### Fix 4: Fix SQL injection

Replace all string interpolation with ClickHouse parameterized queries:

```typescript
// Before (vulnerable)
`WHERE wallet = '${wallet.toLowerCase()}'`

// After (safe)
const result = await client.query({
  query: 'SELECT * FROM wallet_ledger WHERE wallet = {wallet:String}',
  query_params: { wallet: wallet.toLowerCase() },
  format: 'JSONEachRow',
});
```

### Fix 5: Secure ClickHouse

Option A (minimal): Bind ports to localhost only, remove the public port bindings.
Option B (better): Set a password via `CLICKHOUSE_PASSWORD` env var and update all clients.
Option C (best): Both — localhost bind + password + firewall rules.

### Fix 6: Set resource limits

In the Docker config or ClickHouse `users.xml`:
```xml
<max_memory_usage>16000000000</max_memory_usage>  <!-- 16 GB cap -->
```

Docker: `--memory=20g` to prevent OOM-killing other containers.

### Fix 7: Add market metadata table

The `conditions` table has condition_id but no market questions/names. Add a lookup table synced from Gamma API:

```sql
CREATE TABLE IF NOT EXISTS polymarket.market_metadata (
    condition_id    String,
    market_id       String,
    question        String,
    slug            String,
    outcomes        Array(String),
    neg_risk        Bool,
    updated_at      DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY condition_id;
```

Sync from Gamma every 5 minutes. Then the `/positions` endpoint returns market names directly — no client-side enrichment needed.

---

## Frontend Integration

Once the API serves `/positions` and `/activity`, the frontend change is one line:

```typescript
// apps/web/src/hooks/use-positions.ts

// Before (Polymarket Data API via proxy)
const dataClient = createDataClient({ baseUrl: '/api/data' });

// After (own ClickHouse-backed API via existing indexer proxy)
const dataClient = createDataClient({ baseUrl: '/api/indexer' });
```

The existing `/api/indexer/[...path]/route.ts` proxy already points at `138.201.57.139:3005`. Either:
- Add the new endpoints to the existing indexer API on `:3005`, or
- Expose the PnL API on `:3002` and add a new proxy route

If the API returns market names in the response, remove `enrichPositionsWithMarketData()` from `use-positions.ts`.

---

## Implementation Order

### Phase 0: Emergency fixes (do first)

1. **Secure ClickHouse** — bind ports to localhost or set a password. 240GB of data is exposed.
2. **Fix RPC endpoint** — replace `https://polygon-rpc.com` with a paid provider. Stops the crash loop.
3. **Fix SQL injection** — use parameterized queries in `src/api.ts`.

### Phase 1: Positions endpoint (unblocks frontend)

4. Create `user_balances` SummingMergeTree table
5. Backfill from existing 593M transfers
6. Create materialized view for incremental updates
7. Add `market_metadata` table, sync from Gamma
8. Add `GET /positions?user=` endpoint to API
9. Frontend: swap baseUrl to use own indexer

This gives real-time position visibility. Avg price / P&L can show as "—" until Phase 2.

### Phase 2: P&L + Activity (once PnL indexer is running)

10. Fix the hot-supported tables build issue
11. PnL indexer populates `wallet_ledger` and `wallet_pnl_snapshots`
12. Add `GET /activity?user=` endpoint
13. Wire avg_price, pnl, pnl_percent into positions response
14. Frontend: remove Data API proxy dependency entirely

### Phase 3: Live prices + portfolio value

15. Cache CLOB midpoint prices (websocket or poll every 5s)
16. Return `cur_price` and `current_value` in positions
17. `GET /portfolio?user=` — aggregate totalValue, totalPnL
18. Optional: websocket push for live position value updates

---

## Architecture (current + target)

```
                          CURRENT                                    TARGET
                          ───────                                    ──────

Polygon RPC               polygon-rpc.com (free, broken)   →   Alchemy/QuickNode (paid, reliable)
    │                                                               │
    ▼                                                               ▼
Subsquid Processor        ✅ Working (raw events)                   Same
    │                     ❌ PnL indexer crash-looping        →     Fixed (reliable RPC)
    ▼                                                               │
ClickHouse                ✅ 240GB raw data                         │
                          ❌ 0 rows in wallet_ledger          →     Populated
                          ❌ No user_balances MV               →     Added
                          ❌ No market_metadata                →     Added (Gamma sync)
                          ❌ Open to internet, no password     →     Secured
    │                                                               │
    ▼                                                               ▼
API (:3002)               ❌ Only /pnl, /snapshots, /ledger   →    + /positions, /activity
                          ❌ All return empty                  →     Returns real data
                          ❌ SQL injection                     →     Parameterized queries
    │                                                               │
    ▼                                                               ▼
Frontend                  ❌ Uses Polymarket Data API (broken) →    Uses own indexer API
                          ❌ Client-side Gamma enrichment      →    Server-side (market_metadata)
```

---

## Token ID → Condition ID Mapping

The `transfers` table stores `token_id` (a uint256). The frontend needs `condition_id` + `outcome_index`. The relationship is:

```
token_id = uint256(keccak256(abi.encodePacked(conditionId, outcomeIndex)))
```

This is a one-way hash — you can't reverse it. Options:

1. **Build a lookup from `trades` table**: The `trades` table has both `token_id` and `maker_asset_id`/`taker_asset_id`. Cross-reference with the `conditions` table which has `condition_id` + `outcome_slot_count`.
2. **Index `PositionSplit` events**: The `splits` table has `condition_id` + `partition` (array of index sets). The index set encodes which outcome. Combined with the condition_id, you can derive the token_id and build a reverse map.
3. **Gamma API**: Gamma markets include `tokens[].token_id` and `condition_id`. Simplest source for the mapping. Store in `market_metadata`.

Recommendation: Use Gamma API (option 3) for the mapping table. It's the simplest and gives you question/slug/outcomes at the same time.

---

## Risks

- **Backfilling `user_balances` from 593M rows**: Will take time (minutes, not hours) and spike CPU/memory. Run during low traffic. Consider `INSERT ... SELECT` with `max_insert_threads=2` to avoid starving other queries.
- **Neg-risk token wrapping**: Transfers through the Neg Risk Adapter use different token IDs than what Gamma exposes. The adapter events are already indexed (`adapter_splits`, `adapter_merges`, etc.) — need to account for these when computing balances.
- **Disk growth**: 240GB on a 437GB disk. Set a retention policy or TTL on old partitions. Consider `ALTER TABLE ... DROP PARTITION` for data older than needed.
- **SummingMergeTree async merges**: Always query with `sum(balance) GROUP BY`, never raw `SELECT balance`. The MV is eventually consistent but correct when aggregated.
