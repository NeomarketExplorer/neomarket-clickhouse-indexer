/**
 * Build/refresh daily realized-PnL rollup for a single wallet.
 *
 * This is intentionally NOT a materialized view because wallet_ledger is rebuilt
 * via DELETE+INSERT (idempotent per wallet), which would otherwise double-count.
 *
 * Usage:
 *   tsx src/build-rollup-1d.ts <wallet> [startTs] [endTs]
 *
 * Examples:
 *   tsx src/build-rollup-1d.ts 0xabc...
 *   tsx src/build-rollup-1d.ts 0xabc... 1735689600 1767139200
 */

import { createClient } from '@clickhouse/client'
import 'dotenv/config'

function isValidAddress(addr: string): boolean {
  return /^0x[a-fA-F0-9]{40}$/.test(addr)
}

function usage(): never {
  // eslint-disable-next-line no-console
  console.log(`
Usage: tsx src/build-rollup-1d.ts <wallet> [startTs] [endTs]

Defaults:
  startTs: now - 90 days
  endTs:   now
`)
  process.exit(1)
}

const ch = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  request_timeout: 300_000,
})

async function main() {
  const walletArg = process.argv[2]
  if (!walletArg || !isValidAddress(walletArg)) usage()
  const wallet = walletArg.toLowerCase()

  const now = Math.floor(Date.now() / 1000)
  const startTs = process.argv[3] ? Math.max(Number(process.argv[3]), 0) : (now - 90 * 86400)
  const endTs = process.argv[4] ? Math.max(Number(process.argv[4]), 0) : now

  if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || startTs > endTs) {
    usage()
  }

  // Delete existing rollups in the range (idempotent).
  await ch.command({
    query: `
      ALTER TABLE wallet_condition_pnl_1d
      DELETE WHERE wallet = {wallet:String}
        AND day >= toDate(toDateTime64({startTs:UInt64}, 3))
        AND day <= toDate(toDateTime64({endTs:UInt64}, 3))
    `,
    query_params: { wallet, startTs, endTs },
  })

  // Recompute from canonical ledger.
  await ch.command({
    query: `
      INSERT INTO wallet_condition_pnl_1d
      SELECT
        wallet,
        condition_id,
        toDate(block_timestamp) AS day,
        sum(toFloat64(realized_pnl)) AS realized_pnl_usd,
        sum(abs(toFloat64(usdc_delta))) AS volume_usd,
        toUInt32(countIf(realized_pnl != 0)) AS pnl_rows,
        toUInt32(countIf(realized_pnl > 0)) AS win_rows,
        toUInt32(countIf(realized_pnl < 0)) AS loss_rows,
        now64(3) AS updated_at
      FROM wallet_ledger FINAL
      WHERE wallet = {wallet:String}
        AND block_timestamp >= toDateTime64({startTs:UInt64}, 3)
        AND block_timestamp <= toDateTime64({endTs:UInt64}, 3)
      GROUP BY wallet, condition_id, day
    `,
    query_params: { wallet, startTs, endTs },
  })

  const res = await ch.query({
    query: `
      SELECT
        count() AS rows,
        min(day) AS min_day,
        max(day) AS max_day,
        sum(realized_pnl_usd) AS realized_pnl_usd
      FROM wallet_condition_pnl_1d FINAL
      WHERE wallet = {wallet:String}
        AND day >= toDate(toDateTime64({startTs:UInt64}, 3))
        AND day <= toDate(toDateTime64({endTs:UInt64}, 3))
    `,
    query_params: { wallet, startTs, endTs },
    format: 'JSONEachRow',
  })

  const row = (await res.json() as any[])[0]
  // eslint-disable-next-line no-console
  console.log(JSON.stringify({
    wallet,
    startTs,
    endTs,
    rows: Number(row?.rows ?? 0),
    minDay: row?.min_day ?? null,
    maxDay: row?.max_day ?? null,
    realizedPnlUsd: Number(row?.realized_pnl_usd ?? 0),
  }, null, 2))

  await ch.close()
  process.exit(0)
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Fatal:', err)
  process.exit(1)
})

