/**
 * Build wallet ledger + PnL snapshots into ClickHouse.
 *
 * Usage: tsx src/build-ledger.ts <wallet> [interval] [startTs] [endTs] [--dry-run]
 * interval: 1d (default), 1h, 15m, or seconds
 */

import { createClient } from '@clickhouse/client'
import { buildLedgerAndSnapshots, closeClient } from './ledger-engine.js'

function parseInterval(value?: string): number {
  if (!value) return 86400
  if (/^\d+$/.test(value)) return Number(value)
  const match = value.match(/^(\d+)([smhd])$/i)
  if (!match) return 86400
  const amount = Number(match[1])
  const unit = match[2].toLowerCase()
  switch (unit) {
    case 's':
      return amount
    case 'm':
      return amount * 60
    case 'h':
      return amount * 3600
    case 'd':
      return amount * 86400
    default:
      return 86400
  }
}

async function main() {
  const args = process.argv.slice(2)
  if (args.length < 1) {
    console.log(`
Usage: tsx src/build-ledger.ts <wallet> [interval] [startTs] [endTs] [--dry-run]

Examples:
  tsx src/build-ledger.ts 0x1234...
  tsx src/build-ledger.ts 0x1234... 1h
  tsx src/build-ledger.ts 0x1234... 1d 1704067200 1735689600
  tsx src/build-ledger.ts 0x1234... 3600 --dry-run
`)
    process.exit(1)
  }

  const wallet = args[0]
  const interval = parseInterval(args[1])
  const startTs = args[2] ? parseInt(args[2]) : undefined
  const endTs = args[3] ? parseInt(args[3]) : undefined
  const dryRun = args.includes('--dry-run')

  const { ledgerEntries, snapshots } = await buildLedgerAndSnapshots(wallet, interval, startTs, endTs)

  console.log(`Ledger entries: ${ledgerEntries.length}`)
  console.log(`Snapshots: ${snapshots.length}`)

  if (dryRun) {
    await closeClient()
    return
  }

  const client = createClient({
    url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
    database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
    username: process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
  })

  const w = wallet.toLowerCase()

  // Build parameterized DELETE for wallet_ledger
  let ledgerDelete = `ALTER TABLE wallet_ledger DELETE WHERE wallet = {wallet:String}`
  const ledgerParams: Record<string, unknown> = { wallet: w }
  if (startTs || endTs) {
    ledgerDelete += ` AND block_timestamp >= toDateTime64({startTs:UInt64}, 3) AND block_timestamp <= toDateTime64({endTs:UInt64}, 3)`
    ledgerParams.startTs = startTs ?? 0
    ledgerParams.endTs = endTs ?? Math.floor(Date.now() / 1000)
  }

  // Build parameterized DELETE for wallet_pnl_snapshots
  let snapshotDelete = `ALTER TABLE wallet_pnl_snapshots DELETE WHERE wallet = {wallet:String}`
  const snapshotParams: Record<string, unknown> = { wallet: w }
  if (startTs || endTs) {
    snapshotDelete += ` AND snapshot_time >= toDateTime64({startTs:UInt64}, 3) AND snapshot_time <= toDateTime64({endTs:UInt64}, 3)`
    snapshotParams.startTs = startTs ?? 0
    snapshotParams.endTs = endTs ?? Math.floor(Date.now() / 1000)
  }

  await client.exec({ query: ledgerDelete, query_params: ledgerParams })
  await client.exec({ query: snapshotDelete, query_params: snapshotParams })

  if (ledgerEntries.length > 0) {
    await client.insert({
      table: 'wallet_ledger',
      values: ledgerEntries,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }

  if (snapshots.length > 0) {
    await client.insert({
      table: 'wallet_pnl_snapshots',
      values: snapshots,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }

  await client.close()
  await closeClient()
}

main().catch((error) => {
  console.error('Error building ledger:', error)
  process.exit(1)
})
