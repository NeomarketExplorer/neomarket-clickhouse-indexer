/**
 * Backfill ledger + snapshots for many wallets.
 *
 * Usage:
 *   tsx src/backfill-ledger.ts --wallets-file wallets.txt --interval 1d --concurrency 2
 *   tsx src/backfill-ledger.ts --limit 1000 --offset 0 --interval 1h
 */

import { createClient } from '@clickhouse/client'
import { readFileSync } from 'node:fs'
import { buildLedgerAndSnapshots, closeClient } from './ledger-engine.js'

type Args = {
  walletsFile?: string
  limit?: number
  offset?: number
  interval?: number
  startTs?: number
  endTs?: number
  concurrency: number
  dryRun: boolean
}

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

function parseArgs(): Args {
  const args = process.argv.slice(2)
  const get = (flag: string): string | undefined => {
    const idx = args.indexOf(flag)
    if (idx === -1) return undefined
    return args[idx + 1]
  }

  return {
    walletsFile: get('--wallets-file'),
    limit: get('--limit') ? Number(get('--limit')) : undefined,
    offset: get('--offset') ? Number(get('--offset')) : undefined,
    interval: parseInterval(get('--interval')),
    startTs: get('--startTs') ? Number(get('--startTs')) : undefined,
    endTs: get('--endTs') ? Number(get('--endTs')) : undefined,
    concurrency: get('--concurrency') ? Number(get('--concurrency')) : 2,
    dryRun: args.includes('--dry-run'),
  }
}

async function fetchWalletsFromSource(client: ReturnType<typeof createClient>, limit?: number, offset?: number) {
  const limitClause = limit ? `LIMIT ${limit}` : ''
  const offsetClause = offset ? `OFFSET ${offset}` : ''
  const query = `
    SELECT DISTINCT wallet FROM (
      SELECT maker AS wallet FROM trades FINAL
      UNION ALL SELECT taker AS wallet FROM trades FINAL
      UNION ALL SELECT stakeholder AS wallet FROM splits FINAL
      UNION ALL SELECT stakeholder AS wallet FROM merges FINAL
      UNION ALL SELECT redeemer AS wallet FROM redemptions FINAL
      UNION ALL SELECT from AS wallet FROM transfers FINAL
      UNION ALL SELECT to AS wallet FROM transfers FINAL
      UNION ALL SELECT stakeholder AS wallet FROM adapter_splits FINAL
      UNION ALL SELECT stakeholder AS wallet FROM adapter_merges FINAL
      UNION ALL SELECT redeemer AS wallet FROM adapter_redemptions FINAL
      UNION ALL SELECT stakeholder AS wallet FROM adapter_conversions FINAL
      UNION ALL SELECT to AS wallet FROM fee_refunds FINAL
      UNION ALL SELECT to AS wallet FROM fee_withdrawals FINAL
    )
    WHERE wallet != ''
    ${limitClause}
    ${offsetClause}
  `
  const result = await client.query({ query, format: 'JSONEachRow' })
  const rows = await result.json() as { wallet: string }[]
  return rows.map((row) => row.wallet.toLowerCase())
}

async function upsertLedger(
  client: ReturnType<typeof createClient>,
  wallet: string,
  interval: number,
  startTs?: number,
  endTs?: number,
  dryRun?: boolean
) {
  const { ledgerEntries, snapshots } = await buildLedgerAndSnapshots(wallet, interval, startTs, endTs)

  console.log(`[${wallet}] ledger=${ledgerEntries.length} snapshots=${snapshots.length}`)
  if (dryRun) return

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
}

async function run() {
  const args = parseArgs()

  const client = createClient({
    url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
    database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
    username: process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
  })

  let wallets: string[] = []
  if (args.walletsFile) {
    const raw = readFileSync(args.walletsFile, 'utf8')
    wallets = raw.split(/\r?\n/).map((w) => w.trim()).filter(Boolean).map((w) => w.toLowerCase())
  } else {
    wallets = await fetchWalletsFromSource(client, args.limit, args.offset)
  }

  wallets = Array.from(new Set(wallets))
  if (wallets.length === 0) {
    console.log('No wallets found.')
    await client.close()
    await closeClient()
    return
  }

  console.log(`Processing ${wallets.length} wallets...`)
  const concurrency = Math.max(1, args.concurrency)
  let index = 0

  const workers = Array.from({ length: concurrency }, async () => {
    while (index < wallets.length) {
      const wallet = wallets[index++]
      await upsertLedger(client, wallet, args.interval ?? 86400, args.startTs, args.endTs, args.dryRun)
    }
  })

  await Promise.all(workers)
  await client.close()
  await closeClient()
}

run().catch((error) => {
  console.error('Backfill failed:', error)
  process.exit(1)
})
