/**
 * Snapshot scheduler (looped backfill).
 *
 * Usage:
 *   tsx src/snapshot-scheduler.ts --wallets-file wallets.txt --interval 1h --loop --sleep 3600
 *   tsx src/snapshot-scheduler.ts --wallets-from-ch top-volume --wallet-limit 2000 --period 30d --interval 1d
 */

import { createClient } from '@clickhouse/client'
import { readFileSync } from 'node:fs'
import { buildLedgerAndSnapshots, closeClient } from './ledger-engine.js'

const EXCLUDED_WALLETS = [
  '0x0000000000000000000000000000000000000000',
  '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',
  '0xc5d563a36ae78145c45a50134d48a1215220f80a',
  '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296',
  '0x3a3bd7bb9528e159577f7c2e685cc81a765002e2',
  '0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0',
  '0xb768891e3130f6df18214ac804d4db76c2c37730',
] as const

function parsePeriodHours(value?: string): number {
  if (!value) return 24 * 30
  const match = value.match(/^(\d+)([hd])$/i)
  if (!match) return 24 * 30
  const n = Number(match[1])
  const unit = match[2].toLowerCase()
  return unit === 'h' ? n : n * 24
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

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function processWallet(
  client: ReturnType<typeof createClient>,
  wallet: string,
  intervalSeconds: number,
  startTs?: number,
  endTs?: number,
  dryRun?: boolean
) {
  const { ledgerEntries, snapshots } = await buildLedgerAndSnapshots(wallet, intervalSeconds, startTs, endTs)
  console.log(`[${wallet}] ledger=${ledgerEntries.length} snapshots=${snapshots.length}`)
  if (dryRun) return

  const w = wallet.toLowerCase()
  const timeFilterLedger = startTs || endTs
    ? `AND block_timestamp >= toDateTime64({startTs:UInt64}, 3) AND block_timestamp <= toDateTime64({endTs:UInt64}, 3)`
    : ''
  const timeFilterSnapshots = startTs || endTs
    ? `AND snapshot_time >= toDateTime64({startTs:UInt64}, 3) AND snapshot_time <= toDateTime64({endTs:UInt64}, 3)`
    : ''
  const timeParams = startTs || endTs
    ? { startTs: startTs ?? 0, endTs: endTs ?? Math.floor(Date.now() / 1000) }
    : {}

  await client.exec({
    query: `ALTER TABLE wallet_ledger DELETE WHERE wallet = {wallet:String} ${timeFilterLedger}`,
    query_params: { wallet: w, ...timeParams },
  })
  await client.exec({
    query: `ALTER TABLE wallet_pnl_snapshots DELETE WHERE wallet = {wallet:String} ${timeFilterSnapshots}`,
    query_params: { wallet: w, ...timeParams },
  })

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

async function runOnce(wallets: string[], intervalSeconds: number, startTs?: number, endTs?: number, dryRun?: boolean) {
  const client = createClient({
    url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
    database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
    username: process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
  })

  for (const wallet of wallets) {
    await processWallet(client, wallet, intervalSeconds, startTs, endTs, dryRun)
  }

  await client.close()
  await closeClient()
}

async function fetchWalletsFromClickHouse(
  source: 'top-volume' | 'top-trades',
  limit: number,
  periodHours: number,
): Promise<string[]> {
  const client = createClient({
    url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
    database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
    username: process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
  })

  const sortExpr = source === 'top-trades' ? 'totalTrades' : 'totalVolume'
  const result = await client.query({
    query: `
      SELECT
        wallet,
        countMerge(trades_state) AS totalTrades,
        sumMerge(volume_state) AS totalVolume
      FROM wallet_leaderboard_stats_1h
      WHERE bucket >= toStartOfHour(now() - toIntervalHour({periodHours:UInt32}))
        AND wallet NOT IN ({excluded:Array(String)})
      GROUP BY wallet
      HAVING totalTrades >= 5
      ORDER BY ${sortExpr} DESC
      LIMIT {limit:UInt32}
    `,
    query_params: { limit, periodHours, excluded: Array.from(EXCLUDED_WALLETS) },
    format: 'JSONEachRow',
  })

  const rows = await result.json() as Array<{ wallet: string }>
  await client.close()
  return rows.map((r) => r.wallet.toLowerCase())
}

async function main() {
  const args = process.argv.slice(2)
  const walletsFile = args.includes('--wallets-file') ? args[args.indexOf('--wallets-file') + 1] : undefined
  const walletsFromCh = args.includes('--wallets-from-ch') ? (args[args.indexOf('--wallets-from-ch') + 1] as 'top-volume' | 'top-trades') : undefined
  if (!walletsFile && !walletsFromCh) {
    console.log('Usage: tsx src/snapshot-scheduler.ts (--wallets-file wallets.txt | --wallets-from-ch top-volume|top-trades) [--wallet-limit 2000] [--period 30d] [--interval 1d] [--loop] [--sleep 3600]')
    process.exit(1)
  }

  const interval = parseInterval(args.includes('--interval') ? args[args.indexOf('--interval') + 1] : undefined)
  const loop = args.includes('--loop')
  const sleepSeconds = args.includes('--sleep') ? Number(args[args.indexOf('--sleep') + 1]) : interval
  const startTs = args.includes('--startTs') ? Number(args[args.indexOf('--startTs') + 1]) : undefined
  const endTs = args.includes('--endTs') ? Number(args[args.indexOf('--endTs') + 1]) : undefined
  const dryRun = args.includes('--dry-run')
  const walletLimit = args.includes('--wallet-limit') ? Number(args[args.indexOf('--wallet-limit') + 1]) : 2000
  const periodHours = parsePeriodHours(args.includes('--period') ? args[args.indexOf('--period') + 1] : undefined)

  const wallets = walletsFile
    ? readFileSync(walletsFile, 'utf8')
      .split(/\r?\n/)
      .map((w) => w.trim())
      .filter(Boolean)
      .map((w) => w.toLowerCase())
    : await fetchWalletsFromClickHouse(walletsFromCh!, walletLimit, periodHours)

  if (!loop) {
    await runOnce(wallets, interval, startTs, endTs, dryRun)
    return
  }

  while (true) {
    const now = Math.floor(Date.now() / 1000)
    await runOnce(wallets, interval, startTs, endTs ?? now, dryRun)
    await sleep(sleepSeconds * 1000)
  }
}

main().catch((error) => {
  console.error('Snapshot scheduler failed:', error)
  process.exit(1)
})
