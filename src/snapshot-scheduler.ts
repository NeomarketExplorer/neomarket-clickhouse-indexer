/**
 * Snapshot scheduler (looped backfill).
 *
 * Usage:
 *   tsx src/snapshot-scheduler.ts --wallets-file wallets.txt --interval 1h --loop --sleep 3600
 */

import { createClient } from '@clickhouse/client'
import { readFileSync } from 'node:fs'
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
    ? `AND block_timestamp >= toDateTime64(${startTs ?? 0}, 3) AND block_timestamp <= toDateTime64(${endTs ?? Math.floor(Date.now() / 1000)}, 3)`
    : ''
  const timeFilterSnapshots = startTs || endTs
    ? `AND snapshot_time >= toDateTime64(${startTs ?? 0}, 3) AND snapshot_time <= toDateTime64(${endTs ?? Math.floor(Date.now() / 1000)}, 3)`
    : ''

  await client.exec({
    query: `ALTER TABLE wallet_ledger DELETE WHERE wallet = '${w}' ${timeFilterLedger}`,
  })
  await client.exec({
    query: `ALTER TABLE wallet_pnl_snapshots DELETE WHERE wallet = '${w}' ${timeFilterSnapshots}`,
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

async function main() {
  const args = process.argv.slice(2)
  const walletsFile = args.includes('--wallets-file') ? args[args.indexOf('--wallets-file') + 1] : undefined
  if (!walletsFile) {
    console.log('Usage: tsx src/snapshot-scheduler.ts --wallets-file wallets.txt [--interval 1h] [--loop] [--sleep 3600]')
    process.exit(1)
  }

  const interval = parseInterval(args.includes('--interval') ? args[args.indexOf('--interval') + 1] : undefined)
  const loop = args.includes('--loop')
  const sleepSeconds = args.includes('--sleep') ? Number(args[args.indexOf('--sleep') + 1]) : interval
  const startTs = args.includes('--startTs') ? Number(args[args.indexOf('--startTs') + 1]) : undefined
  const endTs = args.includes('--endTs') ? Number(args[args.indexOf('--endTs') + 1]) : undefined
  const dryRun = args.includes('--dry-run')

  const wallets = readFileSync(walletsFile, 'utf8')
    .split(/\r?\n/)
    .map((w) => w.trim())
    .filter(Boolean)
    .map((w) => w.toLowerCase())

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
