/**
 * PnL from snapshots: realized(end) - realized(start), plus unrealized delta.
 *
 * Usage:
 *   tsx src/pnl-from-snapshots.ts <wallet> <startTs> <endTs>
 */

import { createClient } from '@clickhouse/client'

async function querySnapshot(client: ReturnType<typeof createClient>, wallet: string, ts: number) {
  const result = await client.query({
    query: `
      SELECT *
      FROM wallet_pnl_snapshots FINAL
      WHERE wallet = '${wallet.toLowerCase()}'
        AND snapshot_time <= toDateTime64(${ts}, 3)
      ORDER BY snapshot_time DESC
      LIMIT 1
    `,
    format: 'JSONEachRow',
  })
  const rows = await result.json() as Array<{
    snapshot_time: string
    realized_pnl: number
    unrealized_pnl: number
    open_positions_cost: number
    open_positions_value: number
    cashflow: number
  }>
  return rows[0]
}

async function main() {
  const args = process.argv.slice(2)
  if (args.length < 3) {
    console.log('Usage: tsx src/pnl-from-snapshots.ts <wallet> <startTs> <endTs>')
    process.exit(1)
  }

  const wallet = args[0]
  const startTs = Number(args[1])
  const endTs = Number(args[2])

  const client = createClient({
    url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
    database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
    username: process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
  })

  const start = await querySnapshot(client, wallet, startTs)
  const end = await querySnapshot(client, wallet, endTs)

  if (!start || !end) {
    console.log('Missing snapshots for given range.')
    await client.close()
    process.exit(1)
  }

  const realizedDelta = end.realized_pnl - start.realized_pnl
  const unrealizedDelta = end.unrealized_pnl - start.unrealized_pnl
  const cashflowDelta = end.cashflow - start.cashflow
  const totalDelta = realizedDelta + unrealizedDelta

  console.log(`Wallet: ${wallet}`)
  console.log(`Start snapshot: ${start.snapshot_time}`)
  console.log(`End snapshot: ${end.snapshot_time}`)
  console.log(`Realized Δ: ${realizedDelta.toFixed(2)}`)
  console.log(`Unrealized Δ: ${unrealizedDelta.toFixed(2)}`)
  console.log(`Cashflow Δ: ${cashflowDelta.toFixed(2)}`)
  console.log(`Total Δ: ${totalDelta.toFixed(2)}`)

  await client.close()
}

main().catch((error) => {
  console.error('Snapshot PnL failed:', error)
  process.exit(1)
})
