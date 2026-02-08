/**
 * Minimal API for frontend consumption.
 *
 * Endpoints:
 *   GET /health
 *   GET /pnl/:wallet?startTs=&endTs=
 *   GET /snapshots/:wallet?fromTs=&toTs=&limit=
 *   GET /ledger/:wallet?fromTs=&toTs=&limit=
 *   GET /positions?user=ADDRESS
 *   GET /activity?user=ADDRESS&limit=50
 */

import { createServer, IncomingMessage, ServerResponse } from 'node:http'
import { createClient } from '@clickhouse/client'

const client = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
})

function isValidAddress(addr: string): boolean {
  return /^0x[a-fA-F0-9]{40}$/.test(addr)
}

function json(res: ServerResponse, status: number, body: unknown) {
  res.statusCode = status
  res.setHeader('Content-Type', 'application/json')
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type')
  res.end(JSON.stringify(body))
}

async function querySnapshot(wallet: string, ts: number) {
  const result = await client.query({
    query: `
      SELECT *
      FROM wallet_pnl_snapshots FINAL
      WHERE wallet = {wallet:String}
        AND snapshot_time <= toDateTime64({ts:UInt64}, 3)
      ORDER BY snapshot_time DESC
      LIMIT 1
    `,
    query_params: { wallet: wallet.toLowerCase(), ts },
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

async function handleHealth(_req: IncomingMessage, res: ServerResponse) {
  json(res, 200, { ok: true })
}

async function handlePnl(wallet: string, url: URL, res: ServerResponse) {
  if (!isValidAddress(wallet)) {
    json(res, 400, { error: 'Invalid wallet address' })
    return
  }
  const startTs = Number(url.searchParams.get('startTs') || 0)
  const endTs = Number(url.searchParams.get('endTs') || Math.floor(Date.now() / 1000))
  const start = await querySnapshot(wallet, startTs)
  const end = await querySnapshot(wallet, endTs)
  if (!start || !end) {
    json(res, 404, { error: 'Snapshots not found' })
    return
  }
  const realizedDelta = end.realized_pnl - start.realized_pnl
  const unrealizedDelta = end.unrealized_pnl - start.unrealized_pnl
  const cashflowDelta = end.cashflow - start.cashflow
  json(res, 200, {
    wallet: wallet.toLowerCase(),
    start_snapshot: start.snapshot_time,
    end_snapshot: end.snapshot_time,
    realized_delta: realizedDelta,
    unrealized_delta: unrealizedDelta,
    cashflow_delta: cashflowDelta,
    total_delta: realizedDelta + unrealizedDelta,
  })
}

async function handleSnapshots(wallet: string, url: URL, res: ServerResponse) {
  if (!isValidAddress(wallet)) {
    json(res, 400, { error: 'Invalid wallet address' })
    return
  }
  const fromTs = url.searchParams.get('fromTs')
  const toTs = url.searchParams.get('toTs')
  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 500), 1), 10000)

  let query = `SELECT * FROM wallet_pnl_snapshots FINAL WHERE wallet = {wallet:String}`
  const params: Record<string, unknown> = { wallet: wallet.toLowerCase(), limit }

  if (fromTs) {
    query += ` AND snapshot_time >= toDateTime64({fromTs:UInt64}, 3)`
    params.fromTs = Number(fromTs)
  }
  if (toTs) {
    query += ` AND snapshot_time <= toDateTime64({toTs:UInt64}, 3)`
    params.toTs = Number(toTs)
  }
  query += ` ORDER BY snapshot_time ASC LIMIT {limit:UInt32}`

  const result = await client.query({ query, query_params: params, format: 'JSONEachRow' })
  const rows = await result.json()
  json(res, 200, rows)
}

async function handleLedger(wallet: string, url: URL, res: ServerResponse) {
  if (!isValidAddress(wallet)) {
    json(res, 400, { error: 'Invalid wallet address' })
    return
  }
  const fromTs = url.searchParams.get('fromTs')
  const toTs = url.searchParams.get('toTs')
  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 1000), 1), 10000)

  let query = `SELECT * FROM wallet_ledger FINAL WHERE wallet = {wallet:String}`
  const params: Record<string, unknown> = { wallet: wallet.toLowerCase(), limit }

  if (fromTs) {
    query += ` AND block_timestamp >= toDateTime64({fromTs:UInt64}, 3)`
    params.fromTs = Number(fromTs)
  }
  if (toTs) {
    query += ` AND block_timestamp <= toDateTime64({toTs:UInt64}, 3)`
    params.toTs = Number(toTs)
  }
  query += ` ORDER BY block_timestamp ASC, log_index ASC LIMIT {limit:UInt32}`

  const result = await client.query({ query, query_params: params, format: 'JSONEachRow' })
  const rows = await result.json()
  json(res, 200, rows)
}

async function handlePositions(url: URL, res: ServerResponse) {
  const user = url.searchParams.get('user')
  if (!user || !isValidAddress(user)) {
    json(res, 400, { error: 'Missing or invalid user address' })
    return
  }
  const wallet = user.toLowerCase()

  // Get token balances from SummingMergeTree (need to force merge with GROUP BY)
  const balanceResult = await client.query({
    query: `
      SELECT token_id, sum(balance) AS balance
      FROM user_balances
      WHERE wallet = {wallet:String}
      GROUP BY token_id
      HAVING balance > 0
    `,
    query_params: { wallet },
    format: 'JSONEachRow',
  })
  const balances = await balanceResult.json() as Array<{ token_id: string; balance: string }>

  if (balances.length === 0) {
    json(res, 200, [])
    return
  }

  const tokenIds = balances.map(b => b.token_id)

  // Get avg buy price per token from wallet_trades view
  const priceResult = await client.query({
    query: `
      SELECT
        token_id,
        sum(usdc_amount) / sum(token_amount) AS avg_price
      FROM wallet_trades
      WHERE wallet = {wallet:String}
        AND side = 'buy'
        AND token_id IN ({tokenIds:Array(String)})
      GROUP BY token_id
    `,
    query_params: { wallet, tokenIds },
    format: 'JSONEachRow',
  })
  const prices = await priceResult.json() as Array<{ token_id: string; avg_price: number }>
  const priceMap = new Map(prices.map(p => [p.token_id, p.avg_price]))

  // Get market metadata: token_id → (condition_id, question, slug, outcome_index)
  const metaResult = await client.query({
    query: `
      SELECT
        condition_id,
        question,
        slug,
        outcomes,
        token_ids
      FROM market_metadata FINAL
      WHERE hasAny(token_ids, {tokenIds:Array(String)})
    `,
    query_params: { tokenIds },
    format: 'JSONEachRow',
  })
  type MetaRow = {
    condition_id: string
    question: string
    slug: string
    outcomes: string[]
    token_ids: string[]
  }
  const metaRows = await metaResult.json() as MetaRow[]

  // Build token_id → metadata lookup
  const tokenMeta = new Map<string, { condition_id: string; question: string; slug: string; outcome: string; outcome_index: number }>()
  for (const m of metaRows) {
    const tids = Array.isArray(m.token_ids) ? m.token_ids : []
    const outs = Array.isArray(m.outcomes) ? m.outcomes : []
    for (let i = 0; i < tids.length; i++) {
      tokenMeta.set(tids[i], {
        condition_id: m.condition_id,
        question: m.question,
        slug: m.slug,
        outcome: outs[i] ?? `Outcome ${i}`,
        outcome_index: i,
      })
    }
  }

  const positions = balances.map(b => {
    const meta = tokenMeta.get(b.token_id)
    const size = Number(b.balance) / 1e6
    const avgPrice = priceMap.get(b.token_id) ?? 0
    const initialValue = size * avgPrice
    return {
      asset: b.token_id,
      condition_id: meta?.condition_id ?? '',
      outcome: meta?.outcome ?? '',
      outcome_index: meta?.outcome_index ?? 0,
      question: meta?.question ?? '',
      slug: meta?.slug ?? '',
      size,
      avg_price: avgPrice,
      initial_value: initialValue,
    }
  })

  json(res, 200, positions)
}

async function handleActivity(url: URL, res: ServerResponse) {
  const user = url.searchParams.get('user')
  if (!user || !isValidAddress(user)) {
    json(res, 400, { error: 'Missing or invalid user address' })
    return
  }
  const wallet = user.toLowerCase()
  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 50), 1), 500)

  const result = await client.query({
    query: `
      SELECT
        id,
        block_timestamp,
        side,
        token_id,
        token_amount,
        usdc_amount,
        price_per_token
      FROM wallet_trades
      WHERE wallet = {wallet:String}
      ORDER BY block_timestamp DESC
      LIMIT {limit:UInt32}
    `,
    query_params: { wallet, limit },
    format: 'JSONEachRow',
  })

  type TradeRow = {
    id: string
    block_timestamp: string
    side: string
    token_id: string
    token_amount: string
    usdc_amount: string
    price_per_token: number
  }
  const rows = await result.json() as TradeRow[]

  // Enrich with market metadata
  const activityTokenIds = [...new Set(rows.map(r => r.token_id))]
  const actMetaResult = await client.query({
    query: `
      SELECT condition_id, question, slug, outcomes, token_ids
      FROM market_metadata FINAL
      WHERE hasAny(token_ids, {tokenIds:Array(String)})
    `,
    query_params: { tokenIds: activityTokenIds },
    format: 'JSONEachRow',
  })
  type ActMetaRow = { condition_id: string; question: string; slug: string; outcomes: string[]; token_ids: string[] }
  const actMeta = await actMetaResult.json() as ActMetaRow[]
  const actTokenMeta = new Map<string, { condition_id: string; question: string; outcome: string }>()
  for (const m of actMeta) {
    const tids = Array.isArray(m.token_ids) ? m.token_ids : []
    const outs = Array.isArray(m.outcomes) ? m.outcomes : []
    for (let i = 0; i < tids.length; i++) {
      actTokenMeta.set(tids[i], { condition_id: m.condition_id, question: m.question, outcome: outs[i] ?? '' })
    }
  }

  const activity = rows.map(r => {
    const meta = actTokenMeta.get(r.token_id)
    return {
      type: 'trade' as const,
      timestamp: r.block_timestamp,
      condition_id: meta?.condition_id ?? '',
      question: meta?.question ?? '',
      outcome: meta?.outcome ?? '',
      side: r.side.toUpperCase(),
      price: r.price_per_token,
      size: Number(r.token_amount) / 1e6,
      value: Number(r.usdc_amount) / 1e6,
      transaction_hash: r.id.split('-')[0] || '',
    }
  })

  json(res, 200, activity)
}

const server = createServer(async (req, res) => {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.statusCode = 204
    res.setHeader('Access-Control-Allow-Origin', '*')
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS')
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type')
    res.end()
    return
  }

  if (!req.url) {
    json(res, 400, { error: 'Missing URL' })
    return
  }

  const url = new URL(req.url, `http://${req.headers.host}`)
  const pathname = url.pathname

  try {
    if (pathname === '/health') {
      await handleHealth(req, res)
      return
    }

    // Query-param based routes
    if (pathname === '/positions') {
      await handlePositions(url, res)
      return
    }
    if (pathname === '/activity') {
      await handleActivity(url, res)
      return
    }

    // Path-param based routes: /:resource/:wallet
    const parts = pathname.split('/').filter(Boolean)
    if (parts.length < 2) {
      json(res, 404, { error: 'Not found' })
      return
    }

    const [resource, wallet] = parts

    if (resource === 'pnl') {
      await handlePnl(wallet, url, res)
      return
    }
    if (resource === 'snapshots') {
      await handleSnapshots(wallet, url, res)
      return
    }
    if (resource === 'ledger') {
      await handleLedger(wallet, url, res)
      return
    }

    json(res, 404, { error: 'Not found' })
  } catch (error: any) {
    console.error(`Error handling ${pathname}:`, error)
    json(res, 500, { error: error?.message || 'Internal error' })
  }
})

const port = Number(process.env.PORT || 3002)
server.listen(port, () => {
  console.log(`API listening on :${port}`)
})
