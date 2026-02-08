/**
 * API for frontend consumption.
 *
 * Endpoints:
 *   GET /health
 *   GET /pnl/:wallet?startTs=&endTs=
 *   GET /snapshots/:wallet?fromTs=&toTs=&limit=
 *   GET /ledger/:wallet?fromTs=&toTs=&limit=
 *   GET /positions?user=ADDRESS
 *   GET /activity?user=ADDRESS&limit=50&offset=0&type=all&conditionId=
 *   GET /portfolio/history?user=ADDRESS&interval=1d&from=&to=
 *   GET /user/stats?user=ADDRESS
 *   GET /trades?tokenId=ID&limit=50&offset=0
 *   GET /market/stats?conditionId=ID (or ?tokenId=ID)
 *   GET /leaderboard?sort=pnl&limit=20&period=all
 */

import { createServer, IncomingMessage, ServerResponse } from 'node:http'
import { createClient } from '@clickhouse/client'

const client = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
  request_timeout: 300_000,
  clickhouse_settings: {
    max_execution_time: 300,
  },
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

// ── Helpers ──────────────────────────────────────────────────────────

type TokenMeta = {
  condition_id: string
  question: string
  slug: string
  outcome: string
  outcome_index: number
}

async function getTokenMetaMap(tokenIds: string[]): Promise<Map<string, TokenMeta>> {
  if (tokenIds.length === 0) return new Map()
  const result = await client.query({
    query: `
      SELECT condition_id, question, slug, outcomes, token_ids
      FROM market_metadata FINAL
      WHERE hasAny(token_ids, {tokenIds:Array(String)})
    `,
    query_params: { tokenIds },
    format: 'JSONEachRow',
  })
  type Row = { condition_id: string; question: string; slug: string; outcomes: string[]; token_ids: string[] }
  const rows = await result.json() as Row[]
  const map = new Map<string, TokenMeta>()
  for (const m of rows) {
    const tids = Array.isArray(m.token_ids) ? m.token_ids : []
    const outs = Array.isArray(m.outcomes) ? m.outcomes : []
    for (let i = 0; i < tids.length; i++) {
      map.set(tids[i], {
        condition_id: m.condition_id,
        question: m.question,
        slug: m.slug,
        outcome: outs[i] ?? `Outcome ${i}`,
        outcome_index: i,
      })
    }
  }
  return map
}

async function getTokenIdsForCondition(conditionId: string): Promise<string[]> {
  const result = await client.query({
    query: `SELECT token_ids FROM market_metadata FINAL WHERE condition_id = {cid:String} LIMIT 1`,
    query_params: { cid: conditionId },
    format: 'JSONEachRow',
  })
  const rows = await result.json() as Array<{ token_ids: string[] }>
  return rows[0]?.token_ids ?? []
}

function round2(n: number): number {
  return Math.round(n * 100) / 100
}

// ── Existing Handlers ───────────────────────────────────────────────

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

  const metaMap = await getTokenMetaMap(tokenIds)

  const positions = balances.map(b => {
    const meta = metaMap.get(b.token_id)
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

// ── NEW: Enhanced Activity ──────────────────────────────────────────

async function handleActivity(url: URL, res: ServerResponse) {
  const user = url.searchParams.get('user')
  if (!user || !isValidAddress(user)) {
    json(res, 400, { error: 'Missing or invalid user address' })
    return
  }
  const wallet = user.toLowerCase()
  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 50), 1), 200)
  const offset = Math.max(Number(url.searchParams.get('offset') || 0), 0)
  const type = url.searchParams.get('type') || 'all'
  const conditionId = url.searchParams.get('conditionId')

  const validTypes = ['all', 'buy', 'sell']
  if (!validTypes.includes(type)) {
    json(res, 400, { error: 'Invalid type. Use: all, buy, sell' })
    return
  }

  const conditions: string[] = ['wallet = {wallet:String}']
  const params: Record<string, unknown> = { wallet, limit, offset }

  if (type === 'buy' || type === 'sell') {
    conditions.push(`side = {side:String}`)
    params.side = type
  }

  if (conditionId) {
    const condTokenIds = await getTokenIdsForCondition(conditionId)
    if (condTokenIds.length === 0) {
      json(res, 200, [])
      return
    }
    conditions.push(`token_id IN ({filterTokenIds:Array(String)})`)
    params.filterTokenIds = condTokenIds
  }

  const result = await client.query({
    query: `
      SELECT
        id,
        toUnixTimestamp(block_timestamp) AS timestamp,
        side,
        token_id,
        toFloat64(token_amount) / 1000000 AS amount,
        toFloat64(usdc_amount) / 1000000 AS value
      FROM wallet_trades
      WHERE ${conditions.join(' AND ')}
      ORDER BY block_timestamp DESC
      LIMIT {limit:UInt32}
      OFFSET {offset:UInt32}
    `,
    query_params: params,
    format: 'JSONEachRow',
  })

  type TradeRow = {
    id: string
    timestamp: number
    side: string
    token_id: string
    amount: number
    value: number
  }
  const rows = await result.json() as TradeRow[]

  const activityTokenIds = [...new Set(rows.map(r => r.token_id))]
  const metaMap = await getTokenMetaMap(activityTokenIds)

  const activity = rows.map(r => {
    const meta = metaMap.get(r.token_id)
    const price = r.amount > 0 ? round2(r.value / r.amount) : 0
    return {
      id: r.id,
      type: r.side.toLowerCase(),
      user: wallet,
      conditionId: meta?.condition_id ?? '',
      tokenId: r.token_id,
      outcomeIndex: meta?.outcome_index ?? 0,
      amount: r.amount,
      price,
      value: r.value,
      side: r.side.toUpperCase(),
      timestamp: r.timestamp,
      txHash: r.id.split('-')[0] || '',
      market: {
        question: meta?.question ?? '',
        slug: meta?.slug ?? '',
      },
    }
  })

  json(res, 200, activity)
}

// ── NEW: Portfolio History ───────────────────────────────────────────

async function handlePortfolioHistory(url: URL, res: ServerResponse) {
  const user = url.searchParams.get('user')
  if (!user || !isValidAddress(user)) {
    json(res, 400, { error: 'Missing or invalid user address' })
    return
  }
  const wallet = user.toLowerCase()

  const validIntervals = ['1h', '6h', '1d', '1w']
  const interval = url.searchParams.get('interval') || '1d'
  if (!validIntervals.includes(interval)) {
    json(res, 400, { error: 'Invalid interval. Use: 1h, 6h, 1d, 1w' })
    return
  }

  const now = Math.floor(Date.now() / 1000)
  const fromParam = url.searchParams.get('from')
  const toParam = url.searchParams.get('to')
  const fromTs = fromParam ? Math.floor(new Date(fromParam).getTime() / 1000) : now - 30 * 86400
  const toTs = toParam ? Math.floor(new Date(toParam).getTime() / 1000) : now

  const intervalSql: Record<string, string> = {
    '1h': 'INTERVAL 1 HOUR',
    '6h': 'INTERVAL 6 HOUR',
    '1d': 'INTERVAL 1 DAY',
    '1w': 'INTERVAL 1 WEEK',
  }

  const result = await client.query({
    query: `
      SELECT
        toUnixTimestamp(toStartOfInterval(snapshot_time, ${intervalSql[interval]})) AS timestamp,
        argMax(open_positions_value, snapshot_time) AS totalValue,
        argMax(token_count, snapshot_time) AS positions,
        argMax(realized_pnl + unrealized_pnl, snapshot_time) AS pnl
      FROM wallet_pnl_snapshots FINAL
      WHERE wallet = {wallet:String}
        AND snapshot_time >= toDateTime64({fromTs:UInt64}, 3)
        AND snapshot_time <= toDateTime64({toTs:UInt64}, 3)
      GROUP BY timestamp
      ORDER BY timestamp ASC
    `,
    query_params: { wallet, fromTs, toTs },
    format: 'JSONEachRow',
  })

  const snapshots = await result.json() as Array<{
    timestamp: number
    totalValue: number
    positions: number
    pnl: number
  }>

  json(res, 200, { user: wallet, interval, snapshots })
}

// ── NEW: User Stats ─────────────────────────────────────────────────

async function handleUserStats(url: URL, res: ServerResponse) {
  const user = url.searchParams.get('user')
  if (!user || !isValidAddress(user)) {
    json(res, 400, { error: 'Missing or invalid user address' })
    return
  }
  const wallet = user.toLowerCase()

  // Basic trade stats from wallet_trades view
  const statsResult = await client.query({
    query: `
      SELECT
        count() AS totalTrades,
        sum(toFloat64(usdc_amount)) / 1000000 AS totalVolume,
        toUnixTimestamp(min(block_timestamp)) AS firstTradeAt,
        toUnixTimestamp(max(block_timestamp)) AS lastTradeAt
      FROM wallet_trades
      WHERE wallet = {wallet:String}
    `,
    query_params: { wallet },
    format: 'JSONEachRow',
  })
  const stats = (await statsResult.json() as any[])[0]

  const totalTrades = Number(stats?.totalTrades ?? 0)
  if (totalTrades === 0) {
    json(res, 200, {
      user: wallet, totalTrades: 0, totalVolume: 0, marketsTraded: 0,
      winCount: null, lossCount: null, winRate: null, totalRealizedPnl: null,
      bestTrade: null, worstTrade: null, firstTradeAt: null, lastTradeAt: null, avgTradeSize: 0,
    })
    return
  }

  const totalVolume = round2(Number(stats.totalVolume))

  // Distinct token_ids → markets traded
  const tokenResult = await client.query({
    query: `SELECT DISTINCT token_id FROM wallet_trades WHERE wallet = {wallet:String}`,
    query_params: { wallet },
    format: 'JSONEachRow',
  })
  const tokenRows = await tokenResult.json() as Array<{ token_id: string }>
  const tokenIds = tokenRows.map(r => r.token_id)
  const metaMap = await getTokenMetaMap(tokenIds)
  const conditionSet = new Set<string>()
  for (const meta of metaMap.values()) conditionSet.add(meta.condition_id)

  // Win/loss from wallet_ledger (populated per-wallet by build-ledger)
  const ledgerResult = await client.query({
    query: `
      SELECT condition_id, sum(realized_pnl) AS total_pnl
      FROM wallet_ledger FINAL
      WHERE wallet = {wallet:String} AND realized_pnl != 0
      GROUP BY condition_id
    `,
    query_params: { wallet },
    format: 'JSONEachRow',
  })
  const ledgerRows = await ledgerResult.json() as Array<{ condition_id: string; total_pnl: number }>

  let winCount: number | null = null
  let lossCount: number | null = null
  let winRate: number | null = null
  let totalRealizedPnl: number | null = null
  let bestTrade: { market: string; conditionId: string; pnl: number } | null = null
  let worstTrade: { market: string; conditionId: string; pnl: number } | null = null

  if (ledgerRows.length > 0) {
    winCount = 0
    lossCount = 0
    totalRealizedPnl = 0
    let bestPnl = -Infinity
    let worstPnl = Infinity
    let bestCid = ''
    let worstCid = ''

    for (const row of ledgerRows) {
      totalRealizedPnl += row.total_pnl
      if (row.total_pnl > 0) winCount++
      else if (row.total_pnl < 0) lossCount++
      if (row.total_pnl > bestPnl) { bestPnl = row.total_pnl; bestCid = row.condition_id }
      if (row.total_pnl < worstPnl) { worstPnl = row.total_pnl; worstCid = row.condition_id }
    }

    winRate = (winCount + lossCount) > 0
      ? Math.round((winCount / (winCount + lossCount)) * 1000) / 1000
      : null
    totalRealizedPnl = round2(totalRealizedPnl)

    const findMeta = (cid: string) => [...metaMap.values()].find(m => m.condition_id === cid)
    if (bestPnl > -Infinity) bestTrade = { market: findMeta(bestCid)?.question ?? bestCid || 'Unknown', conditionId: bestCid, pnl: round2(bestPnl) }
    if (worstPnl < Infinity) worstTrade = { market: findMeta(worstCid)?.question ?? worstCid || 'Unknown', conditionId: worstCid, pnl: round2(worstPnl) }
  }

  json(res, 200, {
    user: wallet,
    totalTrades,
    totalVolume,
    marketsTraded: conditionSet.size,
    winCount,
    lossCount,
    winRate,
    totalRealizedPnl,
    bestTrade,
    worstTrade,
    firstTradeAt: Number(stats.firstTradeAt) || null,
    lastTradeAt: Number(stats.lastTradeAt) || null,
    avgTradeSize: totalTrades > 0 ? round2(totalVolume / totalTrades) : 0,
  })
}

// ── NEW: Trades (per token on-chain history) ────────────────────────

async function handleTrades(url: URL, res: ServerResponse) {
  const tokenId = url.searchParams.get('tokenId')
  if (!tokenId) {
    json(res, 400, { error: 'Missing tokenId parameter' })
    return
  }
  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 50), 1), 200)
  const offset = Math.max(Number(url.searchParams.get('offset') || 0), 0)

  const result = await client.query({
    query: `
      SELECT
        id,
        toFloat64(usdc_amount) / 1000000 AS value,
        toFloat64(token_amount) / 1000000 AS size,
        if(is_taker_buy, 'BUY', 'SELL') AS side,
        maker,
        taker,
        toUnixTimestamp(block_timestamp) AS timestamp,
        tx_hash,
        block_number
      FROM trades
      WHERE token_id = {tokenId:String}
      ORDER BY block_timestamp DESC, log_index DESC
      LIMIT 1 BY id
      LIMIT {limit:UInt32}
      OFFSET {offset:UInt32}
    `,
    query_params: { tokenId, limit, offset },
    format: 'JSONEachRow',
  })

  const rows = await result.json() as Array<{
    id: string; value: number; size: number; side: string
    maker: string; taker: string; timestamp: number
    tx_hash: string; block_number: number
  }>

  const trades = rows.map(r => ({
    id: r.id,
    price: r.size > 0 ? round2(r.value / r.size) : 0,
    size: r.size,
    side: r.side,
    maker: r.maker,
    taker: r.taker,
    timestamp: r.timestamp,
    txHash: r.tx_hash,
    blockNumber: Number(r.block_number),
  }))

  json(res, 200, trades)
}

// ── NEW: Market Stats ───────────────────────────────────────────────

async function handleMarketStats(url: URL, res: ServerResponse) {
  const conditionId = url.searchParams.get('conditionId')
  const tokenIdParam = url.searchParams.get('tokenId')

  if (!conditionId && !tokenIdParam) {
    json(res, 400, { error: 'Missing conditionId or tokenId parameter' })
    return
  }

  let tokenIds: string[]
  let resolvedConditionId = conditionId || ''

  if (conditionId) {
    tokenIds = await getTokenIdsForCondition(conditionId)
    if (tokenIds.length === 0) {
      json(res, 200, {})
      return
    }
  } else {
    tokenIds = [tokenIdParam!]
    const metaMap = await getTokenMetaMap(tokenIds)
    resolvedConditionId = metaMap.get(tokenIdParam!)?.condition_id ?? ''
  }

  // Trade stats
  const tradeResult = await client.query({
    query: `
      SELECT
        uniqExact(wallet) AS uniqueTraders,
        count() AS totalTrades,
        sum(toFloat64(usdc_amount)) / 1000000 AS onChainVolume,
        avg(toFloat64(usdc_amount)) / 1000000 AS avgTradeSize,
        max(toFloat64(usdc_amount)) / 1000000 AS largestTrade,
        toUnixTimestamp(max(block_timestamp)) AS lastTradeAt,
        sumIf(toFloat64(usdc_amount), block_timestamp >= now() - INTERVAL 24 HOUR) / 1000000 AS volume24h,
        sumIf(toFloat64(usdc_amount), block_timestamp >= now() - INTERVAL 7 DAY) / 1000000 AS volume7d
      FROM wallet_trades
      WHERE token_id IN ({tokenIds:Array(String)})
    `,
    query_params: { tokenIds },
    format: 'JSONEachRow',
  })
  const ts = (await tradeResult.json() as any[])[0] || {}

  // Holder count
  const holderResult = await client.query({
    query: `
      SELECT count() AS holderCount FROM (
        SELECT wallet FROM user_balances
        WHERE token_id IN ({tokenIds:Array(String)})
        GROUP BY wallet HAVING sum(balance) > 0
      )
    `,
    query_params: { tokenIds },
    format: 'JSONEachRow',
  })
  const hc = (await holderResult.json() as any[])[0]?.holderCount ?? 0

  // Top 5 holders + total supply for percentage
  const topResult = await client.query({
    query: `
      SELECT wallet AS user, sum(toFloat64(balance)) / 1000000 AS balance
      FROM user_balances
      WHERE token_id IN ({tokenIds:Array(String)})
        AND wallet != '0x0000000000000000000000000000000000000000'
      GROUP BY wallet HAVING balance > 0
      ORDER BY balance DESC
      LIMIT 5
    `,
    query_params: { tokenIds },
    format: 'JSONEachRow',
  })
  const topHolders = await topResult.json() as Array<{ user: string; balance: number }>

  const supplyResult = await client.query({
    query: `
      SELECT sum(toFloat64(balance)) / 1000000 AS totalSupply
      FROM user_balances
      WHERE token_id IN ({tokenIds:Array(String)})
        AND wallet != '0x0000000000000000000000000000000000000000'
    `,
    query_params: { tokenIds },
    format: 'JSONEachRow',
  })
  const totalSupply = Number((await supplyResult.json() as any[])[0]?.totalSupply ?? 0)

  json(res, 200, {
    conditionId: resolvedConditionId,
    uniqueTraders: Number(ts.uniqueTraders ?? 0),
    totalTrades: Number(ts.totalTrades ?? 0),
    onChainVolume: round2(Number(ts.onChainVolume ?? 0)),
    volume24h: round2(Number(ts.volume24h ?? 0)),
    volume7d: round2(Number(ts.volume7d ?? 0)),
    avgTradeSize: round2(Number(ts.avgTradeSize ?? 0)),
    largestTrade: round2(Number(ts.largestTrade ?? 0)),
    lastTradeAt: Number(ts.lastTradeAt ?? 0),
    holderCount: Number(hc),
    topHolders: topHolders.map(h => ({
      user: h.user,
      balance: round2(h.balance),
      percentage: totalSupply > 0 ? Math.round((h.balance / totalSupply) * 1000) / 10 : 0,
    })),
  })
}

// ── NEW: Leaderboard ────────────────────────────────────────────────

async function handleLeaderboard(url: URL, res: ServerResponse) {
  const sort = url.searchParams.get('sort') || 'pnl'
  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 20), 1), 100)
  const period = url.searchParams.get('period') || 'all'

  const validSorts = ['pnl', 'volume', 'trades']
  if (!validSorts.includes(sort)) {
    json(res, 400, { error: 'Invalid sort. Use: pnl, volume, trades' })
    return
  }
  const validPeriods = ['24h', '7d', '30d', 'all']
  if (!validPeriods.includes(period)) {
    json(res, 400, { error: 'Invalid period. Use: 24h, 7d, 30d, all' })
    return
  }

  const periodFilter: Record<string, string> = {
    '24h': 'AND block_timestamp >= now() - INTERVAL 24 HOUR',
    '7d': 'AND block_timestamp >= now() - INTERVAL 7 DAY',
    '30d': 'AND block_timestamp >= now() - INTERVAL 30 DAY',
    'all': '',
  }

  const sortCol: Record<string, string> = {
    pnl: 'totalPnl',
    volume: 'totalVolume',
    trades: 'totalTrades',
  }

  const result = await client.query({
    query: `
      SELECT
        wallet,
        count() AS totalTrades,
        sum(toFloat64(usdc_amount)) / 1000000 AS totalVolume,
        sum(CASE WHEN side = 'sell' THEN toFloat64(usdc_amount) ELSE -toFloat64(usdc_amount) END) / 1000000 AS totalPnl,
        uniqExact(token_id) AS marketsTraded
      FROM wallet_trades
      WHERE wallet NOT IN (
        '0x0000000000000000000000000000000000000000',
        '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',
        '0xc5d563a36ae78145c45a50134d48a1215220f80a',
        '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296',
        '0x3a3bd7bb9528e159577f7c2e685cc81a765002e2',
        '0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0',
        '0xb768891e3130f6df18214ac804d4db76c2c37730'
      )
        ${periodFilter[period]}
      GROUP BY wallet
      HAVING totalTrades >= 5
      ORDER BY ${sortCol[sort]} DESC
      LIMIT {limit:UInt32}
    `,
    query_params: { limit },
    format: 'JSONEachRow',
  })

  const rows = await result.json() as Array<{
    wallet: string; totalTrades: number; totalVolume: number; totalPnl: number; marketsTraded: number
  }>

  json(res, 200, {
    period,
    sort,
    updatedAt: Math.floor(Date.now() / 1000),
    traders: rows.map((r, i) => ({
      rank: i + 1,
      user: r.wallet,
      totalPnl: round2(Number(r.totalPnl)),
      totalVolume: round2(Number(r.totalVolume)),
      totalTrades: Number(r.totalTrades),
      winRate: null,
      marketsTraded: Number(r.marketsTraded),
    })),
  })
}

// ── Router ──────────────────────────────────────────────────────────

const server = createServer(async (req, res) => {
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
    // Simple routes
    if (pathname === '/health') { await handleHealth(req, res); return }
    if (pathname === '/positions') { await handlePositions(url, res); return }
    if (pathname === '/activity') { await handleActivity(url, res); return }
    if (pathname === '/portfolio/history') { await handlePortfolioHistory(url, res); return }
    if (pathname === '/user/stats') { await handleUserStats(url, res); return }
    if (pathname === '/trades') { await handleTrades(url, res); return }
    if (pathname === '/market/stats') { await handleMarketStats(url, res); return }
    if (pathname === '/leaderboard') { await handleLeaderboard(url, res); return }

    // Path-param routes: /:resource/:wallet
    const parts = pathname.split('/').filter(Boolean)
    if (parts.length >= 2) {
      const [resource, wallet] = parts
      if (resource === 'pnl') { await handlePnl(wallet, url, res); return }
      if (resource === 'snapshots') { await handleSnapshots(wallet, url, res); return }
      if (resource === 'ledger') { await handleLedger(wallet, url, res); return }
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
