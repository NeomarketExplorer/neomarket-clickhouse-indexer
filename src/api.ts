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
 *   GET /market/candles?conditionId=&tokenId=&interval=1h&from=&to=&limit=500
 *   GET /discover/markets?window=1h&limit=20&offset=0&category=&eventId=
 *   GET /leaderboard?sort=netCashflow|pnl|volume|trades&limit=20&period=all&category=&eventId=
 *   GET /leaderboard/explain?user=ADDRESS&period=all&limit=1000&metric=netCashflow|pnl
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
  token_id: string
  condition_id: string
  market_id: string
  question: string
  slug: string
  outcome: string
  outcome_index: number
}

async function getTokenMetaMap(tokenIds: string[]): Promise<Map<string, TokenMeta>> {
  if (tokenIds.length === 0) return new Map()
  // `token_metadata` is derived; keep API functional even if schema isn't deployed yet.
  try {
    const result = await client.query({
      query: `
        SELECT
          token_id,
          condition_id,
          market_id,
          question,
          slug,
          outcome,
          outcome_index
        FROM token_metadata FINAL
        WHERE token_id IN ({tokenIds:Array(String)})
      `,
      query_params: { tokenIds },
      format: 'JSONEachRow',
    })
    type Row = {
      token_id: string
      condition_id: string
      market_id: string
      question: string
      slug: string
      outcome: string
      outcome_index: number
    }
    const rows = await result.json() as Row[]
    const map = new Map<string, TokenMeta>()
    for (const r of rows) {
      map.set(r.token_id, {
        token_id: r.token_id,
        condition_id: r.condition_id,
        market_id: r.market_id,
        question: r.question,
        slug: r.slug,
        outcome: r.outcome,
        outcome_index: Number(r.outcome_index),
      })
    }
    return map
  } catch {
    const result = await client.query({
      query: `
        SELECT condition_id, market_id, question, slug, outcomes, token_ids
        FROM market_metadata FINAL
        WHERE hasAny(token_ids, {tokenIds:Array(String)})
      `,
      query_params: { tokenIds },
      format: 'JSONEachRow',
    })
    type Row = {
      condition_id: string
      market_id: string
      question: string
      slug: string
      outcomes: string[]
      token_ids: string[]
    }
    const rows = await result.json() as Row[]
    const map = new Map<string, TokenMeta>()
    for (const m of rows) {
      const tids = Array.isArray(m.token_ids) ? m.token_ids : []
      const outs = Array.isArray(m.outcomes) ? m.outcomes : []
      for (let i = 0; i < tids.length; i++) {
        if (!tokenIds.includes(tids[i])) continue
        map.set(tids[i], {
          token_id: tids[i],
          condition_id: m.condition_id,
          market_id: m.market_id,
          question: m.question,
          slug: m.slug,
          outcome: outs[i] ?? `Outcome ${i}`,
          outcome_index: i,
        })
      }
    }
    return map
  }
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

type MarketCategories = {
  condition_id: string
  market_id: string
  event_id: string
  event_title: string
  event_slug: string
  categories: string[]
}

async function getMarketCategoriesMap(conditionIds: string[]): Promise<Map<string, MarketCategories>> {
  if (conditionIds.length === 0) return new Map()
  try {
    const result = await client.query({
      query: `
        SELECT
          condition_id,
          market_id,
          event_id,
          event_title,
          event_slug,
          categories
        FROM market_categories FINAL
        WHERE condition_id IN ({conditionIds:Array(String)})
      `,
      query_params: { conditionIds },
      format: 'JSONEachRow',
    })
    const rows = await result.json() as Array<MarketCategories>
    return new Map(rows.map((r) => [r.condition_id, {
      condition_id: r.condition_id,
      market_id: r.market_id ?? '',
      event_id: r.event_id ?? '',
      event_title: r.event_title ?? '',
      event_slug: r.event_slug ?? '',
      categories: Array.isArray(r.categories) ? r.categories : [],
    }]))
  } catch {
    return new Map()
  }
}

async function getLastPriceMap(tokenIds: string[]): Promise<Map<string, { price: number; timestampMs: number }>> {
  if (tokenIds.length === 0) return new Map()
  const result = await client.query({
    query: `
      SELECT
        token_id,
        argMaxMerge(price_state) AS price,
        toUnixTimestamp64Milli(argMaxMerge(ts_state)) AS ts_ms
      FROM token_last_price
      WHERE token_id IN ({tokenIds:Array(String)})
      GROUP BY token_id
    `,
    query_params: { tokenIds },
    format: 'JSONEachRow',
  })
  const rows = await result.json() as Array<{ token_id: string; price: number; ts_ms: string | number }>
  const map = new Map<string, { price: number; timestampMs: number }>()
  for (const r of rows) {
    map.set(r.token_id, { price: Number(r.price), timestampMs: Number(r.ts_ms ?? 0) })
  }
  return map
}

async function getAvgBuyPriceMap(wallet: string, tokenIds: string[]): Promise<Map<string, number>> {
  if (tokenIds.length === 0) return new Map()
  const result = await client.query({
    query: `
      SELECT
        token_id,
        sum(buy_usd) AS usd,
        sum(buy_shares) AS shares
      FROM wallet_token_buys
      WHERE wallet = {wallet:String}
        AND token_id IN ({tokenIds:Array(String)})
      GROUP BY token_id
    `,
    query_params: { wallet, tokenIds },
    format: 'JSONEachRow',
  })
  const rows = await result.json() as Array<{ token_id: string; usd: number; shares: number }>
  const map = new Map<string, number>()
  for (const r of rows) {
    const shares = Number(r.shares)
    map.set(r.token_id, shares > 0 ? Number(r.usd) / shares : 0)
  }
  return map
}

function round2(n: number): number {
  return Math.round(n * 100) / 100
}

const LEADERBOARD_EXCLUDED_WALLETS = [
  '0x0000000000000000000000000000000000000000',
  '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',
  '0xc5d563a36ae78145c45a50134d48a1215220f80a',
  '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296',
  '0x3a3bd7bb9528e159577f7c2e685cc81a765002e2',
  '0xe3f18acc55091e2c48d883fc8c8413319d4ab7b0',
  '0xb768891e3130f6df18214ac804d4db76c2c37730',
] as const

const LEADERBOARD_EXCLUDED_WALLETS_SQL = LEADERBOARD_EXCLUDED_WALLETS.map((wallet) => `'${wallet}'`).join(', ')
const LEADERBOARD_CASHFLOW_METRIC_ID = 'net_cashflow_usd_v1'
const LEADERBOARD_CASHFLOW_METRIC_DEFINITION = 'sum(sell_usdc) - sum(buy_usdc)'
const LEADERBOARD_PNL_METRIC_ID = 'realized_pnl_usd_v1'
const LEADERBOARD_PNL_METRIC_DEFINITION = 'sum(realized_pnl) from wallet_ledger'
const LEADERBOARD_VALID_PERIODS = ['24h', '7d', '30d', 'all'] as const
type LeaderboardPeriod = (typeof LEADERBOARD_VALID_PERIODS)[number]

type LeaderboardRow = {
  wallet: string
  totalTrades: number
  totalVolume: number
  netCashflow: number
  marketsTraded: number
}

type LeaderboardRealizedPnlRow = {
  wallet: string
  realizedPnl: number
}

type LeaderboardStatsSort = 'netCashflow' | 'volume' | 'trades'
type LeaderboardSort = LeaderboardStatsSort | 'pnl'

function leaderboardPeriodFilter(period: LeaderboardPeriod, column = 'block_timestamp'): string {
  if (period === '24h') return `AND ${column} >= now() - INTERVAL 24 HOUR`
  if (period === '7d') return `AND ${column} >= now() - INTERVAL 7 DAY`
  if (period === '30d') return `AND ${column} >= now() - INTERVAL 30 DAY`
  return ''
}

async function queryLeaderboardFromMaterialized(period: LeaderboardPeriod, sort: LeaderboardStatsSort, limit: number): Promise<LeaderboardRow[]> {
  const sortCol: Record<string, string> = {
    netCashflow: 'netCashflow',
    volume: 'totalVolume',
    trades: 'totalTrades',
  }

  let table = 'wallet_leaderboard_stats_1h'
  let timeFilter = ''

  if (period === 'all') {
    table = 'wallet_leaderboard_stats_all'
  } else {
    const periodHours: Record<string, number> = {
      '24h': 24,
      '7d': 24 * 7,
      '30d': 24 * 30,
    }
    timeFilter = `AND bucket >= toStartOfHour(now() - toIntervalHour(${periodHours[period]}))`
  }

  const result = await client.query({
    query: `
    SELECT
      wallet,
      countMerge(trades_state) AS totalTrades,
      sumMerge(volume_state) AS totalVolume,
      sumMerge(pnl_state) AS netCashflow,
      uniqExactMerge(markets_state) AS marketsTraded
    FROM ${table}
    WHERE wallet NOT IN (${LEADERBOARD_EXCLUDED_WALLETS_SQL})
      ${timeFilter}
    GROUP BY wallet
    HAVING totalTrades >= 5
    ORDER BY ${sortCol[sort]} DESC
    LIMIT {limit:UInt32}
    `,
    query_params: { limit },
    format: 'JSONEachRow',
  })

  return await result.json() as LeaderboardRow[]
}

async function queryLeaderboardFromRaw(period: LeaderboardPeriod, sort: LeaderboardStatsSort, limit: number): Promise<LeaderboardRow[]> {

  const sortCol: Record<string, string> = {
    netCashflow: 'netCashflow',
    volume: 'totalVolume',
    trades: 'totalTrades',
  }

  const result = await client.query({
    query: `
      SELECT
        wallet,
        count() AS totalTrades,
        sum(toFloat64(usdc_amount)) / 1000000 AS totalVolume,
        sum(CASE WHEN side = 'sell' THEN toFloat64(usdc_amount) ELSE -toFloat64(usdc_amount) END) / 1000000 AS netCashflow,
        uniqExact(token_id) AS marketsTraded
      FROM wallet_trades
      WHERE wallet NOT IN (${LEADERBOARD_EXCLUDED_WALLETS_SQL})
        ${leaderboardPeriodFilter(period)}
      GROUP BY wallet
      HAVING totalTrades >= 5
      ORDER BY ${sortCol[sort]} DESC
      LIMIT {limit:UInt32}
    `,
    query_params: { limit },
    format: 'JSONEachRow',
  })

  return await result.json() as LeaderboardRow[]
}

async function queryLeaderboardStatsByWallets(period: LeaderboardPeriod, wallets: string[]): Promise<Map<string, LeaderboardRow>> {
  if (wallets.length === 0) return new Map()

  const result = await client.query({
    query: `
      SELECT
        wallet,
        count() AS totalTrades,
        sum(toFloat64(usdc_amount)) / 1000000 AS totalVolume,
        sum(if(side = 'sell', toFloat64(usdc_amount), -toFloat64(usdc_amount))) / 1000000 AS netCashflow,
        uniqExact(token_id) AS marketsTraded
      FROM wallet_trades
      WHERE wallet IN ({wallets:Array(String)})
        ${leaderboardPeriodFilter(period)}
      GROUP BY wallet
    `,
    query_params: { wallets },
    format: 'JSONEachRow',
  })

  const rows = await result.json() as LeaderboardRow[]
  return new Map(rows.map((row) => [row.wallet, row]))
}

async function queryLeaderboardStatsByWalletsFiltered(
  period: LeaderboardPeriod,
  wallets: string[],
  category: string,
  eventId: string,
): Promise<Map<string, LeaderboardRow>> {
  if (wallets.length === 0) return new Map()

  const result = await client.query({
    query: `
      SELECT
        wt.wallet AS wallet,
        count() AS totalTrades,
        sum(toFloat64(wt.usdc_amount)) / 1000000 AS totalVolume,
        sum(if(wt.side = 'sell', toFloat64(wt.usdc_amount), -toFloat64(wt.usdc_amount))) / 1000000 AS netCashflow,
        uniqExact(tm.condition_id) AS marketsTraded
      FROM wallet_trades wt
      INNER JOIN token_metadata tm ON wt.token_id = tm.token_id
      INNER JOIN market_categories FINAL mc ON tm.condition_id = mc.condition_id
      WHERE wt.wallet IN ({wallets:Array(String)})
        ${leaderboardPeriodFilter(period)}
        AND ({category:String} = '' OR has(ifNull(mc.categories, []), {category:String}))
        AND ({eventId:String} = '' OR mc.event_id = {eventId:String})
      GROUP BY wt.wallet
    `,
    query_params: { wallets, category, eventId },
    format: 'JSONEachRow',
  })

  const rows = await result.json() as LeaderboardRow[]
  return new Map(rows.map((row) => [row.wallet, row]))
}

async function queryLeaderboardRealizedPnl(period: LeaderboardPeriod, limit: number): Promise<LeaderboardRealizedPnlRow[]> {
  const result = await client.query({
    query: `
      SELECT
        wallet,
        sum(realized_pnl) AS realizedPnl
      FROM wallet_ledger FINAL
      WHERE wallet NOT IN (${LEADERBOARD_EXCLUDED_WALLETS_SQL})
        ${leaderboardPeriodFilter(period)}
      GROUP BY wallet
      ORDER BY realizedPnl DESC
      LIMIT {limit:UInt32}
    `,
    query_params: { limit },
    format: 'JSONEachRow',
  })

  return await result.json() as LeaderboardRealizedPnlRow[]
}

async function queryLeaderboardRealizedPnlFiltered(
  period: LeaderboardPeriod,
  limit: number,
  category: string,
  eventId: string,
): Promise<LeaderboardRealizedPnlRow[]> {
  const result = await client.query({
    query: `
      SELECT
        l.wallet AS wallet,
        sum(l.realized_pnl) AS realizedPnl,
        count() AS events
      FROM wallet_ledger FINAL l
      INNER JOIN market_categories FINAL mc ON l.condition_id = mc.condition_id
      WHERE l.wallet NOT IN (${LEADERBOARD_EXCLUDED_WALLETS_SQL})
        ${leaderboardPeriodFilter(period, 'l.block_timestamp')}
        AND ({category:String} = '' OR has(ifNull(mc.categories, []), {category:String}))
        AND ({eventId:String} = '' OR mc.event_id = {eventId:String})
      GROUP BY l.wallet
      HAVING events >= 5
      ORDER BY realizedPnl DESC
      LIMIT {limit:UInt32}
    `,
    query_params: { limit, category, eventId },
    format: 'JSONEachRow',
  })

  return await result.json() as LeaderboardRealizedPnlRow[]
}

async function queryRealizedPnlForWallets(period: LeaderboardPeriod, wallets: string[]): Promise<Map<string, number>> {
  if (wallets.length === 0) return new Map()

  const result = await client.query({
    query: `
      SELECT
        wallet,
        sum(realized_pnl) AS realizedPnl
      FROM wallet_ledger FINAL
      WHERE wallet IN ({wallets:Array(String)})
        ${leaderboardPeriodFilter(period)}
      GROUP BY wallet
    `,
    query_params: { wallets },
    format: 'JSONEachRow',
  })

  const rows = await result.json() as Array<{ wallet: string; realizedPnl: number }>
  return new Map(rows.map((row) => [row.wallet, Number(row.realizedPnl)]))
}

async function queryRealizedPnlForWalletsFiltered(
  period: LeaderboardPeriod,
  wallets: string[],
  category: string,
  eventId: string,
): Promise<Map<string, number>> {
  if (wallets.length === 0) return new Map()

  const result = await client.query({
    query: `
      SELECT
        l.wallet AS wallet,
        sum(l.realized_pnl) AS realizedPnl
      FROM wallet_ledger FINAL l
      INNER JOIN market_categories FINAL mc ON l.condition_id = mc.condition_id
      WHERE l.wallet IN ({wallets:Array(String)})
        ${leaderboardPeriodFilter(period, 'l.block_timestamp')}
        AND ({category:String} = '' OR has(ifNull(mc.categories, []), {category:String}))
        AND ({eventId:String} = '' OR mc.event_id = {eventId:String})
      GROUP BY l.wallet
    `,
    query_params: { wallets, category, eventId },
    format: 'JSONEachRow',
  })

  const rows = await result.json() as Array<{ wallet: string; realizedPnl: number }>
  return new Map(rows.map((row) => [row.wallet, Number(row.realizedPnl)]))
}

async function queryLeaderboardFromRawFiltered(
  period: LeaderboardPeriod,
  sort: LeaderboardStatsSort,
  limit: number,
  category: string,
  eventId: string,
): Promise<LeaderboardRow[]> {
  const sortCol: Record<string, string> = {
    netCashflow: 'netCashflow',
    volume: 'totalVolume',
    trades: 'totalTrades',
  }

  const result = await client.query({
    query: `
      SELECT
        wt.wallet AS wallet,
        count() AS totalTrades,
        sum(toFloat64(wt.usdc_amount)) / 1000000 AS totalVolume,
        sum(CASE WHEN wt.side = 'sell' THEN toFloat64(wt.usdc_amount) ELSE -toFloat64(wt.usdc_amount) END) / 1000000 AS netCashflow,
        uniqExact(tm.condition_id) AS marketsTraded
      FROM wallet_trades wt
      INNER JOIN token_metadata tm ON wt.token_id = tm.token_id
      INNER JOIN market_categories FINAL mc ON tm.condition_id = mc.condition_id
      WHERE wt.wallet NOT IN (${LEADERBOARD_EXCLUDED_WALLETS_SQL})
        ${leaderboardPeriodFilter(period)}
        AND ({category:String} = '' OR has(ifNull(mc.categories, []), {category:String}))
        AND ({eventId:String} = '' OR mc.event_id = {eventId:String})
      GROUP BY wt.wallet
      HAVING totalTrades >= 5
      ORDER BY ${sortCol[sort]} DESC
      LIMIT {limit:UInt32}
    `,
    query_params: { limit, category, eventId },
    format: 'JSONEachRow',
  })

  return await result.json() as LeaderboardRow[]
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

  const metaMap = await getTokenMetaMap(tokenIds)
  const conditionIds = Array.from(new Set(
    tokenIds.map(tid => metaMap.get(tid)?.condition_id).filter(Boolean) as string[],
  ))

  const [avgBuyMap, lastPriceMap, catMap] = await Promise.all([
    getAvgBuyPriceMap(wallet, tokenIds),
    getLastPriceMap(tokenIds),
    getMarketCategoriesMap(conditionIds),
  ])

  // Backward-compatible fallbacks: these aggregate tables may not be backfilled yet on older deployments.
  if (avgBuyMap.size < tokenIds.length) {
    const fallback = await client.query({
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
    const rows = await fallback.json() as Array<{ token_id: string; avg_price: number }>
    for (const r of rows) {
      if (!avgBuyMap.has(r.token_id)) avgBuyMap.set(r.token_id, Number(r.avg_price) || 0)
    }
  }

  if (lastPriceMap.size < tokenIds.length) {
    const fallback = await client.query({
      query: `
        SELECT
          token_id,
          argMax(price_per_token, tuple(block_number, log_index)) AS price,
          toUnixTimestamp64Milli(argMax(block_timestamp, tuple(block_number, log_index))) AS ts_ms
        FROM trades
        WHERE token_id IN ({tokenIds:Array(String)})
        GROUP BY token_id
      `,
      query_params: { tokenIds },
      format: 'JSONEachRow',
    })
    const rows = await fallback.json() as Array<{ token_id: string; price: number; ts_ms: string | number }>
    for (const r of rows) {
      if (!lastPriceMap.has(r.token_id)) lastPriceMap.set(r.token_id, { price: Number(r.price) || 0, timestampMs: Number(r.ts_ms ?? 0) })
    }
  }

  const positions = balances.map((b) => {
    const meta = metaMap.get(b.token_id)
    const size = Number(b.balance) / 1e6

    const avgPrice = avgBuyMap.get(b.token_id) ?? 0
    const last = lastPriceMap.get(b.token_id)
    const currentPrice = last?.price ?? 0

    const initialValue = size * avgPrice
    const currentValue = size * currentPrice
    const unrealizedPnl = size * (currentPrice - avgPrice)

    const condId = meta?.condition_id ?? ''
    const cat = condId ? catMap.get(condId) : undefined

    return {
      asset: b.token_id,
      condition_id: condId,
      market_id: meta?.market_id ?? '',
      event_id: cat?.event_id ?? '',
      categories: cat?.categories ?? [],
      outcome: meta?.outcome ?? '',
      outcome_index: meta?.outcome_index ?? 0,
      question: meta?.question ?? '',
      slug: meta?.slug ?? '',
      size,
      avg_price: round2(avgPrice),
      current_price: round2(currentPrice),
      initial_value: round2(initialValue),
      current_value: round2(currentValue),
      unrealized_pnl: round2(unrealizedPnl),
      price_updated_at_ms: last?.timestampMs ?? 0,
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
    if (bestPnl > -Infinity) bestTrade = { market: findMeta(bestCid)?.question ?? (bestCid || 'Unknown'), conditionId: bestCid, pnl: round2(bestPnl) }
    if (worstPnl < Infinity) worstTrade = { market: findMeta(worstCid)?.question ?? (worstCid || 'Unknown'), conditionId: worstCid, pnl: round2(worstPnl) }
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

  // Perf: polymarket.trades is (currently) ordered by id, not token_id/time.
  // Constrain time by default so the UI doesn't scan years of data to show "recent trades".
  const now = Math.floor(Date.now() / 1000)
  const fromParam = url.searchParams.get('from')
  const toParam = url.searchParams.get('to')

  const defaultLookbackDays = 30
  const fromTsBase = fromParam ? Math.max(Number(fromParam), 0) : (now - defaultLookbackDays * 86400)
  const toTs = toParam ? Math.max(Number(toParam), 0) : now

  async function fetchTrades(fromTs: number) {
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
        PREWHERE token_id = {tokenId:String}
        WHERE block_timestamp >= toDateTime({fromTs:UInt64})
          AND block_timestamp <= toDateTime({toTs:UInt64})
        ORDER BY block_timestamp DESC, log_index DESC
        LIMIT {limit:UInt32}
        OFFSET {offset:UInt32}
      `,
      query_params: { tokenId, fromTs, toTs, limit, offset },
      format: 'JSONEachRow',
    })

    return await result.json() as Array<{
      id: string; value: number; size: number; side: string
      maker: string; taker: string; timestamp: number
      tx_hash: string; block_number: number
    }>
  }

  // If the caller didn't specify a time range and the market is illiquid, widen the window.
  let rows = await fetchTrades(fromTsBase)
  if (!fromParam && rows.length < Math.min(limit, 10)) {
    rows = await fetchTrades(now - 365 * 86400)
  }

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
      json(res, 200, {
        conditionId, uniqueTraders: 0, totalTrades: 0, onChainVolume: 0,
        volume24h: 0, volume7d: 0, avgTradeSize: 0, largestTrade: 0,
        lastTradeAt: 0, holderCount: 0, topHolders: [],
      })
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
  const sort = url.searchParams.get('sort') || 'netCashflow'
  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 20), 1), 100)
  const period = (url.searchParams.get('period') || 'all') as LeaderboardPeriod
  const category = (url.searchParams.get('category') || '').trim()
  const eventId = (url.searchParams.get('eventId') || '').trim()
  const isFiltered = category.length > 0 || eventId.length > 0

  const validSorts = ['netCashflow', 'pnl', 'volume', 'trades']
  if (!validSorts.includes(sort)) {
    json(res, 400, { error: 'Invalid sort. Use: netCashflow, pnl, volume, trades' })
    return
  }
  if (!LEADERBOARD_VALID_PERIODS.includes(period)) {
    json(res, 400, { error: 'Invalid period. Use: 24h, 7d, 30d, all' })
    return
  }

  if (sort === 'pnl') {
    const pnlRows = isFiltered
      ? await queryLeaderboardRealizedPnlFiltered(period, limit, category, eventId)
      : await queryLeaderboardRealizedPnl(period, limit)
    const wallets = pnlRows.map((row) => row.wallet)
    const statsByWallet = isFiltered
      ? await queryLeaderboardStatsByWalletsFiltered(period, wallets, category, eventId)
      : await queryLeaderboardStatsByWallets(period, wallets)

    json(res, 200, {
      period,
      sort,
      sortNormalized: 'pnl',
      filters: {
        category: category || null,
        eventId: eventId || null,
      },
      updatedAt: Math.floor(Date.now() / 1000),
      metric: {
        id: LEADERBOARD_PNL_METRIC_ID,
        label: 'Realized PnL (USD)',
        isPnl: true,
        formula: LEADERBOARD_PNL_METRIC_DEFINITION,
        valueField: 'realizedPnlUsd',
        notes: [
          'Realized PnL is read from wallet_ledger and only includes wallets processed by the ledger/snapshot jobs.',
          'Net cashflow is still returned for each row as a separate field.',
        ],
      },
      traders: pnlRows.map((row, i) => {
        const stats = statsByWallet.get(row.wallet)
        return {
          rank: i + 1,
          user: row.wallet,
          realizedPnlUsd: round2(Number(row.realizedPnl)),
          netCashflowUsd: round2(Number(stats?.netCashflow ?? 0)),
          totalPnl: round2(Number(stats?.netCashflow ?? 0)),
          totalVolume: round2(Number(stats?.totalVolume ?? 0)),
          totalTrades: Number(stats?.totalTrades ?? 0),
          winRate: null,
          marketsTraded: Number(stats?.marketsTraded ?? 0),
        }
      }),
    })
    return
  }

  const normalizedSort = sort as LeaderboardStatsSort

  let rows: LeaderboardRow[] = []
  if (isFiltered) {
    rows = await queryLeaderboardFromRawFiltered(period, normalizedSort, limit, category, eventId)
  } else {
    try {
      rows = await queryLeaderboardFromMaterialized(period, normalizedSort, limit)
      if (rows.length === 0) {
        throw new Error('materialized leaderboard returned no rows')
      }
    } catch (error) {
      console.warn('[leaderboard] materialized query failed; falling back to raw query', error)
      rows = await queryLeaderboardFromRaw(period, normalizedSort, limit)
    }
  }

  const realizedPnlByWallet = isFiltered
    ? await queryRealizedPnlForWalletsFiltered(period, rows.map((row) => row.wallet), category, eventId)
    : await queryRealizedPnlForWallets(period, rows.map((row) => row.wallet))
  const realizedCoverage = rows.length > 0
    ? round2((rows.filter((row) => realizedPnlByWallet.has(row.wallet)).length / rows.length) * 100)
    : 0

  json(res, 200, {
    period,
    sort,
    sortNormalized: normalizedSort,
    filters: {
      category: category || null,
      eventId: eventId || null,
    },
    updatedAt: Math.floor(Date.now() / 1000),
    metric: {
      id: LEADERBOARD_CASHFLOW_METRIC_ID,
      label: 'Net Cashflow (USD)',
      isPnl: false,
      formula: LEADERBOARD_CASHFLOW_METRIC_DEFINITION,
      valueField: 'netCashflowUsd',
      availablePnlMetric: LEADERBOARD_PNL_METRIC_ID,
      notes: [
        `Realized PnL coverage in this response: ${realizedCoverage}% of rows.`,
        'Legacy field totalPnl is retained for compatibility and equals netCashflowUsd.',
      ],
    },
    traders: rows.map((r, i) => ({
      rank: i + 1,
      user: r.wallet,
      realizedPnlUsd: realizedPnlByWallet.has(r.wallet)
        ? round2(Number(realizedPnlByWallet.get(r.wallet)))
        : null,
      netCashflowUsd: round2(Number(r.netCashflow)),
      totalPnl: round2(Number(r.netCashflow)),
      totalVolume: round2(Number(r.totalVolume)),
      totalTrades: Number(r.totalTrades),
      winRate: null,
      marketsTraded: Number(r.marketsTraded),
    })),
  })
}

async function handleLeaderboardExplain(url: URL, res: ServerResponse) {
  const user = url.searchParams.get('user')
  if (!user || !isValidAddress(user)) {
    json(res, 400, { error: 'Missing or invalid user address' })
    return
  }

  const wallet = user.toLowerCase()
  const period = (url.searchParams.get('period') || 'all') as LeaderboardPeriod
  if (!LEADERBOARD_VALID_PERIODS.includes(period)) {
    json(res, 400, { error: 'Invalid period. Use: 24h, 7d, 30d, all' })
    return
  }

  const metric = url.searchParams.get('metric') || 'netCashflow'
  if (!['netCashflow', 'pnl'].includes(metric)) {
    json(res, 400, { error: 'Invalid metric. Use: netCashflow or pnl' })
    return
  }

  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 1000), 1), 10000)

  if (metric === 'pnl') {
    const summaryResult = await client.query({
      query: `
        SELECT
          count() AS totalEvents,
          sum(realized_pnl) AS realizedPnl,
          sum(usdc_delta) AS cashflow,
          uniqExact(token_id) AS marketsTraded
        FROM wallet_ledger FINAL
        WHERE wallet = {wallet:String}
          ${leaderboardPeriodFilter(period)}
      `,
      query_params: { wallet },
      format: 'JSONEachRow',
    })

    const summaryRow = (await summaryResult.json() as Array<{
      totalEvents: number
      realizedPnl: number
      cashflow: number
      marketsTraded: number
    }>)[0]

    const detailsResult = await client.query({
      query: `
        SELECT
          id,
          tx_hash,
          block_timestamp,
          token_id,
          condition_id,
          event_type,
          quantity,
          usdc_delta,
          cost_basis,
          realized_pnl
        FROM wallet_ledger FINAL
        WHERE wallet = {wallet:String}
          ${leaderboardPeriodFilter(period)}
        ORDER BY block_timestamp ASC, id ASC
        LIMIT {limit:UInt32}
      `,
      query_params: { wallet, limit },
      format: 'JSONEachRow',
    })

    const rows = await detailsResult.json() as Array<{
      id: string
      tx_hash: string
      block_timestamp: string
      token_id: string
      condition_id: string
      event_type: string
      quantity: number
      usdc_delta: number
      cost_basis: number
      realized_pnl: number
    }>

    let runningRealized = 0
    const events = rows.map((row) => {
      runningRealized += Number(row.realized_pnl)
      return {
        eventId: row.id,
        txHash: row.tx_hash || row.id.split('-')[0],
        blockTimestamp: row.block_timestamp,
        blockTimestampUnix: Math.floor(new Date(row.block_timestamp).getTime() / 1000),
        tokenId: row.token_id,
        conditionId: row.condition_id,
        eventType: row.event_type,
        quantity: round2(Number(row.quantity)),
        usdcDeltaUsd: round2(Number(row.usdc_delta)),
        costBasisUsd: round2(Number(row.cost_basis)),
        realizedPnlUsd: round2(Number(row.realized_pnl)),
        runningRealizedPnlUsd: round2(runningRealized),
      }
    })

    json(res, 200, {
      user: wallet,
      period,
      metric: {
        id: LEADERBOARD_PNL_METRIC_ID,
        label: 'Realized PnL (USD)',
        isPnl: true,
        formula: LEADERBOARD_PNL_METRIC_DEFINITION,
        notes: [
          'PnL explain uses wallet_ledger events.',
          'If this wallet has not been processed by ledger jobs, events may be empty.',
        ],
      },
      summary: {
        totalEvents: Number(summaryRow?.totalEvents ?? 0),
        realizedPnlUsd: round2(Number(summaryRow?.realizedPnl ?? 0)),
        cashflowUsd: round2(Number(summaryRow?.cashflow ?? 0)),
        marketsTraded: Number(summaryRow?.marketsTraded ?? 0),
        eventCountReturned: events.length,
        eventLimit: limit,
      },
      events,
    })
    return
  }

  const summaryResult = await client.query({
    query: `
      SELECT
        count() AS totalTrades,
        sum(toFloat64(usdc_amount)) / 1000000 AS totalVolume,
        sum(if(side = 'sell', toFloat64(usdc_amount), -toFloat64(usdc_amount))) / 1000000 AS netCashflow,
        uniqExact(token_id) AS marketsTraded
      FROM wallet_trades
      WHERE wallet = {wallet:String}
        ${leaderboardPeriodFilter(period)}
    `,
    query_params: { wallet },
    format: 'JSONEachRow',
  })

  const summaryRow = (await summaryResult.json() as Array<{
    totalTrades: number
    totalVolume: number
    netCashflow: number
    marketsTraded: number
  }>)[0]

  const detailsResult = await client.query({
    query: `
      SELECT
        id,
        block_timestamp,
        token_id,
        side,
        toFloat64(usdc_amount) / 1000000 AS usdc_amount,
        if(side = 'sell', toFloat64(usdc_amount), -toFloat64(usdc_amount)) / 1000000 AS signed_usdc
      FROM wallet_trades
      WHERE wallet = {wallet:String}
        ${leaderboardPeriodFilter(period)}
      ORDER BY block_timestamp ASC, id ASC
      LIMIT {limit:UInt32}
    `,
    query_params: { wallet, limit },
    format: 'JSONEachRow',
  })

  const rows = await detailsResult.json() as Array<{
    id: string
    block_timestamp: string
    token_id: string
    side: 'buy' | 'sell'
    usdc_amount: number
    signed_usdc: number
  }>

  let runningNetCashflow = 0
  const events = rows.map((row) => {
    runningNetCashflow += Number(row.signed_usdc)
    const txHash = row.id.split('-')[0]
    return {
      eventId: row.id,
      txHash,
      blockTimestamp: row.block_timestamp,
      blockTimestampUnix: Math.floor(new Date(row.block_timestamp).getTime() / 1000),
      tokenId: row.token_id,
      side: row.side,
      usdcAmount: round2(Number(row.usdc_amount)),
      signedCashflowUsd: round2(Number(row.signed_usdc)),
      runningNetCashflowUsd: round2(runningNetCashflow),
    }
  })

  json(res, 200, {
    user: wallet,
    period,
    metric: {
      id: LEADERBOARD_CASHFLOW_METRIC_ID,
      label: 'Net Cashflow (USD)',
      isPnl: false,
      formula: LEADERBOARD_CASHFLOW_METRIC_DEFINITION,
      notes: [
        'Positive signed cashflow is sell-side USDC inflow.',
        'Negative signed cashflow is buy-side USDC outflow.',
      ],
    },
    summary: {
      totalTrades: Number(summaryRow?.totalTrades ?? 0),
      totalVolumeUsd: round2(Number(summaryRow?.totalVolume ?? 0)),
      netCashflowUsd: round2(Number(summaryRow?.netCashflow ?? 0)),
      marketsTraded: Number(summaryRow?.marketsTraded ?? 0),
      eventCountReturned: events.length,
      eventLimit: limit,
    },
    events,
  })
}

// ── NEW: Market Candles (OHLCV) ─────────────────────────────────────

async function handleMarketCandles(url: URL, res: ServerResponse) {
  const conditionId = url.searchParams.get('conditionId')
  const tokenIdParam = url.searchParams.get('tokenId')

  if (!conditionId && !tokenIdParam) {
    json(res, 400, { error: 'Missing conditionId or tokenId parameter' })
    return
  }

  const validIntervals = ['1m', '5m', '15m', '1h', '4h', '1d', '1w']
  const interval = url.searchParams.get('interval') || '1h'
  if (!validIntervals.includes(interval)) {
    json(res, 400, { error: 'Invalid interval. Use: 1m, 5m, 15m, 1h, 4h, 1d, 1w' })
    return
  }

  const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 500), 1), 5000)

  // Resolve tokenId
  let tokenId = tokenIdParam || ''
  let resolvedConditionId = conditionId || ''

  if (!tokenId) {
    const tokenIds = await getTokenIdsForCondition(conditionId!)
    if (tokenIds.length === 0) {
      json(res, 400, { error: 'No tokens found for this conditionId' })
      return
    }
    tokenId = tokenIds[0] // first outcome (YES)
  }

  if (!resolvedConditionId && tokenId) {
    const metaMap = await getTokenMetaMap([tokenId])
    resolvedConditionId = metaMap.get(tokenId)?.condition_id ?? ''
  }

  // Default time ranges based on interval
  const now = Math.floor(Date.now() / 1000)
  const defaultFrom: Record<string, number> = {
    '1m': now - 24 * 3600,
    '5m': now - 48 * 3600,
    '15m': now - 7 * 86400,
    '1h': now - 7 * 86400,
    '4h': now - 30 * 86400,
    '1d': now - 90 * 86400,
    '1w': now - 365 * 86400,
  }

  const fromTs = Number(url.searchParams.get('from') || defaultFrom[interval])
  const toTs = Number(url.searchParams.get('to') || now)

  // Query pre-aggregated candles_1m table instead of raw trades
  let query: string
  if (interval === '1m') {
    // Direct read from 1-minute candles
    query = `
      SELECT
        toUnixTimestamp(time) AS time,
        argMinMerge(open) AS open,
        maxMerge(high) AS high,
        minMerge(low) AS low,
        argMaxMerge(close) AS close,
        sumMerge(volume) AS volume,
        countMerge(trades) AS trades
      FROM candles_1m
      WHERE token_id = {tokenId:String}
        AND time >= toDateTime({fromTs:UInt64})
        AND time <= toDateTime({toTs:UInt64})
      GROUP BY time
      ORDER BY time ASC
      LIMIT {limit:UInt32}
    `
  } else {
    // Re-aggregate 1m candles into larger buckets
    const intervalSql: Record<string, string> = {
      '5m': 'INTERVAL 5 MINUTE',
      '15m': 'INTERVAL 15 MINUTE',
      '1h': 'INTERVAL 1 HOUR',
      '4h': 'INTERVAL 4 HOUR',
      '1d': 'INTERVAL 1 DAY',
      '1w': 'INTERVAL 1 WEEK',
    }
    query = `
      SELECT
        toUnixTimestamp(bucket) AS time,
        argMin(open_price, t) AS open,
        max(high_price) AS high,
        min(low_price) AS low,
        argMax(close_price, t) AS close,
        sum(vol) AS volume,
        sum(trade_count) AS trades
      FROM (
        SELECT
          time AS t,
          toStartOfInterval(time, ${intervalSql[interval]}) AS bucket,
          argMinMerge(open) AS open_price,
          maxMerge(high) AS high_price,
          minMerge(low) AS low_price,
          argMaxMerge(close) AS close_price,
          sumMerge(volume) AS vol,
          countMerge(trades) AS trade_count
        FROM candles_1m
        WHERE token_id = {tokenId:String}
          AND time >= toDateTime({fromTs:UInt64})
          AND time <= toDateTime({toTs:UInt64})
        GROUP BY time
      )
      GROUP BY bucket
      ORDER BY bucket ASC
      LIMIT {limit:UInt32}
    `
  }

  const result = await client.query({
    query,
    query_params: { tokenId, fromTs, toTs, limit },
    format: 'JSONEachRow',
  })

  const candles = await result.json() as Array<{
    time: number; open: number; high: number; low: number; close: number; volume: number; trades: number
  }>

	  json(res, 200, {
	    conditionId: resolvedConditionId,
	    tokenId,
	    interval,
	    candles: candles.map(c => ({
	      time: c.time,
	      // Do not round prices here; rounding can turn real red/green candles into dojis.
	      open: c.open,
	      high: c.high,
	      low: c.low,
	      close: c.close,
	      volume: round2(c.volume),
	      trades: Number(c.trades),
	    })),
	  })
	}

// ── Discovery (Trending/Volume Windows) ──────────────────────────────
//
// Frontend expects an array (or `{data: [...]}`), not a nested `{markets: ...}` object.
// This implementation uses existing prod tables: `candles_1m` + `market_metadata`.
// Category/event filters are accepted but not enforced until taxonomy is synced into ClickHouse.

async function handleDiscoverMarkets(url: URL, res: ServerResponse) {
  const window = (url.searchParams.get('window') || '24h').trim()
  const limitRaw = Number(url.searchParams.get('limit') || 20)
  const offsetRaw = Number(url.searchParams.get('offset') || 0)

  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(200, Math.floor(limitRaw))) : 20
  const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.floor(offsetRaw)) : 0

  const intervalSql: Record<string, string> = {
    '1h': 'INTERVAL 1 HOUR',
    '3h': 'INTERVAL 3 HOUR',
    '6h': 'INTERVAL 6 HOUR',
    '12h': 'INTERVAL 12 HOUR',
    '24h': 'INTERVAL 24 HOUR',
    '7d': 'INTERVAL 7 DAY',
  }

  if (!intervalSql[window]) {
    json(res, 400, { error: `Invalid window. Use one of: ${Object.keys(intervalSql).join(', ')}` })
    return
  }

  const result = await client.query({
    query: `
      SELECT
        condition_id AS marketId,
        any(question) AS question,
        any(outcomes) AS outcomes,
        arrayMap(x -> x.2, arraySort(groupArray((idx, price)))) AS outcomePrices,
        sum(ifNull(vol, 0.0)) AS volumeUsd
      FROM (
        SELECT
          condition_id,
          question,
          outcomes,
          token_id,
          idx
        FROM market_metadata FINAL
        ARRAY JOIN token_ids AS token_id, arrayEnumerate(token_ids) AS idx
      ) mm
      LEFT JOIN (
        SELECT
          token_id,
          argMaxMerge(close) AS price
        FROM candles_1m
        WHERE time >= now() - INTERVAL 14 DAY
        GROUP BY token_id
      ) p USING (token_id)
      LEFT JOIN (
        SELECT
          token_id,
          sumMerge(volume) AS vol
        FROM candles_1m
        WHERE time >= now() - ${intervalSql[window]}
        GROUP BY token_id
      ) v USING (token_id)
      GROUP BY condition_id
      ORDER BY volumeUsd DESC
      LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    `,
    query_params: { limit, offset },
    format: 'JSONEachRow',
  })

  const rows = await result.json()
  json(res, 200, rows)
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
    if (pathname === '/market/candles') { await handleMarketCandles(url, res); return }
    if (pathname === '/discover/markets') { await handleDiscoverMarkets(url, res); return }
    if (pathname === '/leaderboard/explain') { await handleLeaderboardExplain(url, res); return }
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
