import 'dotenv/config'
import { writeFileSync } from 'node:fs'

type LocalLeaderboardRow = {
  rank: number
  user: string
  netCashflowUsd: number
  totalVolume: number
  totalTrades: number
}

type PolymarketLeaderboardRow = {
  rank: number
  wallet: string
  pnl: number
  volume: number
  userName: string
}

type UpstreamStatus = {
  name: string
  url: string
  ok: boolean
  latencyMs: number
  detail: string
}

type AuditReport = {
  generatedAt: string
  params: Record<string, string | number | boolean>
  upstreams: UpstreamStatus[]
  local?: {
    count: number
    source: string
  }
  polymarket: {
    count: number
    source: string
  }
  comparison?: {
    compareTopN: number
    overlapCount: number
    overlapRatio: number
    top10Overlap: number
    jaccard: number
    meanAbsoluteRankDelta: number
    maxAbsoluteRankDelta: number
    sampleMismatches: Array<{
      wallet: string
      localRank: number
      polymarketRank: number
      rankDelta: number
      localPnl: number
      polymarketPnl: number
      pnlDelta: number
      localVolume: number
      polymarketVolume: number
      volumeDelta: number
    }>
  }
  status: 'pass' | 'warn' | 'fail'
  notes: string[]
}

type ComparisonSummary = NonNullable<AuditReport['comparison']>

function getArg(name: string, fallback: string): string {
  const idx = process.argv.indexOf(`--${name}`)
  if (idx === -1 || idx + 1 >= process.argv.length) return fallback
  return process.argv[idx + 1]
}

function getNumberArg(name: string, fallback: number): number {
  const raw = getArg(name, String(fallback))
  const n = Number(raw)
  return Number.isFinite(n) ? n : fallback
}

function getBoolArg(name: string): boolean {
  return process.argv.includes(`--${name}`)
}

function round(n: number, digits = 4): number {
  const m = 10 ** digits
  return Math.round(n * m) / m
}

async function timedFetch(url: string, timeoutMs: number, init?: RequestInit): Promise<{ response: Response; latencyMs: number }> {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)
  const started = Date.now()
  try {
    const response = await fetch(url, {
      ...init,
      signal: controller.signal,
    })
    return { response, latencyMs: Date.now() - started }
  } finally {
    clearTimeout(timeout)
  }
}

async function checkUpstreams(timeoutMs: number): Promise<UpstreamStatus[]> {
  const checks: Array<() => Promise<UpstreamStatus>> = [
    async () => {
      const url = 'https://data-api.polymarket.com/v1/leaderboard?timeframe=ALL&sortBy=PNL&order=DESC&page=1&pageSize=1'
      try {
        const { response, latencyMs } = await timedFetch(url, timeoutMs)
        if (!response.ok) {
          return { name: 'data-api', url, ok: false, latencyMs, detail: `HTTP ${response.status}` }
        }
        const body = await response.json() as Array<{ proxyWallet?: string }>
        const first = body[0]?.proxyWallet?.toLowerCase() || ''
        const ok = /^0x[a-f0-9]{40}$/.test(first)
        return { name: 'data-api', url, ok, latencyMs, detail: ok ? 'leaderboard reachable' : 'unexpected payload' }
      } catch (error) {
        return { name: 'data-api', url, ok: false, latencyMs: -1, detail: String(error) }
      }
    },
    async () => {
      const url = 'https://gamma-api.polymarket.com/markets?limit=1'
      try {
        const { response, latencyMs } = await timedFetch(url, timeoutMs)
        if (!response.ok) {
          return { name: 'gamma-api', url, ok: false, latencyMs, detail: `HTTP ${response.status}` }
        }
        const body = await response.json() as Array<{ conditionId?: string }>
        const ok = typeof body[0]?.conditionId === 'string'
        return { name: 'gamma-api', url, ok, latencyMs, detail: ok ? 'markets reachable' : 'unexpected payload' }
      } catch (error) {
        return { name: 'gamma-api', url, ok: false, latencyMs: -1, detail: String(error) }
      }
    },
    async () => {
      const url = 'https://clob.polymarket.com/time'
      try {
        const { response, latencyMs } = await timedFetch(url, timeoutMs)
        if (!response.ok) {
          return { name: 'clob-api', url, ok: false, latencyMs, detail: `HTTP ${response.status}` }
        }
        const body = await response.text()
        const unixSeconds = Number(body.trim())
        const ok = Number.isFinite(unixSeconds) && unixSeconds > 1_700_000_000
        return { name: 'clob-api', url, ok, latencyMs, detail: ok ? 'time endpoint reachable' : 'unexpected payload' }
      } catch (error) {
        return { name: 'clob-api', url, ok: false, latencyMs: -1, detail: String(error) }
      }
    },
  ]

  return Promise.all(checks.map((run) => run()))
}

async function fetchLocalLeaderboard(baseUrl: string, period: string, sort: string, limit: number, timeoutMs: number): Promise<LocalLeaderboardRow[]> {
  const query = new URL('/leaderboard', baseUrl)
  query.searchParams.set('period', period)
  query.searchParams.set('sort', sort)
  query.searchParams.set('limit', String(limit))

  const { response } = await timedFetch(query.toString(), timeoutMs)
  if (!response.ok) {
    const body = await response.text()
    throw new Error(`Local leaderboard request failed (${response.status}): ${body}`)
  }

  const data = await response.json() as { traders?: Array<{
    rank: number
    user: string
    netCashflowUsd?: number
    totalPnl: number
    totalVolume: number
    totalTrades: number
  }> }

  return (data.traders || []).map((row) => ({
    rank: Number(row.rank),
    user: String(row.user).toLowerCase(),
    netCashflowUsd: Number(row.netCashflowUsd ?? row.totalPnl ?? 0),
    totalVolume: Number(row.totalVolume),
    totalTrades: Number(row.totalTrades),
  }))
}

async function fetchPolymarketLeaderboard(timeframe: string, sortBy: string, target: number, timeoutMs: number): Promise<PolymarketLeaderboardRow[]> {
  const rows: PolymarketLeaderboardRow[] = []
  const seen = new Set<string>()

  for (let page = 1; page <= 20 && rows.length < target; page++) {
    const url = new URL('https://data-api.polymarket.com/v1/leaderboard')
    url.searchParams.set('timeframe', timeframe)
    url.searchParams.set('sortBy', sortBy)
    url.searchParams.set('order', 'DESC')
    url.searchParams.set('page', String(page))
    url.searchParams.set('pageSize', '25')

    const { response } = await timedFetch(url.toString(), timeoutMs)
    if (!response.ok) {
      throw new Error(`Polymarket leaderboard request failed (${response.status}) on page ${page}`)
    }

    const body = await response.json() as Array<{
      rank: string
      proxyWallet: string
      pnl: number
      vol: number
      userName: string
    }>

    if (body.length === 0) break

    let addedThisPage = 0
    for (const row of body) {
      const wallet = row.proxyWallet.toLowerCase()
      if (seen.has(wallet)) continue
      seen.add(wallet)
      addedThisPage++
      rows.push({
        rank: Number(row.rank),
        wallet,
        pnl: Number(row.pnl),
        volume: Number(row.vol),
        userName: row.userName || '',
      })
      if (rows.length >= target) break
    }

    if (addedThisPage === 0) break
  }

  return rows
}

function compareLeaderboards(localRows: LocalLeaderboardRow[], pmRows: PolymarketLeaderboardRow[], compareTopN: number) {
  const localTop = localRows.slice(0, compareTopN)
  const pmTop = pmRows.slice(0, compareTopN)

  const localByWallet = new Map(localTop.map((row) => [row.user, row]))
  const pmByWallet = new Map(pmTop.map((row) => [row.wallet, row]))

  const overlapWallets = [...localByWallet.keys()].filter((wallet) => pmByWallet.has(wallet))
  const overlapCount = overlapWallets.length
  const overlapRatio = localTop.length > 0 ? overlapCount / localTop.length : 0

  const union = new Set([...localByWallet.keys(), ...pmByWallet.keys()]).size
  const jaccard = union > 0 ? overlapCount / union : 0

  const rankDeltas: number[] = []
  const sampleMismatches: ComparisonSummary['sampleMismatches'] = []

  for (const wallet of overlapWallets.slice(0, 20)) {
    const local = localByWallet.get(wallet)!
    const pm = pmByWallet.get(wallet)!
    const rankDelta = Math.abs(local.rank - pm.rank)
    rankDeltas.push(rankDelta)

    sampleMismatches.push({
      wallet,
      localRank: local.rank,
      polymarketRank: pm.rank,
      rankDelta,
      localPnl: round(local.netCashflowUsd, 2),
      polymarketPnl: round(pm.pnl, 2),
      pnlDelta: round(local.netCashflowUsd - pm.pnl, 2),
      localVolume: round(local.totalVolume, 2),
      polymarketVolume: round(pm.volume, 2),
      volumeDelta: round(local.totalVolume - pm.volume, 2),
    })
  }

  const top10Local = new Set(localRows.slice(0, 10).map((row) => row.user))
  const top10Pm = new Set(pmRows.slice(0, 10).map((row) => row.wallet))
  const top10Overlap = [...top10Local].filter((wallet) => top10Pm.has(wallet)).length

  const meanAbsoluteRankDelta = rankDeltas.length > 0
    ? rankDeltas.reduce((sum, value) => sum + value, 0) / rankDeltas.length
    : 0

  const maxAbsoluteRankDelta = rankDeltas.length > 0 ? Math.max(...rankDeltas) : 0

  return {
    compareTopN,
    overlapCount,
    overlapRatio: round(overlapRatio),
    top10Overlap,
    jaccard: round(jaccard),
    meanAbsoluteRankDelta: round(meanAbsoluteRankDelta),
    maxAbsoluteRankDelta,
    sampleMismatches,
  }
}

async function main() {
  const localBase = getArg('local-base', process.env.LOCAL_LEADERBOARD_API || 'http://localhost:3002')
  const localPeriod = getArg('local-period', 'all')
  const localSort = getArg('local-sort', 'netCashflow')
  const pmTimeframe = getArg('pm-timeframe', 'ALL')
  const pmSortBy = getArg('pm-sort', 'PNL')
  const limit = Math.max(1, Math.min(getNumberArg('limit', 100), 500))
  const compareTopN = Math.max(1, Math.min(getNumberArg('compare-top', 50), limit))
  const minOverlap = Math.max(0, Math.min(getNumberArg('min-overlap', 0.2), 1))
  const skipLocal = getBoolArg('skip-local')
  const strict = getBoolArg('strict')
  const reportFile = getArg('report-file', '')
  const timeoutMs = Math.max(1_000, getNumberArg('timeout-ms', 30_000))

  const upstreams = await checkUpstreams(timeoutMs)
  const notes: string[] = []

  let localRows: LocalLeaderboardRow[] | undefined
  let polymarketRows: PolymarketLeaderboardRow[] = []

  polymarketRows = await fetchPolymarketLeaderboard(pmTimeframe, pmSortBy, limit, timeoutMs)
  if (polymarketRows.length === 0) {
    throw new Error('Polymarket leaderboard returned no rows')
  }

  if (!skipLocal) {
    localRows = await fetchLocalLeaderboard(localBase, localPeriod, localSort, limit, timeoutMs)
    if (localRows.length === 0) {
      notes.push('Local leaderboard returned zero rows')
    }
  }

  let comparison: AuditReport['comparison']
  if (localRows) {
    comparison = compareLeaderboards(localRows, polymarketRows, Math.min(compareTopN, localRows.length, polymarketRows.length))
  }

  const allUpstreamsOk = upstreams.every((status) => status.ok)
  let status: AuditReport['status'] = 'pass'
  if (!allUpstreamsOk) {
    status = 'warn'
    notes.push('One or more upstream Polymarket APIs are unhealthy')
  }

  if (!skipLocal && !localRows) {
    status = 'fail'
    notes.push('Local leaderboard could not be fetched')
  }

  if (comparison) {
    if (comparison.overlapRatio < minOverlap) {
      status = 'fail'
      notes.push(`Overlap ratio ${comparison.overlapRatio} is below threshold ${minOverlap}`)
    } else if (comparison.top10Overlap < 2) {
      status = status === 'fail' ? 'fail' : 'warn'
      notes.push('Top-10 overlap is low (<2)')
    }
  }

  const report: AuditReport = {
    generatedAt: new Date().toISOString(),
    params: {
      localBase,
      localPeriod,
      localSort,
      pmTimeframe,
      pmSortBy,
      limit,
      compareTopN,
      minOverlap,
      skipLocal,
      strict,
      timeoutMs,
    },
    upstreams,
    ...(localRows ? { local: { count: localRows.length, source: `${localBase}/leaderboard` } } : {}),
    polymarket: {
      count: polymarketRows.length,
      source: 'https://data-api.polymarket.com/v1/leaderboard',
    },
    ...(comparison ? { comparison } : {}),
    status,
    notes,
  }

  if (reportFile) {
    writeFileSync(reportFile, JSON.stringify(report, null, 2))
  }

  console.log(JSON.stringify(report, null, 2))

  if (strict && status !== 'pass') {
    process.exitCode = 1
  }
}

main().catch((error) => {
  console.error('[leaderboard-audit-agent] failed:', error)
  process.exit(1)
})
