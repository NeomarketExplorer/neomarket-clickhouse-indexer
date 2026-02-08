/**
 * Sync market metadata from Gamma API into ClickHouse market_metadata table.
 * No cross-container dependencies — hits Gamma API directly.
 *
 * Usage:
 *   npm run sync:metadata           # one-shot sync
 *   npm run sync:metadata -- --loop # sync every 5 minutes
 */

import { createClient } from '@clickhouse/client'
import 'dotenv/config'

const GAMMA_API = process.env.GAMMA_API_URL || 'https://gamma-api.polymarket.com'
const BATCH_SIZE = 100 // Gamma API max per request
const SYNC_INTERVAL_MS = 5 * 60 * 1000 // 5 minutes

const ch = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
})

interface GammaMarket {
  id: string
  conditionId: string
  question: string
  slug?: string
  outcomes: string // JSON string like '["Yes","No"]'
  clobTokenIds?: string // JSON string like '["123","456"]'
  active?: boolean
  closed?: boolean
}

function parseJsonArray(val: string | undefined | null): string[] {
  if (!val) return []
  try {
    const parsed = JSON.parse(val)
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

async function fetchGammaMarkets(offset: number, closed: boolean): Promise<GammaMarket[]> {
  const params = new URLSearchParams({
    limit: String(BATCH_SIZE),
    offset: String(offset),
    closed: String(closed),
  })
  const url = `${GAMMA_API}/markets?${params}`
  const res = await fetch(url)
  if (!res.ok) {
    throw new Error(`Gamma API error: ${res.status} ${res.statusText}`)
  }
  return res.json() as Promise<GammaMarket[]>
}

async function syncMarkets(closed: boolean): Promise<number> {
  let offset = 0
  let totalSynced = 0

  while (true) {
    const markets = await fetchGammaMarkets(offset, closed)
    if (markets.length === 0) break

    const values = markets
      .filter(m => m.conditionId)
      .map(m => ({
        condition_id: m.conditionId,
        market_id: m.id,
        question: m.question,
        slug: m.slug ?? '',
        outcomes: parseJsonArray(m.outcomes),
        token_ids: parseJsonArray(m.clobTokenIds),
        neg_risk: false,
      }))

    if (values.length > 0) {
      await ch.insert({
        table: 'market_metadata',
        values,
        format: 'JSONEachRow',
      })
    }

    totalSynced += values.length
    offset += markets.length

    if (totalSynced % 5000 === 0 && totalSynced > 0) {
      console.log(`  ... synced ${totalSynced} markets (closed=${closed})`)
    }

    if (markets.length < BATCH_SIZE) break

    // Rate limit: 50ms between requests
    await new Promise(r => setTimeout(r, 50))
  }

  return totalSynced
}

async function syncMetadata(): Promise<number> {
  console.log('🔄 Syncing market metadata from Gamma API → ClickHouse...')
  const startTime = Date.now()

  // Sync open markets (changes frequently)
  const openCount = await syncMarkets(false)

  // Sync closed markets (only on first run or if very few in DB)
  const countResult = await ch.query({
    query: 'SELECT count() AS c FROM market_metadata',
    format: 'JSONEachRow',
  })
  const [{ c }] = await countResult.json() as [{ c: string }]
  let closedCount = 0
  if (Number(c) < 10000) {
    // First sync or very few markets — also sync closed ones
    console.log('  ... also syncing closed markets (first run)')
    closedCount = await syncMarkets(true)
  }

  const total = openCount + closedCount
  const durationSec = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log(`✅ Synced ${total} markets (${openCount} open, ${closedCount} closed) in ${durationSec}s`)
  return total
}

async function main() {
  const loop = process.argv.includes('--loop')

  await syncMetadata()

  if (loop) {
    console.log(`🔁 Looping every ${SYNC_INTERVAL_MS / 1000}s`)
    setInterval(async () => {
      try {
        await syncMetadata()
      } catch (err) {
        console.error('Sync failed:', err)
      }
    }, SYNC_INTERVAL_MS)
  } else {
    await ch.close()
    process.exit(0)
  }
}

main().catch(err => {
  console.error('Fatal:', err)
  process.exit(1)
})
