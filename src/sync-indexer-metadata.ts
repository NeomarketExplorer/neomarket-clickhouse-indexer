/**
 * Sync event/category metadata from the Postgres indexer into ClickHouse.
 *
 * This allows ClickHouse leaderboards/discovery queries to join against the
 * curated categories taxonomy without requiring cross-database joins.
 *
 * Usage:
 *   npm run sync:indexer-metadata
 *   npm run sync:indexer-metadata -- --loop
 */

import { createClient } from '@clickhouse/client'
import 'dotenv/config'

const INDEXER_API = process.env.INDEXER_API_URL || 'http://localhost:3001'
const INDEXER_TOKEN = process.env.INDEXER_INTERNAL_TOKEN || ''
const SYNC_INTERVAL_MS = 10 * 60 * 1000 // 10 minutes

const ch = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
})

type MarketCategoriesRow = {
  condition_id: string
  market_id: string
  event_id: string | null
  event_title: string | null
  event_slug: string | null
  categories: string[]
  updated_at: string | null
}

async function fetchBatch(offset: number, limit: number): Promise<MarketCategoriesRow[]> {
  const params = new URLSearchParams({ offset: String(offset), limit: String(limit) })
  const url = `${INDEXER_API}/internal/export/market-categories?${params}`
  const res = await fetch(url, {
    headers: INDEXER_TOKEN ? { 'x-internal-token': INDEXER_TOKEN } : undefined,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`indexer export error: HTTP ${res.status} ${res.statusText} ${text}`.trim())
  }
  const payload = await res.json() as { data: MarketCategoriesRow[] }
  return payload.data ?? []
}

async function syncOnce(): Promise<number> {
  console.log('🔄 Syncing indexer categories → ClickHouse market_categories...')
  const start = Date.now()

  const BATCH = 2000
  let offset = 0
  let total = 0

  while (true) {
    const rows = await fetchBatch(offset, BATCH)
    if (rows.length === 0) break

    const values = rows
      .filter(r => r.condition_id)
      .map(r => ({
        condition_id: r.condition_id,
        market_id: r.market_id ?? '',
        event_id: r.event_id ?? '',
        event_title: r.event_title ?? '',
        event_slug: r.event_slug ?? '',
        categories: Array.isArray(r.categories) ? r.categories : [],
      }))

    if (values.length > 0) {
      await ch.insert({
        table: 'market_categories',
        values,
        format: 'JSONEachRow',
      })
      total += values.length
    }

    offset += rows.length
    if (rows.length < BATCH) break
    await new Promise(r => setTimeout(r, 50))
  }

  const sec = ((Date.now() - start) / 1000).toFixed(1)
  console.log(`✅ Synced ${total} rows in ${sec}s`)
  return total
}

async function main() {
  const loop = process.argv.includes('--loop')
  await syncOnce()

  if (loop) {
    console.log(`🔁 Looping every ${Math.round(SYNC_INTERVAL_MS / 1000)}s`)
    setInterval(() => {
      syncOnce().catch((err) => console.error('Sync failed:', err))
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

