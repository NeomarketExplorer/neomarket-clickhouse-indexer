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

const STATUS_PROCESSOR_ID = 'market_categories_sync'

const ch = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
})

function toChDateTime(value: string | null | undefined): string | null {
  if (!value) return null
  const d = new Date(value)
  if (!Number.isFinite(d.getTime())) return null
  const pad = (n: number) => String(n).padStart(2, '0')
  // ClickHouse parses "YYYY-MM-DD HH:MM:SS" reliably.
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`
}

type MarketCategoriesRow = {
  condition_id: string
  market_id: string
  event_id: string | null
  event_title: string | null
  event_slug: string | null
  categories: string[]
  updated_at: string | null
}

async function fetchBatch(offset: number, limit: number, since: string | null): Promise<MarketCategoriesRow[]> {
  const params = new URLSearchParams({ offset: String(offset), limit: String(limit) })
  if (since) params.set('since', since)
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

async function getLastSince(): Promise<string | null> {
  try {
    const result = await ch.query({
      query: `
        SELECT formatDateTime(last_timestamp, '%FT%TZ') AS last_timestamp
        FROM indexer_status FINAL
        WHERE processor_id = {id:String}
        ORDER BY updated_at DESC
        LIMIT 1
      `,
      query_params: { id: STATUS_PROCESSOR_ID },
      format: 'JSONEachRow',
    })
    const rows = await result.json() as Array<{ last_timestamp: string }>
    const ts = rows[0]?.last_timestamp?.trim()
    return ts ? ts : null
  } catch {
    return null
  }
}

async function setLastSince(iso: string): Promise<void> {
  const ts = toChDateTime(iso)
  if (!ts) return
  await ch.insert({
    table: 'indexer_status',
    values: [{
      processor_id: STATUS_PROCESSOR_ID,
      last_block: 0,
      last_timestamp: ts,
    }],
    format: 'JSONEachRow',
  })
}

async function syncOnce(): Promise<number> {
  const since = await getLastSince()
  console.log(`🔄 Syncing indexer categories → ClickHouse market_categories...${since ? ` (since ${since})` : ''}`)
  const start = Date.now()

  // Keep batches small: ClickHouse HTTP has strict max form-field sizes and
  // large param arrays can trip "Field value too long".
  const BATCH = 200
  let offset = 0
  let total = 0
  let maxUpdatedAtIso: string | null = null
  let lastCheckpointMs = 0

  while (true) {
    const rows = await fetchBatch(offset, BATCH, since)
    if (rows.length === 0) break

    // Track checkpoint.
    for (const r of rows) {
      if (!r.updated_at) continue
      if (!maxUpdatedAtIso) maxUpdatedAtIso = r.updated_at
      else if (new Date(r.updated_at).getTime() > new Date(maxUpdatedAtIso).getTime()) maxUpdatedAtIso = r.updated_at
    }

    const incoming = rows
      .filter(r => r.condition_id)
      .map(r => {
        const categories = Array.isArray(r.categories) ? r.categories.filter(Boolean).map(String) : []
        categories.sort()
        return {
          condition_id: r.condition_id,
          market_id: r.market_id ?? '',
          event_id: r.event_id ?? '',
          event_title: r.event_title ?? '',
          event_slug: r.event_slug ?? '',
          categories,
          updated_at_iso: r.updated_at,
        }
      })

    // Change detection: only write rows that actually changed vs current.
    const conditionIds = incoming.map(r => r.condition_id)
    const existingResult = await ch.query({
      query: `
        SELECT
          condition_id,
          market_id,
          event_id,
          event_title,
          event_slug,
          categories,
          toString(updated_at) AS updated_at
        FROM market_categories FINAL
        WHERE condition_id IN ({ids:Array(String)})
      `,
      query_params: { ids: conditionIds },
      format: 'JSONEachRow',
    })
    const existingRows = await existingResult.json() as Array<{
      condition_id: string
      market_id: string
      event_id: string
      event_title: string
      event_slug: string
      categories: string[]
      updated_at: string
    }>
    const existingById = new Map(existingRows.map(r => {
      const cats = Array.isArray(r.categories) ? r.categories.filter(Boolean).map(String).sort() : []
      return [r.condition_id, { ...r, categories: cats }]
    }))

    const changed = incoming.filter(r => {
      const ex = existingById.get(r.condition_id)
      if (!ex) return true
      if ((ex.market_id ?? '') !== (r.market_id ?? '')) return true
      if ((ex.event_id ?? '') !== (r.event_id ?? '')) return true
      if ((ex.event_title ?? '') !== (r.event_title ?? '')) return true
      if ((ex.event_slug ?? '') !== (r.event_slug ?? '')) return true
      const exCats = Array.isArray(ex.categories) ? ex.categories : []
      if (exCats.length !== r.categories.length) return true
      for (let i = 0; i < exCats.length; i++) {
        if (exCats[i] !== r.categories[i]) return true
      }
      return false
    })

    const currentValues = changed.map(r => ({
      condition_id: r.condition_id,
      market_id: r.market_id,
      event_id: r.event_id,
      event_title: r.event_title,
      event_slug: r.event_slug,
      categories: r.categories,
      updated_at: toChDateTime(r.updated_at_iso) ?? undefined,
    }))

    const historyValues = changed
      .map(r => {
        const validFrom = toChDateTime(r.updated_at_iso) ?? toChDateTime(new Date().toISOString())
        if (!validFrom) return null
        const version = r.updated_at_iso ? Math.floor(new Date(r.updated_at_iso).getTime() / 1000) : Math.floor(Date.now() / 1000)
        return {
          condition_id: r.condition_id,
          market_id: r.market_id,
          event_id: r.event_id,
          event_title: r.event_title,
          event_slug: r.event_slug,
          categories: r.categories,
          primary_category: r.categories[0] ?? '',
          valid_from: validFrom,
          version,
          updated_at: validFrom,
        }
      })
      .filter((v): v is NonNullable<typeof v> => Boolean(v))

    if (currentValues.length > 0) {
      await ch.insert({
        table: 'market_categories',
        values: currentValues,
        format: 'JSONEachRow',
      })
      total += currentValues.length
    }

    if (historyValues.length > 0) {
      await ch.insert({
        table: 'market_categories_history',
        values: historyValues,
        format: 'JSONEachRow',
      })
    }

    // Persist a checkpoint periodically so we don't restart from scratch if the
    // container is restarted mid-sync.
    if (maxUpdatedAtIso && (Date.now() - lastCheckpointMs) > 30_000) {
      await setLastSince(maxUpdatedAtIso)
      lastCheckpointMs = Date.now()
    }

    offset += rows.length
    if (rows.length < BATCH) break
    await new Promise(r => setTimeout(r, 50))
  }

  const sec = ((Date.now() - start) / 1000).toFixed(1)
  console.log(`✅ Synced ${total} rows in ${sec}s`)

  if (maxUpdatedAtIso) {
    await setLastSince(maxUpdatedAtIso)
  }

  return total
}

async function main() {
  const loop = process.argv.includes('--loop')
  if (loop) {
    console.log(`🔁 Looping every ${Math.round(SYNC_INTERVAL_MS / 1000)}s`)
    // Never crash the container in loop mode; log and retry.
    await syncOnce().catch((err) => console.error('Sync failed:', err))
    setInterval(() => {
      syncOnce().catch((err) => console.error('Sync failed:', err))
    }, SYNC_INTERVAL_MS)
    return
  }

  await syncOnce()
  await ch.close()
  process.exit(0)
}

main().catch(err => {
  console.error('Fatal:', err)
  process.exit(1)
})
