/**
 * Sync market metadata from the neomarket-indexer PostgreSQL
 * into ClickHouse market_metadata table.
 *
 * Usage:
 *   npm run sync:metadata           # one-shot sync
 *   npm run sync:metadata -- --loop # sync every 5 minutes
 */

import { createClient as createClickHouseClient } from '@clickhouse/client'
import 'dotenv/config'

const POSTGRES_CONTAINER = 'postgres-tswcc4sko4sg8s00sgs8gwos-093551802429'
const PG_HOST = process.env.PG_HOST || POSTGRES_CONTAINER
const PG_PORT = Number(process.env.PG_PORT || 5432)
const PG_USER = process.env.PG_USER || 'postgres'
const PG_PASSWORD = process.env.PG_PASSWORD || 'postgres'
const PG_DATABASE = process.env.PG_DATABASE || 'polymarket'

const BATCH_SIZE = 5000
const SYNC_INTERVAL_MS = 5 * 60 * 1000 // 5 minutes

const ch = createClickHouseClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
})

interface MarketRow {
  condition_id: string
  market_id: string
  question: string
  slug: string
  outcomes: string[]
  token_ids: string[]
  neg_risk: boolean
}

/**
 * Query Postgres via its HTTP interface isn't available,
 * so we use a TCP connection with the built-in pg module-less approach.
 * We'll use fetch against a simple query endpoint, or if that's not available,
 * shell out to psql.
 *
 * Simplest reliable approach: use node's net module to speak the pg wire protocol...
 * Actually, let's just use the pg npm package. It's one dependency.
 */

async function queryPostgres(query: string): Promise<any[]> {
  // Dynamic import so this only loads when sync-metadata runs
  const pg = await import('pg')
  const client = new pg.default.Client({
    host: PG_HOST,
    port: PG_PORT,
    user: PG_USER,
    password: PG_PASSWORD,
    database: PG_DATABASE,
  })
  await client.connect()
  try {
    const result = await client.query(query)
    return result.rows
  } finally {
    await client.end()
  }
}

async function syncMetadata(): Promise<number> {
  console.log('🔄 Syncing market metadata from PostgreSQL → ClickHouse...')
  const startTime = Date.now()

  let offset = 0
  let totalSynced = 0

  while (true) {
    const rows: MarketRow[] = await queryPostgres(`
      SELECT
        condition_id,
        id AS market_id,
        question,
        COALESCE(slug, '') AS slug,
        outcomes,
        outcome_token_ids AS token_ids,
        false AS neg_risk
      FROM markets
      WHERE condition_id IS NOT NULL
        AND condition_id != ''
      ORDER BY id
      LIMIT ${BATCH_SIZE} OFFSET ${offset}
    `)

    if (rows.length === 0) break

    const values = rows.map(row => ({
      condition_id: row.condition_id,
      market_id: row.market_id,
      question: row.question,
      slug: row.slug,
      outcomes: Array.isArray(row.outcomes) ? row.outcomes : [],
      token_ids: Array.isArray(row.token_ids) ? row.token_ids : [],
      neg_risk: row.neg_risk,
    }))

    await ch.insert({
      table: 'market_metadata',
      values,
      format: 'JSONEachRow',
    })

    totalSynced += rows.length
    offset += rows.length

    if (totalSynced % 50000 === 0) {
      console.log(`  ... synced ${totalSynced} markets`)
    }

    if (rows.length < BATCH_SIZE) break
  }

  const durationSec = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log(`✅ Synced ${totalSynced} markets in ${durationSec}s`)
  return totalSynced
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
