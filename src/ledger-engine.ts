import { createClient } from '@clickhouse/client'
import 'dotenv/config'
import { solidityPackedKeccak256 } from 'ethers'
import {
  USDC_SCALE,
  TOKEN_SCALE,
  USDC_ADDRESS,
  NEGRISK_ADAPTER,
  NEGRISK_WRAPPED_COLLATERAL,
  CTF_EXCHANGE_BINARY,
  CTF_EXCHANGE_MULTI,
  toTokenNumber,
  toUsdcNumber,
} from './constants.js'

const ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'
const ZERO_BYTES32 = '0x' + '0'.repeat(64)
const TOKEN_PER_USDC = TOKEN_SCALE / USDC_SCALE

export enum PnlMode {
  REALIZED_PERIOD_ONLY = 1,
  REALIZED_WITH_HISTORY = 2,
  REALIZED_PERIOD_PLUS_UNREALIZED = 3,
  TOTAL_PNL = 4,
}

export interface PnlResult {
  mode: PnlMode
  wallet: string
  periodStart: Date
  periodEnd: Date

  // Realized PnL components
  realizedFromSells: number
  realizedFromRedemptions: number
  realizedFromMerges: number
  realizedFromResolutionLosses: number
  realizedFromFees: number
  totalRealized: number

  // Unrealized (if mode 3 or 4)
  unrealizedPnl: number
  openPositionsCost: number
  openPositionsValue: number

  // Combined
  totalPnl: number

  // Stats
  totalBuyCost: number
  totalBuyTokens: number
  totalSellProceeds: number
  totalSellTokens: number
  tradeCount: number
  redemptionCount: number
  mergeCount: number
}

interface Lot {
  quantity: number
  unitCost: number
  timestamp: number
}

interface LotConsumption {
  quantity: number
  unitCost: number
  timestamp: number
}

class PositionManager {
  private positions = new Map<string, Lot[]>()

  addTokens(tokenId: string, quantity: number, unitCost: number, timestamp: number): void {
    if (!this.positions.has(tokenId)) {
      this.positions.set(tokenId, [])
    }
    const lots = this.positions.get(tokenId)!
    lots.push({ quantity, unitCost, timestamp })
  }

  consumeTokens(tokenId: string, quantity: number): { costBasis: number; consumptions: LotConsumption[] } {
    const lots = this.positions.get(tokenId)
    if (!lots || lots.length === 0) {
      return { costBasis: 0, consumptions: [] }
    }

    let remaining = quantity
    let costBasis = 0
    const consumptions: LotConsumption[] = []

    while (remaining > 0.0000001 && lots.length > 0) {
      const lot = lots[0]
      const take = Math.min(remaining, lot.quantity)
      const cost = take * lot.unitCost

      costBasis += cost
      consumptions.push({ quantity: take, unitCost: lot.unitCost, timestamp: lot.timestamp })

      lot.quantity -= take
      remaining -= take

      if (lot.quantity <= 0.0000001) {
        lots.shift()
      }
    }

    return { costBasis, consumptions }
  }

  getOpenPositions(): Map<string, Lot[]> {
    return new Map(Array.from(this.positions.entries()).filter(([_, lots]) => lots.length > 0))
  }

  getOpenPositionsCost(filter?: { startTs?: number; endTs?: number }): number {
    let total = 0
    for (const lots of this.positions.values()) {
      for (const lot of lots) {
        if (!filter || isLotInRange(lot, filter)) {
          total += lot.quantity * lot.unitCost
        }
      }
    }
    return total
  }

  getOpenPositionsValue(lastPrices: Map<string, number>, filter?: { startTs?: number; endTs?: number }): number {
    let total = 0
    for (const [tokenId, lots] of this.positions) {
      const price = lastPrices.get(tokenId) || 0
      if (price === 0) continue
      for (const lot of lots) {
        if (!filter || isLotInRange(lot, filter)) {
          total += lot.quantity * price
        }
      }
    }
    return total
  }

  getTotalQuantity(tokenId: string): number {
    const lots = this.positions.get(tokenId)
    if (!lots) return 0
    return lots.reduce((sum, lot) => sum + lot.quantity, 0)
  }

  getAverageUnitCost(tokenId: string): number {
    const lots = this.positions.get(tokenId)
    if (!lots || lots.length === 0) return 0
    let totalQty = 0
    let totalCost = 0
    for (const lot of lots) {
      totalQty += lot.quantity
      totalCost += lot.quantity * lot.unitCost
    }
    return totalQty > 0 ? totalCost / totalQty : 0
  }
}

interface TradeRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  maker: string
  taker: string
  maker_asset_id: string
  taker_asset_id: string
  maker_amount: string
  taker_amount: string
  fee: string
  is_maker_buy: boolean
  is_taker_buy: boolean
  token_id: string
  usdc_amount: string
  token_amount: string
}

interface SplitRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  stakeholder: string
  collateral_token: string
  parent_collection_id: string
  condition_id: string
  partition: any
  amount: string
}

interface MergeRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  stakeholder: string
  condition_id: string
  amount: string
}

interface RedemptionRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  redeemer: string
  condition_id: string
  index_sets: any
  payout: string
}

interface TransferRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  operator: string
  from: string
  to: string
  token_id: string
  value: string
}

interface AdapterSplitRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  stakeholder: string
  condition_id: string
  amount: string
}

interface AdapterMergeRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  stakeholder: string
  condition_id: string
  amount: string
}

interface AdapterRedemptionRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  redeemer: string
  condition_id: string
  amounts: any
  payout: string
}

interface AdapterConversionRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  stakeholder: string
  market_id: string
  index_set: string
  amount: string
}

interface FeeRefundRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  module: string
  order_hash: string
  to: string
  token_id: string
  refund: string
  fee_charged: string
}

interface FeeWithdrawalRow {
  id: string
  tx_hash: string
  log_index: number
  block_number: string
  block_timestamp: string
  module: string
  token: string
  to: string
  token_id: string
  amount: string
}

interface ConditionRow {
  condition_id: string
  oracle: string
  outcome_slot_count: number
  payout_numerators: any
  payout_denominator: string
  resolved_at: string
  is_resolved: number
  height: string
}

interface ConditionMetaRow {
  condition_id: string
  parent_collection_id: string
  collateral_token: string
}

interface NegRiskMarketRow {
  market_id: string
  question_count: number
}

interface ConditionInfo {
  conditionId: string
  outcomeSlotCount: number
  payoutNumerators: bigint[]
  payoutDenominator: bigint
  resolvedAt: number
  resolvedBlock: number
  parentCollectionId: string
  collateralToken: string
  tokenIds: string[]
}

interface LedgerEntry {
  id: string
  wallet: string
  event_type: string
  tx_hash: string
  log_index: number
  block_number: number
  block_timestamp: Date
  token_id: string
  condition_id: string
  quantity: number
  usdc_delta: number
  unit_price: number
  cost_basis: number
  realized_pnl: number
  entry_timestamp: Date
  metadata: string
}

interface RealizedEvent {
  type: string
  timestamp: number
  entryTimestamp?: number
  tokenId: string
  proceeds: number
  costBasis: number
  realizedPnl: number
}

interface LedgerBuildResult {
  ledgerEntries: LedgerEntry[]
  realizedEvents: RealizedEvent[]
  positions: PositionManager
  lastPrices: Map<string, number>
  snapshots: any[]
  stats: {
    tradeCount: number
    redemptionCount: number
    mergeCount: number
    totalBuyCost: number
    totalBuyTokens: number
    totalSellProceeds: number
    totalSellTokens: number
  }
}

const client = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
})

async function query<T>(sql: string): Promise<T[]> {
  const result = await client.query({ query: sql, format: 'JSONEachRow' })
  return result.json() as Promise<T[]>
}

function parseArray(value: any): string[] {
  if (!value) return []
  if (Array.isArray(value)) return value.map((v) => v.toString())
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return []
    try {
      const parsed = JSON.parse(trimmed)
      if (Array.isArray(parsed)) return parsed.map((v) => v.toString())
      return []
    } catch {
      return []
    }
  }
  return []
}

function toTs(value: string): number {
  return Math.floor(new Date(value).getTime() / 1000)
}

function toBlockNumber(value: string): number {
  return Number(value)
}

function isLotInRange(lot: Lot, range: { startTs?: number; endTs?: number }): boolean {
  if (range.startTs && lot.timestamp < range.startTs) return false
  if (range.endTs && lot.timestamp > range.endTs) return false
  return true
}

function weightedEntryTimestamp(consumptions: LotConsumption[]): number | undefined {
  if (consumptions.length === 0) return undefined
  let totalQty = 0
  let weighted = 0
  for (const c of consumptions) {
    totalQty += c.quantity
    weighted += c.quantity * c.timestamp
  }
  if (totalQty === 0) return undefined
  return weighted / totalQty
}

function computeCollectionId(parentCollectionId: string, conditionId: string, indexSet: bigint): string {
  return solidityPackedKeccak256(
    ['bytes32', 'bytes32', 'uint256'],
    [parentCollectionId, conditionId, indexSet]
  )
}

function computePositionId(collateralToken: string, collectionId: string): string {
  const hex = solidityPackedKeccak256(['address', 'bytes32'], [collateralToken, collectionId])
  return BigInt(hex).toString()
}

function computeTokenIds(conditionId: string, outcomeCount: number, parentCollectionId: string, collateralToken: string): string[] {
  const tokenIds: string[] = []
  for (let i = 0; i < outcomeCount; i++) {
    const indexSet = 1n << BigInt(i)
    const collectionId = computeCollectionId(parentCollectionId, conditionId, indexSet)
    tokenIds.push(computePositionId(collateralToken, collectionId))
  }
  return tokenIds
}

function indexSetContains(indexSet: bigint, bitIndex: number): boolean {
  if (bitIndex < 0) return false
  return (indexSet & (1n << BigInt(bitIndex))) !== 0n
}

function computeNegRiskQuestionId(marketId: string, questionIndex: number): string {
  const normalized = normalizeBytes32(marketId)
  const bytes = Buffer.from(normalized.slice(2), 'hex')
  bytes[31] = questionIndex & 0xff
  return `0x${bytes.toString('hex')}`
}

function computeConditionIdFromQuestion(oracle: string, questionId: string, outcomeSlotCount: number): string {
  return solidityPackedKeccak256(
    ['address', 'bytes32', 'uint256'],
    [oracle, questionId, BigInt(outcomeSlotCount)]
  )
}

function computeNegRiskTokenIds(marketId: string, questionCount: number): Array<{ yes: string; no: string }> {
  const results: Array<{ yes: string; no: string }> = []
  for (let i = 0; i < questionCount; i++) {
    const questionId = computeNegRiskQuestionId(marketId, i)
    const conditionId = computeConditionIdFromQuestion(NEGRISK_ADAPTER, questionId, 2)
    const tokens = computeTokenIds(conditionId, 2, ZERO_BYTES32, NEGRISK_WRAPPED_COLLATERAL)
    results.push({ yes: tokens[0], no: tokens[1] })
  }
  return results
}

function normalizeBytes32(value: string | null | undefined): string {
  if (!value) return ZERO_BYTES32
  if (value.startsWith('0x') && value.length === 66) return value
  if (value.startsWith('0x') && value.length < 66) {
    return '0x' + value.slice(2).padStart(64, '0')
  }
  return ZERO_BYTES32
}

function normalizeAddress(value: string | null | undefined): string {
  if (!value) return ZERO_ADDRESS
  if (value.startsWith('0x')) return value.toLowerCase()
  return ZERO_ADDRESS
}

async function getTrades(wallet: string, endTs?: number): Promise<TradeRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM trades FINAL
    WHERE (maker = '${w}' OR taker = '${w}')
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<TradeRow>(sql)
}

async function getSplits(wallet: string, endTs?: number): Promise<SplitRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM splits FINAL
    WHERE stakeholder = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<SplitRow>(sql)
}

async function getMerges(wallet: string, endTs?: number): Promise<MergeRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM merges FINAL
    WHERE stakeholder = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<MergeRow>(sql)
}

async function getRedemptions(wallet: string, endTs?: number): Promise<RedemptionRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM redemptions FINAL
    WHERE redeemer = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<RedemptionRow>(sql)
}

async function getTransfers(wallet: string, endTs?: number): Promise<TransferRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM transfers FINAL
    WHERE (from = '${w}' OR to = '${w}')
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<TransferRow>(sql)
}

async function getAdapterSplits(wallet: string, endTs?: number): Promise<AdapterSplitRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM adapter_splits FINAL
    WHERE stakeholder = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<AdapterSplitRow>(sql)
}

async function getAdapterMerges(wallet: string, endTs?: number): Promise<AdapterMergeRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM adapter_merges FINAL
    WHERE stakeholder = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<AdapterMergeRow>(sql)
}

async function getAdapterRedemptions(wallet: string, endTs?: number): Promise<AdapterRedemptionRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM adapter_redemptions FINAL
    WHERE redeemer = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<AdapterRedemptionRow>(sql)
}

async function getAdapterConversions(wallet: string, endTs?: number): Promise<AdapterConversionRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM adapter_conversions FINAL
    WHERE stakeholder = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<AdapterConversionRow>(sql)
}

async function getFeeRefunds(wallet: string, endTs?: number): Promise<FeeRefundRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM fee_refunds FINAL
    WHERE to = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<FeeRefundRow>(sql)
}

async function getFeeWithdrawals(wallet: string, endTs?: number): Promise<FeeWithdrawalRow[]> {
  const w = wallet.toLowerCase()
  const timeFilter = endTs ? `AND block_timestamp <= toDateTime64(${endTs}, 3)` : ''
  const sql = `
    SELECT *
    FROM fee_withdrawals FINAL
    WHERE to = '${w}'
    ${timeFilter}
    ORDER BY block_timestamp ASC, log_index ASC
  `
  return query<FeeWithdrawalRow>(sql)
}

async function getAllConditions(): Promise<ConditionRow[]> {
  const sql = `
    SELECT condition_id, oracle, outcome_slot_count, payout_numerators, payout_denominator, resolved_at, is_resolved, height
    FROM conditions FINAL
  `
  return query<ConditionRow>(sql)
}

async function getConditionMeta(): Promise<ConditionMetaRow[]> {
  const sql = `
    SELECT
      condition_id,
      argMin(parent_collection_id, block_timestamp) as parent_collection_id,
      argMin(collateral_token, block_timestamp) as collateral_token
    FROM (
      SELECT condition_id, parent_collection_id, collateral_token, block_timestamp
      FROM splits FINAL
      UNION ALL
      SELECT condition_id, parent_collection_id, collateral_token, block_timestamp
      FROM merges FINAL
      UNION ALL
      SELECT condition_id, parent_collection_id, collateral_token, block_timestamp
      FROM redemptions FINAL
    )
    GROUP BY condition_id
  `
  return query<ConditionMetaRow>(sql)
}

async function getNegRiskMarkets(): Promise<NegRiskMarketRow[]> {
  const sql = `
    SELECT market_id, max(question_count) as question_count
    FROM neg_risk_markets FINAL
    GROUP BY market_id
  `
  return query<NegRiskMarketRow>(sql)
}

function buildTransferMap(transfers: TransferRow[]): Map<string, TransferRow[]> {
  const map = new Map<string, TransferRow[]>()
  for (const transfer of transfers) {
    if (!map.has(transfer.tx_hash)) map.set(transfer.tx_hash, [])
    map.get(transfer.tx_hash)!.push(transfer)
  }
  return map
}

function selectTransfers(
  transfersByTx: Map<string, TransferRow[]>,
  txHash: string,
  filter: (t: TransferRow) => boolean
): TransferRow[] {
  const list = transfersByTx.get(txHash) || []
  return list.filter(filter)
}

function splitTokenAmountsFromTransfers(transfers: TransferRow[]): Map<string, number> {
  const map = new Map<string, number>()
  for (const t of transfers) {
    const qty = toTokenNumber(BigInt(t.value))
    const prev = map.get(t.token_id) || 0
    map.set(t.token_id, prev + qty)
  }
  return map
}

function computeFallbackTokenAmounts(
  condition: ConditionInfo | undefined,
  indexSets: bigint[] | null,
  usdcAmount: bigint
): Map<string, number> {
  const map = new Map<string, number>()
  if (!condition || condition.tokenIds.length === 0) return map
  const outcomeCount = condition.tokenIds.length
  const amountTokens = toTokenNumber(usdcAmount * TOKEN_PER_USDC)
  const sets = indexSets && indexSets.length > 0
    ? indexSets
    : Array.from({ length: outcomeCount }, (_, i) => 1n << BigInt(i))
  for (const indexSet of sets) {
    const outcomeIndex = indexSetToOutcomeIndex(indexSet, outcomeCount)
    if (outcomeIndex === null) continue
    const tokenId = condition.tokenIds[outcomeIndex]
    map.set(tokenId, (map.get(tokenId) || 0) + amountTokens)
  }
  return map
}

function indexSetToOutcomeIndex(indexSet: bigint, outcomeCount: number): number | null {
  for (let i = 0; i < outcomeCount; i++) {
    if (indexSet === (1n << BigInt(i))) return i
  }
  return null
}

function computePayoutRatios(condition: ConditionInfo | undefined): number[] {
  if (!condition || condition.payoutDenominator === 0n) return []
  return condition.payoutNumerators.map((n) => Number(n) / Number(condition.payoutDenominator))
}

function createLedgerEntry(params: {
  id: string
  wallet: string
  event_type: string
  tx_hash: string
  log_index: number
  block_number: number
  block_timestamp: Date
  token_id?: string
  condition_id?: string
  quantity?: number
  usdc_delta?: number
  unit_price?: number
  cost_basis?: number
  realized_pnl?: number
  entry_timestamp?: Date
  metadata?: object
}): LedgerEntry {
  return {
    id: params.id,
    wallet: params.wallet,
    event_type: params.event_type,
    tx_hash: params.tx_hash,
    log_index: params.log_index,
    block_number: params.block_number,
    block_timestamp: params.block_timestamp,
    token_id: params.token_id || '',
    condition_id: params.condition_id || '',
    quantity: params.quantity ?? 0,
    usdc_delta: params.usdc_delta ?? 0,
    unit_price: params.unit_price ?? 0,
    cost_basis: params.cost_basis ?? 0,
    realized_pnl: params.realized_pnl ?? 0,
    entry_timestamp: params.entry_timestamp ?? new Date(0),
    metadata: params.metadata ? JSON.stringify(params.metadata) : '',
  }
}

async function buildConditionInfo(endTs?: number): Promise<Map<string, ConditionInfo>> {
  const [conditions, metaRows] = await Promise.all([
    getAllConditions(),
    getConditionMeta(),
  ])

  const metaMap = new Map<string, ConditionMetaRow>()
  for (const row of metaRows) {
    metaMap.set(row.condition_id, row)
  }

  const conditionMap = new Map<string, ConditionInfo>()
  for (const row of conditions) {
    const meta = metaMap.get(row.condition_id)
    const parentCollectionId = normalizeBytes32(meta?.parent_collection_id || ZERO_BYTES32)
    const oracle = normalizeAddress(row.oracle)
    const collateralToken = normalizeAddress(
      meta?.collateral_token ||
        (oracle === NEGRISK_ADAPTER.toLowerCase() ? NEGRISK_WRAPPED_COLLATERAL : USDC_ADDRESS)
    )

    const resolvedAtRaw = row.is_resolved ? toTs(row.resolved_at) : 0
    const resolvedBlockRaw = row.is_resolved ? Number(row.height) : 0
    const resolvedAt = endTs && resolvedAtRaw > endTs ? 0 : resolvedAtRaw
    const resolvedBlock = resolvedAt > 0 ? resolvedBlockRaw : 0

    const payoutNumerators = resolvedAt > 0
      ? parseArray(row.payout_numerators).map((v) => BigInt(v))
      : []
    const payoutDenominator = resolvedAt > 0 ? BigInt(row.payout_denominator || '0') : 0n
    const outcomeSlotCount = Number(row.outcome_slot_count)

    const tokenIds = computeTokenIds(row.condition_id, outcomeSlotCount, parentCollectionId, collateralToken)
    conditionMap.set(row.condition_id, {
      conditionId: row.condition_id,
      outcomeSlotCount,
      payoutNumerators,
      payoutDenominator,
      resolvedAt,
      resolvedBlock,
      parentCollectionId,
      collateralToken,
      tokenIds,
    })
  }

  return conditionMap
}

async function buildLedger(
  wallet: string,
  endTs?: number,
  snapshotConfig?: { intervalSeconds: number; startTs?: number; endTs?: number }
): Promise<LedgerBuildResult> {
  const w = wallet.toLowerCase()

  const [
    trades,
    splits,
    merges,
    redemptions,
    transfers,
    adapterSplits,
    adapterMerges,
    adapterRedemptions,
    adapterConversions,
    feeRefunds,
    feeWithdrawals,
    conditionMap,
    negRiskMarkets,
  ] = await Promise.all([
    getTrades(w, endTs),
    getSplits(w, endTs),
    getMerges(w, endTs),
    getRedemptions(w, endTs),
    getTransfers(w, endTs),
    getAdapterSplits(w, endTs),
    getAdapterMerges(w, endTs),
    getAdapterRedemptions(w, endTs),
    getAdapterConversions(w, endTs),
    getFeeRefunds(w, endTs),
    getFeeWithdrawals(w, endTs),
    buildConditionInfo(endTs),
    getNegRiskMarkets(),
  ])

  const transfersByTx = buildTransferMap(transfers)
  const negRiskMarketMap = new Map<string, number>()
  for (const row of negRiskMarkets) {
    negRiskMarketMap.set(row.market_id, Number(row.question_count))
  }
  const positions = new PositionManager()
  const lastPrices = new Map<string, number>()
  const ledgerEntries: LedgerEntry[] = []
  const snapshots: any[] = []
  const realizedEvents: RealizedEvent[] = []

  let tradeCount = 0
  let redemptionCount = 0
  let mergeCount = 0
  let totalBuyCost = 0
  let totalBuyTokens = 0
  let totalSellProceeds = 0
  let totalSellTokens = 0

  type Event = { type: string; ts: number; logIndex: number; blockNumber: number; data: any }
  const events: Event[] = []
  const tradeTxs = new Set<string>()
  const transferSkipTxs = new Set<string>()

  for (const trade of trades) {
    tradeTxs.add(trade.tx_hash)
    events.push({
      type: 'trade',
      ts: toTs(trade.block_timestamp),
      logIndex: trade.log_index,
      blockNumber: toBlockNumber(trade.block_number),
      data: trade,
    })
  }
  for (const split of splits) {
    transferSkipTxs.add(split.tx_hash)
    events.push({
      type: 'split',
      ts: toTs(split.block_timestamp),
      logIndex: split.log_index,
      blockNumber: toBlockNumber(split.block_number),
      data: split,
    })
  }
  for (const merge of merges) {
    transferSkipTxs.add(merge.tx_hash)
    events.push({
      type: 'merge',
      ts: toTs(merge.block_timestamp),
      logIndex: merge.log_index,
      blockNumber: toBlockNumber(merge.block_number),
      data: merge,
    })
  }
  for (const redemption of redemptions) {
    transferSkipTxs.add(redemption.tx_hash)
    events.push({
      type: 'redemption',
      ts: toTs(redemption.block_timestamp),
      logIndex: redemption.log_index,
      blockNumber: toBlockNumber(redemption.block_number),
      data: redemption,
    })
  }
  for (const split of adapterSplits) {
    transferSkipTxs.add(split.tx_hash)
    events.push({
      type: 'adapter_split',
      ts: toTs(split.block_timestamp),
      logIndex: split.log_index,
      blockNumber: toBlockNumber(split.block_number),
      data: split,
    })
  }
  for (const merge of adapterMerges) {
    transferSkipTxs.add(merge.tx_hash)
    events.push({
      type: 'adapter_merge',
      ts: toTs(merge.block_timestamp),
      logIndex: merge.log_index,
      blockNumber: toBlockNumber(merge.block_number),
      data: merge,
    })
  }
  for (const redemption of adapterRedemptions) {
    transferSkipTxs.add(redemption.tx_hash)
    events.push({
      type: 'adapter_redemption',
      ts: toTs(redemption.block_timestamp),
      logIndex: redemption.log_index,
      blockNumber: toBlockNumber(redemption.block_number),
      data: redemption,
    })
  }
  for (const conversion of adapterConversions) {
    transferSkipTxs.add(conversion.tx_hash)
    events.push({
      type: 'adapter_conversion',
      ts: toTs(conversion.block_timestamp),
      logIndex: conversion.log_index,
      blockNumber: toBlockNumber(conversion.block_number),
      data: conversion,
    })
  }
  for (const refund of feeRefunds) {
    events.push({
      type: 'fee_refund',
      ts: toTs(refund.block_timestamp),
      logIndex: refund.log_index,
      blockNumber: toBlockNumber(refund.block_number),
      data: refund,
    })
  }
  for (const withdrawal of feeWithdrawals) {
    events.push({
      type: 'fee_withdrawal',
      ts: toTs(withdrawal.block_timestamp),
      logIndex: withdrawal.log_index,
      blockNumber: toBlockNumber(withdrawal.block_number),
      data: withdrawal,
    })
  }

  for (const transfer of transfers) {
    if (transferSkipTxs.has(transfer.tx_hash)) continue
    if (
      tradeTxs.has(transfer.tx_hash) &&
      [CTF_EXCHANGE_BINARY.toLowerCase(), CTF_EXCHANGE_MULTI.toLowerCase()].includes(
        transfer.operator.toLowerCase()
      )
    ) {
      continue
    }
    const from = transfer.from.toLowerCase()
    const to = transfer.to.toLowerCase()
    if (from !== w && to !== w) continue
    if (from === w && to === w) continue
    events.push({
      type: 'transfer',
      ts: toTs(transfer.block_timestamp),
      logIndex: transfer.log_index,
      blockNumber: toBlockNumber(transfer.block_number),
      data: transfer,
    })
  }

  for (const condition of conditionMap.values()) {
    if (condition.resolvedAt > 0 && condition.payoutDenominator > 0n) {
      events.push({
        type: 'resolution',
        ts: condition.resolvedAt,
        logIndex: Number.MAX_SAFE_INTEGER,
        blockNumber: condition.resolvedBlock,
        data: condition,
      })
    }
  }

  events.sort((a, b) => {
    if (a.ts !== b.ts) return a.ts - b.ts
    if (a.blockNumber !== b.blockNumber) return a.blockNumber - b.blockNumber
    if (a.logIndex !== b.logIndex) return a.logIndex - b.logIndex
    return a.type.localeCompare(b.type)
  })

  let nextSnapshotTs: number | null = null
  let lastSnapshotTs: number | null = null
  if (snapshotConfig) {
    const startAnchor = snapshotConfig.startTs ?? (events[0]?.ts ?? 0)
    const alignedStart = Math.floor(startAnchor / snapshotConfig.intervalSeconds) * snapshotConfig.intervalSeconds
    nextSnapshotTs = snapshotConfig.startTs ?? (alignedStart + snapshotConfig.intervalSeconds)
  }

  let cumulativeRealized = 0
  let cumulativeCashflow = 0

  const appendLedgerEntry = (entry: LedgerEntry) => {
    ledgerEntries.push(entry)
    cumulativeRealized += entry.realized_pnl
    cumulativeCashflow += entry.usdc_delta
  }

  const maybeSnapshot = (currentTs: number) => {
    if (!snapshotConfig || nextSnapshotTs === null) return
    while (nextSnapshotTs <= currentTs) {
      const openPositionsCost = positions.getOpenPositionsCost()
      const openPositionsValue = positions.getOpenPositionsValue(lastPrices)
      snapshots.push({
        wallet: w,
        snapshot_time: new Date(nextSnapshotTs * 1000),
        realized_pnl: cumulativeRealized,
        unrealized_pnl: openPositionsValue - openPositionsCost,
        open_positions_cost: openPositionsCost,
        open_positions_value: openPositionsValue,
        cashflow: cumulativeCashflow,
        token_count: positions.getOpenPositions().size,
        height: 0,
      })
      lastSnapshotTs = nextSnapshotTs
      nextSnapshotTs += snapshotConfig.intervalSeconds
    }
  }

  for (const event of events) {
    maybeSnapshot(event.ts)
    switch (event.type) {
      case 'trade': {
        const trade = event.data as TradeRow
        tradeCount += 1

        const isMaker = trade.maker === w
        const isTaker = trade.taker === w

        const qty = toTokenNumber(BigInt(trade.token_amount))
        const usdc = toUsdcNumber(BigInt(trade.usdc_amount))
        const fee = toUsdcNumber(BigInt(trade.fee))

        const timestamp = event.ts
        const blockTimestamp = new Date(trade.block_timestamp)

        const handleBuy = (role: string) => {
          if (qty <= 0) return
          const unitCost = usdc / qty
          positions.addTokens(trade.token_id, qty, unitCost, timestamp)
          totalBuyCost += usdc
          totalBuyTokens += qty
          lastPrices.set(trade.token_id, unitCost)

          appendLedgerEntry(createLedgerEntry({
            id: `${trade.id}-${role}-buy`,
            wallet: w,
            event_type: 'trade_buy',
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
            block_number: Number(trade.block_number),
            block_timestamp: blockTimestamp,
            token_id: trade.token_id,
            quantity: qty,
            usdc_delta: -usdc,
            unit_price: unitCost,
            cost_basis: usdc,
            entry_timestamp: blockTimestamp,
          }))
        }

        const handleSell = (role: string) => {
          if (qty <= 0) return
          const proceeds = usdc - fee
          const unitPrice = proceeds / qty
          const { costBasis, consumptions } = positions.consumeTokens(trade.token_id, qty)
          totalSellProceeds += proceeds
          totalSellTokens += qty
          lastPrices.set(trade.token_id, unitPrice)

          for (const consumption of consumptions) {
            realizedEvents.push({
              type: 'sell',
              timestamp,
              entryTimestamp: consumption.timestamp,
              tokenId: trade.token_id,
              proceeds: consumption.quantity * unitPrice,
              costBasis: consumption.quantity * consumption.unitCost,
              realizedPnl: consumption.quantity * (unitPrice - consumption.unitCost),
            })
          }

          const entryTs = weightedEntryTimestamp(consumptions)
          appendLedgerEntry(createLedgerEntry({
            id: `${trade.id}-${role}-sell`,
            wallet: w,
            event_type: 'trade_sell',
            tx_hash: trade.tx_hash,
            log_index: trade.log_index,
            block_number: Number(trade.block_number),
            block_timestamp: blockTimestamp,
            token_id: trade.token_id,
            quantity: qty,
            usdc_delta: proceeds,
            unit_price: unitPrice,
            cost_basis: costBasis,
            realized_pnl: proceeds - costBasis,
            entry_timestamp: entryTs ? new Date(entryTs * 1000) : new Date(0),
          }))
        }

        if (isMaker) {
          if (trade.is_maker_buy) {
            handleBuy('maker')
          } else {
            handleSell('maker')
          }
        }
        if (isTaker) {
          if (trade.is_taker_buy) {
            handleBuy('taker')
          } else {
            handleSell('taker')
          }
        }
        break
      }
      case 'split': {
        const split = event.data as SplitRow
        const blockTimestamp = new Date(split.block_timestamp)
        const usdcAmount = BigInt(split.amount)
        const totalCost = toUsdcNumber(usdcAmount)
        const mintTransfers = selectTransfers(
          transfersByTx,
          split.tx_hash,
          (t) => t.from === ZERO_ADDRESS && t.to === w
        )
        const tokenAmounts = splitTokenAmountsFromTransfers(mintTransfers)

        let totalMintQty = Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0)

        if (totalMintQty === 0) {
          const condition = conditionMap.get(split.condition_id)
          const indexSets = parseArray(split.partition).map((v) => BigInt(v))
          const fallback = computeFallbackTokenAmounts(condition, indexSets, usdcAmount)
          for (const [tokenId, qty] of fallback) tokenAmounts.set(tokenId, qty)
          totalMintQty = Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0)
        }

        const unitCost = totalMintQty > 0 ? totalCost / totalMintQty : 0
        for (const [tokenId, qty] of tokenAmounts) {
          if (qty <= 0) continue
          positions.addTokens(tokenId, qty, unitCost, event.ts)
        }

        appendLedgerEntry(createLedgerEntry({
          id: split.id,
          wallet: w,
          event_type: 'split',
          tx_hash: split.tx_hash,
          log_index: split.log_index,
          block_number: Number(split.block_number),
          block_timestamp: blockTimestamp,
          condition_id: split.condition_id,
          quantity: totalMintQty,
          usdc_delta: -totalCost,
          unit_price: unitCost,
          cost_basis: totalCost,
          metadata: { token_count: tokenAmounts.size },
        }))
        break
      }
      case 'merge': {
        mergeCount += 1
        const merge = event.data as MergeRow
        const blockTimestamp = new Date(merge.block_timestamp)
        const usdcAmount = BigInt(merge.amount)
        const proceeds = toUsdcNumber(usdcAmount)

        const burnTransfers = selectTransfers(
          transfersByTx,
          merge.tx_hash,
          (t) => t.to === ZERO_ADDRESS && t.from === w
        )
        const tokenAmounts = splitTokenAmountsFromTransfers(burnTransfers)

        let totalBurnQty = Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0)
        if (totalBurnQty === 0) {
          const condition = conditionMap.get(merge.condition_id)
          const fallback = computeFallbackTokenAmounts(condition, null, usdcAmount)
          for (const [tokenId, qty] of fallback) tokenAmounts.set(tokenId, qty)
          totalBurnQty = Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0)
        }

        const unitProceeds = totalBurnQty > 0 ? proceeds / totalBurnQty : 0
        let totalCostBasis = 0
        const entryConsumptions: LotConsumption[] = []

        for (const [tokenId, qty] of tokenAmounts) {
          if (qty <= 0) continue
          const { costBasis, consumptions } = positions.consumeTokens(tokenId, qty)
          totalCostBasis += costBasis
          entryConsumptions.push(...consumptions)
          for (const consumption of consumptions) {
            realizedEvents.push({
              type: 'merge',
              timestamp: event.ts,
              entryTimestamp: consumption.timestamp,
              tokenId,
              proceeds: consumption.quantity * unitProceeds,
              costBasis: consumption.quantity * consumption.unitCost,
              realizedPnl: consumption.quantity * (unitProceeds - consumption.unitCost),
            })
          }
        }

        appendLedgerEntry(createLedgerEntry({
          id: merge.id,
          wallet: w,
          event_type: 'merge',
          tx_hash: merge.tx_hash,
          log_index: merge.log_index,
          block_number: Number(merge.block_number),
          block_timestamp: blockTimestamp,
          condition_id: merge.condition_id,
          quantity: totalBurnQty,
          usdc_delta: proceeds,
          unit_price: unitProceeds,
          cost_basis: totalCostBasis,
          realized_pnl: proceeds - totalCostBasis,
          entry_timestamp: weightedEntryTimestamp(entryConsumptions)
            ? new Date(weightedEntryTimestamp(entryConsumptions)! * 1000)
            : new Date(0),
        }))
        break
      }
      case 'redemption': {
        redemptionCount += 1
        const redemption = event.data as RedemptionRow
        const blockTimestamp = new Date(redemption.block_timestamp)
        const payout = toUsdcNumber(BigInt(redemption.payout))

        const burnTransfers = selectTransfers(
          transfersByTx,
          redemption.tx_hash,
          (t) => t.to === ZERO_ADDRESS && t.from === w
        )
        const tokenAmounts = splitTokenAmountsFromTransfers(burnTransfers)

        if (tokenAmounts.size === 0) {
          const condition = conditionMap.get(redemption.condition_id)
          const indexSets = parseArray(redemption.index_sets).map((v) => BigInt(v))
          const fallback = computeFallbackTokenAmounts(condition, indexSets, BigInt(redemption.payout))
          for (const [tokenId, qty] of fallback) tokenAmounts.set(tokenId, qty)
        }

        const condition = conditionMap.get(redemption.condition_id)
        const ratios = computePayoutRatios(condition)
        let expected = 0
        const payoutsByToken = new Map<string, number>()

        if (condition && ratios.length > 0) {
          condition.tokenIds.forEach((tokenId, i) => {
            const ratio = ratios[i] || 0
            const qty = tokenAmounts.get(tokenId) || 0
            if (qty > 0) {
              const tokenPayout = qty * ratio
              payoutsByToken.set(tokenId, tokenPayout)
              expected += tokenPayout
            }
          })
        }

        const payoutScale = expected > 0 ? payout / expected : 0
        let totalCostBasis = 0
        const entryConsumptions: LotConsumption[] = []

        if (expected > 0) {
          for (const [tokenId, qty] of tokenAmounts) {
            if (qty <= 0) continue
            const ratio = ratios[condition?.tokenIds.indexOf(tokenId) ?? -1] || 0
            const unitProceeds = ratio * payoutScale
            const { costBasis, consumptions } = positions.consumeTokens(tokenId, qty)
            totalCostBasis += costBasis
            entryConsumptions.push(...consumptions)
            for (const consumption of consumptions) {
              realizedEvents.push({
                type: 'redemption',
                timestamp: event.ts,
                entryTimestamp: consumption.timestamp,
                tokenId,
                proceeds: consumption.quantity * unitProceeds,
                costBasis: consumption.quantity * consumption.unitCost,
                realizedPnl: consumption.quantity * (unitProceeds - consumption.unitCost),
              })
            }
          }
        } else {
          const totalQty = Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0)
          const unitProceeds = totalQty > 0 ? payout / totalQty : 0
          for (const [tokenId, qty] of tokenAmounts) {
            if (qty <= 0) continue
            const { costBasis, consumptions } = positions.consumeTokens(tokenId, qty)
            totalCostBasis += costBasis
            entryConsumptions.push(...consumptions)
            for (const consumption of consumptions) {
              realizedEvents.push({
                type: 'redemption',
                timestamp: event.ts,
                entryTimestamp: consumption.timestamp,
                tokenId,
                proceeds: consumption.quantity * unitProceeds,
                costBasis: consumption.quantity * consumption.unitCost,
                realizedPnl: consumption.quantity * (unitProceeds - consumption.unitCost),
              })
            }
          }
        }

        appendLedgerEntry(createLedgerEntry({
          id: redemption.id,
          wallet: w,
          event_type: 'redemption',
          tx_hash: redemption.tx_hash,
          log_index: redemption.log_index,
          block_number: Number(redemption.block_number),
          block_timestamp: blockTimestamp,
          condition_id: redemption.condition_id,
          quantity: Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0),
          usdc_delta: payout,
          cost_basis: totalCostBasis,
          realized_pnl: payout - totalCostBasis,
          entry_timestamp: weightedEntryTimestamp(entryConsumptions)
            ? new Date(weightedEntryTimestamp(entryConsumptions)! * 1000)
            : new Date(0),
        }))
        break
      }
      case 'adapter_split': {
        const split = event.data as AdapterSplitRow
        const blockTimestamp = new Date(split.block_timestamp)
        const usdcAmount = BigInt(split.amount)
        const totalCost = toUsdcNumber(usdcAmount)
        const condition = conditionMap.get(split.condition_id)

        const mintTransfers = selectTransfers(
          transfersByTx,
          split.tx_hash,
          (t) => t.from === ZERO_ADDRESS && t.to === w
        )
        const tokenAmounts = splitTokenAmountsFromTransfers(mintTransfers)

        if (tokenAmounts.size === 0) {
          const fallback = computeFallbackTokenAmounts(condition, null, usdcAmount)
          for (const [tokenId, qty] of fallback) tokenAmounts.set(tokenId, qty)
        }

        const totalMintQty = Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0)
        const unitCost = totalMintQty > 0 ? totalCost / totalMintQty : 0
        for (const [tokenId, qty] of tokenAmounts) {
          if (qty <= 0) continue
          positions.addTokens(tokenId, qty, unitCost, event.ts)
        }

        appendLedgerEntry(createLedgerEntry({
          id: split.id,
          wallet: w,
          event_type: 'adapter_split',
          tx_hash: split.tx_hash,
          log_index: split.log_index,
          block_number: Number(split.block_number),
          block_timestamp: blockTimestamp,
          condition_id: split.condition_id,
          quantity: totalMintQty,
          usdc_delta: -totalCost,
          unit_price: unitCost,
          cost_basis: totalCost,
          metadata: { token_count: tokenAmounts.size },
        }))
        break
      }
      case 'adapter_merge': {
        mergeCount += 1
        const merge = event.data as AdapterMergeRow
        const blockTimestamp = new Date(merge.block_timestamp)
        const usdcAmount = BigInt(merge.amount)
        const proceeds = toUsdcNumber(usdcAmount)

        const burnTransfers = selectTransfers(
          transfersByTx,
          merge.tx_hash,
          (t) => t.to === ZERO_ADDRESS && t.from === w
        )
        const tokenAmounts = splitTokenAmountsFromTransfers(burnTransfers)

        if (tokenAmounts.size === 0) {
          const condition = conditionMap.get(merge.condition_id)
          const fallback = computeFallbackTokenAmounts(condition, null, usdcAmount)
          for (const [tokenId, qty] of fallback) tokenAmounts.set(tokenId, qty)
        }

        const totalBurnQty = Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0)
        const unitProceeds = totalBurnQty > 0 ? proceeds / totalBurnQty : 0
        let totalCostBasis = 0
        const entryConsumptions: LotConsumption[] = []

        for (const [tokenId, qty] of tokenAmounts) {
          if (qty <= 0) continue
          const { costBasis, consumptions } = positions.consumeTokens(tokenId, qty)
          totalCostBasis += costBasis
          entryConsumptions.push(...consumptions)
          for (const consumption of consumptions) {
            realizedEvents.push({
              type: 'merge',
              timestamp: event.ts,
              entryTimestamp: consumption.timestamp,
              tokenId,
              proceeds: consumption.quantity * unitProceeds,
              costBasis: consumption.quantity * consumption.unitCost,
              realizedPnl: consumption.quantity * (unitProceeds - consumption.unitCost),
            })
          }
        }

        appendLedgerEntry(createLedgerEntry({
          id: merge.id,
          wallet: w,
          event_type: 'adapter_merge',
          tx_hash: merge.tx_hash,
          log_index: merge.log_index,
          block_number: Number(merge.block_number),
          block_timestamp: blockTimestamp,
          condition_id: merge.condition_id,
          quantity: totalBurnQty,
          usdc_delta: proceeds,
          unit_price: unitProceeds,
          cost_basis: totalCostBasis,
          realized_pnl: proceeds - totalCostBasis,
          entry_timestamp: weightedEntryTimestamp(entryConsumptions)
            ? new Date(weightedEntryTimestamp(entryConsumptions)! * 1000)
            : new Date(0),
        }))
        break
      }
      case 'adapter_redemption': {
        redemptionCount += 1
        const redemption = event.data as AdapterRedemptionRow
        const blockTimestamp = new Date(redemption.block_timestamp)
        const payout = toUsdcNumber(BigInt(redemption.payout))
        const condition = conditionMap.get(redemption.condition_id)

        const tokenAmounts = new Map<string, number>()
        const amounts = parseArray(redemption.amounts)
        if (condition && amounts.length > 0) {
          for (let i = 0; i < Math.min(condition.tokenIds.length, amounts.length); i++) {
            const qty = toTokenNumber(BigInt(amounts[i]))
            if (qty > 0) tokenAmounts.set(condition.tokenIds[i], qty)
          }
        }

        if (tokenAmounts.size === 0) {
          const burnTransfers = selectTransfers(
            transfersByTx,
            redemption.tx_hash,
            (t) => t.to === ZERO_ADDRESS && t.from === w
          )
          const transferAmounts = splitTokenAmountsFromTransfers(burnTransfers)
          for (const [tokenId, qty] of transferAmounts) tokenAmounts.set(tokenId, qty)
        }

        let expected = 0
        const ratios = computePayoutRatios(condition)
        if (condition && ratios.length > 0) {
          condition.tokenIds.forEach((tokenId, i) => {
            const ratio = ratios[i] || 0
            const qty = tokenAmounts.get(tokenId) || 0
            if (qty > 0) expected += qty * ratio
          })
        }

        const payoutScale = expected > 0 ? payout / expected : 0
        let totalCostBasis = 0
        const entryConsumptions: LotConsumption[] = []

        if (expected > 0) {
          for (const [tokenId, qty] of tokenAmounts) {
            if (qty <= 0) continue
            const ratio = ratios[condition?.tokenIds.indexOf(tokenId) ?? -1] || 0
            const unitProceeds = ratio * payoutScale
            const { costBasis, consumptions } = positions.consumeTokens(tokenId, qty)
            totalCostBasis += costBasis
            entryConsumptions.push(...consumptions)
            for (const consumption of consumptions) {
              realizedEvents.push({
                type: 'redemption',
                timestamp: event.ts,
                entryTimestamp: consumption.timestamp,
                tokenId,
                proceeds: consumption.quantity * unitProceeds,
                costBasis: consumption.quantity * consumption.unitCost,
                realizedPnl: consumption.quantity * (unitProceeds - consumption.unitCost),
              })
            }
          }
        } else {
          const totalQty = Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0)
          const unitProceeds = totalQty > 0 ? payout / totalQty : 0
          for (const [tokenId, qty] of tokenAmounts) {
            if (qty <= 0) continue
            const { costBasis, consumptions } = positions.consumeTokens(tokenId, qty)
            totalCostBasis += costBasis
            entryConsumptions.push(...consumptions)
            for (const consumption of consumptions) {
              realizedEvents.push({
                type: 'redemption',
                timestamp: event.ts,
                entryTimestamp: consumption.timestamp,
                tokenId,
                proceeds: consumption.quantity * unitProceeds,
                costBasis: consumption.quantity * consumption.unitCost,
                realizedPnl: consumption.quantity * (unitProceeds - consumption.unitCost),
              })
            }
          }
        }

        appendLedgerEntry(createLedgerEntry({
          id: redemption.id,
          wallet: w,
          event_type: 'adapter_redemption',
          tx_hash: redemption.tx_hash,
          log_index: redemption.log_index,
          block_number: Number(redemption.block_number),
          block_timestamp: blockTimestamp,
          condition_id: redemption.condition_id,
          quantity: Array.from(tokenAmounts.values()).reduce((sum, v) => sum + v, 0),
          usdc_delta: payout,
          cost_basis: totalCostBasis,
          realized_pnl: payout - totalCostBasis,
          entry_timestamp: weightedEntryTimestamp(entryConsumptions)
            ? new Date(weightedEntryTimestamp(entryConsumptions)! * 1000)
            : new Date(0),
        }))
        break
      }
      case 'adapter_conversion': {
        const conversion = event.data as AdapterConversionRow
        const blockTimestamp = new Date(conversion.block_timestamp)
        const burnTransfers = selectTransfers(
          transfersByTx,
          conversion.tx_hash,
          (t) => t.to === ZERO_ADDRESS && t.from === w
        )
        const mintTransfers = selectTransfers(
          transfersByTx,
          conversion.tx_hash,
          (t) => t.from === ZERO_ADDRESS && t.to === w
        )

        const burned = splitTokenAmountsFromTransfers(burnTransfers)
        const minted = splitTokenAmountsFromTransfers(mintTransfers)
        let usedFallback = false

        if (burned.size === 0 && minted.size === 0) {
          const questionCount = negRiskMarketMap.get(conversion.market_id) || 0
          if (questionCount > 0) {
            const indexSet = BigInt(conversion.index_set)
            const perQuestionQty = toTokenNumber(BigInt(conversion.amount))
            const tokensByQuestion = computeNegRiskTokenIds(conversion.market_id, questionCount)
            for (let i = 0; i < questionCount; i++) {
              const tokenIds = tokensByQuestion[i]
              if (!tokenIds) continue
              if (indexSetContains(indexSet, i)) {
                burned.set(tokenIds.no, (burned.get(tokenIds.no) || 0) + perQuestionQty)
              } else {
                minted.set(tokenIds.yes, (minted.get(tokenIds.yes) || 0) + perQuestionQty)
              }
            }
            usedFallback = true
          }
        }

        let totalBurnQty = Array.from(burned.values()).reduce((sum, v) => sum + v, 0)
        let totalMintQty = Array.from(minted.values()).reduce((sum, v) => sum + v, 0)
        let totalCostBasis = 0
        const entryConsumptions: LotConsumption[] = []

        for (const [tokenId, qty] of burned) {
          if (qty <= 0) continue
          const { costBasis, consumptions } = positions.consumeTokens(tokenId, qty)
          totalCostBasis += costBasis
          entryConsumptions.push(...consumptions)
        }

        if (totalMintQty > 0) {
          const unitCost = totalCostBasis > 0 ? totalCostBasis / totalMintQty : 0
          for (const [tokenId, qty] of minted) {
            if (qty <= 0) continue
            const appliedCost = unitCost > 0 ? unitCost : (lastPrices.get(tokenId) || 0)
            positions.addTokens(tokenId, qty, appliedCost, event.ts)
            if (appliedCost > 0) lastPrices.set(tokenId, appliedCost)
          }
        }

        appendLedgerEntry(createLedgerEntry({
          id: conversion.id,
          wallet: w,
          event_type: 'adapter_conversion',
          tx_hash: conversion.tx_hash,
          log_index: conversion.log_index,
          block_number: Number(conversion.block_number),
          block_timestamp: blockTimestamp,
          condition_id: conversion.market_id,
          quantity: totalMintQty > 0 ? totalMintQty : totalBurnQty,
          cost_basis: totalCostBasis,
          entry_timestamp: weightedEntryTimestamp(entryConsumptions)
            ? new Date(weightedEntryTimestamp(entryConsumptions)! * 1000)
            : new Date(0),
          metadata: {
            index_set: conversion.index_set,
            burn_tokens: burned.size,
            mint_tokens: minted.size,
            fallback: usedFallback,
          },
        }))
        break
      }
      case 'transfer': {
        const transfer = event.data as TransferRow
        const blockTimestamp = new Date(transfer.block_timestamp)
        const qty = toTokenNumber(BigInt(transfer.value))
        if (qty <= 0) break

        const from = transfer.from.toLowerCase()
        const to = transfer.to.toLowerCase()

        if (from === w) {
          const { costBasis, consumptions } = positions.consumeTokens(transfer.token_id, qty)
          appendLedgerEntry(createLedgerEntry({
            id: transfer.id,
            wallet: w,
            event_type: transfer.to === ZERO_ADDRESS ? 'burn' : 'transfer_out',
            tx_hash: transfer.tx_hash,
            log_index: transfer.log_index,
            block_number: Number(transfer.block_number),
            block_timestamp: blockTimestamp,
            token_id: transfer.token_id,
            quantity: qty,
            usdc_delta: 0,
            unit_price: qty > 0 ? costBasis / qty : 0,
            cost_basis: costBasis,
            realized_pnl: 0,
            entry_timestamp: weightedEntryTimestamp(consumptions)
              ? new Date(weightedEntryTimestamp(consumptions)! * 1000)
              : new Date(0),
            metadata: { to },
          }))
          break
        }

        if (to === w) {
          const existingAvg = positions.getAverageUnitCost(transfer.token_id)
          const fallbackPrice = lastPrices.get(transfer.token_id) || 0
          const unitCost = existingAvg > 0 ? existingAvg : fallbackPrice
          positions.addTokens(transfer.token_id, qty, unitCost, event.ts)
          if (unitCost > 0) lastPrices.set(transfer.token_id, unitCost)
          appendLedgerEntry(createLedgerEntry({
            id: transfer.id,
            wallet: w,
            event_type: transfer.from === ZERO_ADDRESS ? 'mint' : 'transfer_in',
            tx_hash: transfer.tx_hash,
            log_index: transfer.log_index,
            block_number: Number(transfer.block_number),
            block_timestamp: blockTimestamp,
            token_id: transfer.token_id,
            quantity: qty,
            usdc_delta: 0,
            unit_price: unitCost,
            cost_basis: qty * unitCost,
            realized_pnl: 0,
            entry_timestamp: blockTimestamp,
            metadata: { from },
          }))
        }
        break
      }
      case 'fee_refund': {
        const refund = event.data as FeeRefundRow
        const blockTimestamp = new Date(refund.block_timestamp)
        const amount = toUsdcNumber(BigInt(refund.refund))
        realizedEvents.push({
          type: 'fee',
          timestamp: event.ts,
          entryTimestamp: event.ts,
          tokenId: refund.token_id.toString(),
          proceeds: amount,
          costBasis: 0,
          realizedPnl: amount,
        })
        appendLedgerEntry(createLedgerEntry({
          id: refund.id,
          wallet: w,
          event_type: 'fee_refund',
          tx_hash: refund.tx_hash,
          log_index: refund.log_index,
          block_number: Number(refund.block_number),
          block_timestamp: blockTimestamp,
          token_id: refund.token_id.toString(),
          usdc_delta: amount,
          realized_pnl: amount,
        }))
        break
      }
      case 'fee_withdrawal': {
        const withdrawal = event.data as FeeWithdrawalRow
        const blockTimestamp = new Date(withdrawal.block_timestamp)
        const amount = toUsdcNumber(BigInt(withdrawal.amount))
        realizedEvents.push({
          type: 'fee',
          timestamp: event.ts,
          entryTimestamp: event.ts,
          tokenId: withdrawal.token_id.toString(),
          proceeds: amount,
          costBasis: 0,
          realizedPnl: amount,
        })
        appendLedgerEntry(createLedgerEntry({
          id: withdrawal.id,
          wallet: w,
          event_type: 'fee_withdrawal',
          tx_hash: withdrawal.tx_hash,
          log_index: withdrawal.log_index,
          block_number: Number(withdrawal.block_number),
          block_timestamp: blockTimestamp,
          token_id: withdrawal.token_id.toString(),
          usdc_delta: amount,
          realized_pnl: amount,
        }))
        break
      }
      case 'resolution': {
        const condition = event.data as ConditionInfo
        const ratios = computePayoutRatios(condition)
        if (ratios.length === 0) break

        for (let i = 0; i < condition.tokenIds.length; i++) {
          const ratio = ratios[i] || 0
          if (ratio > 0) continue
          const tokenId = condition.tokenIds[i]
          const qty = positions.getTotalQuantity(tokenId)
          if (qty <= 0) continue

          const { costBasis, consumptions } = positions.consumeTokens(tokenId, qty)
          for (const consumption of consumptions) {
            realizedEvents.push({
              type: 'resolution_loss',
              timestamp: event.ts,
              entryTimestamp: consumption.timestamp,
              tokenId,
              proceeds: 0,
              costBasis: consumption.quantity * consumption.unitCost,
              realizedPnl: -consumption.quantity * consumption.unitCost,
            })
          }

          appendLedgerEntry(createLedgerEntry({
            id: `${condition.conditionId}-${tokenId}-loss-${event.ts}`,
            wallet: w,
            event_type: 'resolution_loss',
            tx_hash: '',
            log_index: 0,
            block_number: 0,
            block_timestamp: new Date(event.ts * 1000),
            token_id: tokenId,
            condition_id: condition.conditionId,
            quantity: qty,
            usdc_delta: 0,
            cost_basis: costBasis,
            realized_pnl: -costBasis,
            entry_timestamp: weightedEntryTimestamp(consumptions)
              ? new Date(weightedEntryTimestamp(consumptions)! * 1000)
              : new Date(0),
          }))
        }
        break
      }
      default:
        break
    }
  }

  if (snapshotConfig && nextSnapshotTs !== null) {
    const finalTs = snapshotConfig.endTs ?? endTs ?? (events[events.length - 1]?.ts ?? 0)
    maybeSnapshot(finalTs)
    if (snapshotConfig.endTs && (lastSnapshotTs === null || lastSnapshotTs < snapshotConfig.endTs)) {
      const openPositionsCost = positions.getOpenPositionsCost()
      const openPositionsValue = positions.getOpenPositionsValue(lastPrices)
      snapshots.push({
        wallet: w,
        snapshot_time: new Date(snapshotConfig.endTs * 1000),
        realized_pnl: cumulativeRealized,
        unrealized_pnl: openPositionsValue - openPositionsCost,
        open_positions_cost: openPositionsCost,
        open_positions_value: openPositionsValue,
        cashflow: cumulativeCashflow,
        token_count: positions.getOpenPositions().size,
        height: 0,
      })
      lastSnapshotTs = snapshotConfig.endTs
    }
  }

  return {
    ledgerEntries,
    realizedEvents,
    positions,
    lastPrices,
    snapshots,
    stats: {
      tradeCount,
      redemptionCount,
      mergeCount,
      totalBuyCost,
      totalBuyTokens,
      totalSellProceeds,
      totalSellTokens,
    },
  }
}

export async function calculatePnl(
  wallet: string,
  mode: PnlMode,
  periodStart?: Date,
  periodEnd?: Date
): Promise<PnlResult> {
  const startTs = periodStart ? Math.floor(periodStart.getTime() / 1000) : undefined
  const endTs = periodEnd ? Math.floor(periodEnd.getTime() / 1000) : Math.floor(Date.now() / 1000)

  const { realizedEvents, positions, lastPrices, stats } = await buildLedger(wallet, endTs)

  let realizedFromSells = 0
  let realizedFromRedemptions = 0
  let realizedFromMerges = 0
  let realizedFromResolutionLosses = 0
  let realizedFromFees = 0

  const isInPeriod = (ts: number) => {
    if (startTs && ts < startTs) return false
    if (endTs && ts > endTs) return false
    return true
  }

  const realizedForMode = (event: RealizedEvent): boolean => {
    if (!isInPeriod(event.timestamp)) return false
    if (mode === PnlMode.REALIZED_PERIOD_ONLY || mode === PnlMode.REALIZED_PERIOD_PLUS_UNREALIZED) {
      if (!event.entryTimestamp) return true
      return isInPeriod(event.entryTimestamp)
    }
    return true
  }

  for (const event of realizedEvents) {
    if (!realizedForMode(event)) continue
    switch (event.type) {
      case 'sell':
        realizedFromSells += event.realizedPnl
        break
      case 'redemption':
        realizedFromRedemptions += event.realizedPnl
        break
      case 'merge':
        realizedFromMerges += event.realizedPnl
        break
      case 'resolution_loss':
        realizedFromResolutionLosses += event.realizedPnl
        break
      case 'fee':
        realizedFromFees += event.realizedPnl
        break
      default:
        break
    }
  }

  const totalRealized =
    realizedFromSells +
    realizedFromRedemptions +
    realizedFromMerges +
    realizedFromResolutionLosses +
    realizedFromFees

  let openPositionsCost = 0
  let openPositionsValue = 0
  let unrealizedPnl = 0

  if (mode === PnlMode.REALIZED_PERIOD_PLUS_UNREALIZED || mode === PnlMode.TOTAL_PNL) {
    const filter = mode === PnlMode.REALIZED_PERIOD_PLUS_UNREALIZED
      ? { startTs, endTs }
      : undefined
    openPositionsCost = positions.getOpenPositionsCost(filter)
    openPositionsValue = positions.getOpenPositionsValue(lastPrices, filter)
    unrealizedPnl = openPositionsValue - openPositionsCost
  }

  const totalPnl = totalRealized + unrealizedPnl

  return {
    mode,
    wallet: wallet.toLowerCase(),
    periodStart: periodStart || new Date(0),
    periodEnd: periodEnd || new Date(),
    realizedFromSells,
    realizedFromRedemptions,
    realizedFromMerges,
    realizedFromResolutionLosses,
    realizedFromFees,
    totalRealized,
    unrealizedPnl,
    openPositionsCost,
    openPositionsValue,
    totalPnl,
    totalBuyCost: stats.totalBuyCost,
    totalBuyTokens: stats.totalBuyTokens,
    totalSellProceeds: stats.totalSellProceeds,
    totalSellTokens: stats.totalSellTokens,
    tradeCount: stats.tradeCount,
    redemptionCount: stats.redemptionCount,
    mergeCount: stats.mergeCount,
  }
}

export async function buildLedgerAndSnapshots(
  wallet: string,
  intervalSeconds: number,
  startTs?: number,
  endTs?: number
): Promise<{ ledgerEntries: LedgerEntry[]; snapshots: any[] }> {
  const { ledgerEntries, snapshots } = await buildLedger(wallet, endTs, {
    intervalSeconds,
    startTs,
    endTs,
  })

  return { ledgerEntries, snapshots }
}

export async function getOpenPositionsForWallet(
  wallet: string,
  endTs?: number
): Promise<Map<string, number>> {
  const { positions } = await buildLedger(wallet, endTs)
  const open = positions.getOpenPositions()
  const totals = new Map<string, number>()
  for (const [tokenId, lots] of open) {
    const qty = lots.reduce((sum, lot) => sum + lot.quantity, 0)
    if (qty > 0) totals.set(tokenId, qty)
  }
  return totals
}

export async function closeClient() {
  await client.close()
}
