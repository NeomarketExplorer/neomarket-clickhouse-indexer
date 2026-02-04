export type TableDefinition<T extends object> = {
  name: string
  columns: Record<keyof T, string>
}

// Trade from OrderFilled event
export interface Trade {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  order_hash: string
  maker: string
  taker: string
  maker_asset_id: string
  taker_asset_id: string
  maker_amount: bigint
  taker_amount: bigint
  fee: bigint

  is_maker_buy: boolean
  is_taker_buy: boolean
  token_id: string
  usdc_amount: bigint
  token_amount: bigint
  price_per_token: number

  height: bigint
}

export const tradesTable: TableDefinition<Trade> = {
  name: 'trades',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    order_hash: 'String',
    maker: 'String',
    taker: 'String',
    maker_asset_id: 'String',
    taker_asset_id: 'String',
    maker_amount: 'UInt256',
    taker_amount: 'UInt256',
    fee: 'UInt256',
    is_maker_buy: 'Bool',
    is_taker_buy: 'Bool',
    token_id: 'String',
    usdc_amount: 'UInt256',
    token_amount: 'UInt256',
    price_per_token: 'Float64',
    height: 'UInt64',
  },
}

// Split from PositionSplit event
export interface Split {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  stakeholder: string
  collateral_token: string
  parent_collection_id: string
  condition_id: string
  partition: bigint[]
  amount: bigint

  height: bigint
}

export const splitsTable: TableDefinition<Split> = {
  name: 'splits',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    stakeholder: 'String',
    collateral_token: 'String',
    parent_collection_id: 'String',
    condition_id: 'String',
    partition: 'Array(UInt256)',
    amount: 'UInt256',
    height: 'UInt64',
  },
}

// Merge from PositionsMerge event
export interface Merge {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  stakeholder: string
  collateral_token: string
  parent_collection_id: string
  condition_id: string
  partition: bigint[]
  amount: bigint

  height: bigint
}

export const mergesTable: TableDefinition<Merge> = {
  name: 'merges',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    stakeholder: 'String',
    collateral_token: 'String',
    parent_collection_id: 'String',
    condition_id: 'String',
    partition: 'Array(UInt256)',
    amount: 'UInt256',
    height: 'UInt64',
  },
}

// Redemption from PayoutRedemption event
export interface Redemption {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  redeemer: string
  collateral_token: string
  parent_collection_id: string
  condition_id: string
  index_sets: bigint[]
  payout: bigint

  height: bigint
}

export const redemptionsTable: TableDefinition<Redemption> = {
  name: 'redemptions',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    redeemer: 'String',
    collateral_token: 'String',
    parent_collection_id: 'String',
    condition_id: 'String',
    index_sets: 'Array(UInt256)',
    payout: 'UInt256',
    height: 'UInt64',
  },
}

// Condition from ConditionPreparation/ConditionResolution events
export interface Condition {
  condition_id: string
  oracle: string
  question_id: string
  outcome_slot_count: number

  is_resolved: boolean
  payout_numerators: bigint[]
  payout_denominator: bigint
  resolved_at: Date | null

  created_block: bigint
  created_at: Date

  height: bigint
}

export const conditionsTable: TableDefinition<Condition> = {
  name: 'conditions',
  columns: {
    condition_id: 'String',
    oracle: 'String',
    question_id: 'String',
    outcome_slot_count: 'UInt8',
    is_resolved: 'Bool',
    payout_numerators: 'Array(UInt256)',
    payout_denominator: 'UInt256',
    resolved_at: 'DateTime64(3)',
    created_block: 'UInt64',
    created_at: 'DateTime64(3)',
    height: 'UInt64',
  },
}

// NegRisk market metadata (question count)
export interface NegRiskMarket {
  market_id: string
  question_count: number
  updated_at: Date
  height: bigint
}

export const negRiskMarketsTable: TableDefinition<NegRiskMarket> = {
  name: 'neg_risk_markets',
  columns: {
    market_id: 'String',
    question_count: 'UInt32',
    updated_at: 'DateTime64(3)',
    height: 'UInt64',
  },
}

// ERC1155 transfers from ConditionalTokens (inventory tracking)
export interface Transfer {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  operator: string
  from: string
  to: string
  token_id: string
  value: bigint

  height: bigint
}

export const transfersTable: TableDefinition<Transfer> = {
  name: 'transfers',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    operator: 'String',
    from: 'String',
    to: 'String',
    token_id: 'String',
    value: 'UInt256',
    height: 'UInt64',
  },
}

// NegRiskAdapter events (to avoid mixing with CTF events)
export interface AdapterSplit {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  stakeholder: string
  condition_id: string
  amount: bigint

  height: bigint
}

export const adapterSplitsTable: TableDefinition<AdapterSplit> = {
  name: 'adapter_splits',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    stakeholder: 'String',
    condition_id: 'String',
    amount: 'UInt256',
    height: 'UInt64',
  },
}

export interface AdapterMerge {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  stakeholder: string
  condition_id: string
  amount: bigint

  height: bigint
}

export const adapterMergesTable: TableDefinition<AdapterMerge> = {
  name: 'adapter_merges',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    stakeholder: 'String',
    condition_id: 'String',
    amount: 'UInt256',
    height: 'UInt64',
  },
}

export interface AdapterRedemption {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  redeemer: string
  condition_id: string
  amounts: bigint[]
  payout: bigint

  height: bigint
}

export const adapterRedemptionsTable: TableDefinition<AdapterRedemption> = {
  name: 'adapter_redemptions',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    redeemer: 'String',
    condition_id: 'String',
    amounts: 'Array(UInt256)',
    payout: 'UInt256',
    height: 'UInt64',
  },
}

export interface AdapterConversion {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  stakeholder: string
  market_id: string
  index_set: bigint
  amount: bigint

  height: bigint
}

export const adapterConversionsTable: TableDefinition<AdapterConversion> = {
  name: 'adapter_conversions',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    stakeholder: 'String',
    market_id: 'String',
    index_set: 'UInt256',
    amount: 'UInt256',
    height: 'UInt64',
  },
}

// Fee module events
export interface FeeRefund {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  module: string
  order_hash: string
  to: string
  token_id: bigint
  refund: bigint
  fee_charged: bigint

  height: bigint
}

export const feeRefundsTable: TableDefinition<FeeRefund> = {
  name: 'fee_refunds',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    module: 'String',
    order_hash: 'String',
    to: 'String',
    token_id: 'UInt256',
    refund: 'UInt256',
    fee_charged: 'UInt256',
    height: 'UInt64',
  },
}

export interface FeeWithdrawal {
  id: string
  tx_hash: string
  log_index: number
  block_number: bigint
  block_timestamp: Date

  module: string
  token: string
  to: string
  token_id: bigint
  amount: bigint

  height: bigint
}

export const feeWithdrawalsTable: TableDefinition<FeeWithdrawal> = {
  name: 'fee_withdrawals',
  columns: {
    id: 'String',
    tx_hash: 'String',
    log_index: 'UInt32',
    block_number: 'UInt64',
    block_timestamp: 'DateTime64(3)',
    module: 'String',
    token: 'String',
    to: 'String',
    token_id: 'UInt256',
    amount: 'UInt256',
    height: 'UInt64',
  },
}

export const allTables = [
  tradesTable,
  splitsTable,
  mergesTable,
  redemptionsTable,
  conditionsTable,
  negRiskMarketsTable,
  transfersTable,
  adapterSplitsTable,
  adapterMergesTable,
  adapterRedemptionsTable,
  adapterConversionsTable,
  feeRefundsTable,
  feeWithdrawalsTable,
]
