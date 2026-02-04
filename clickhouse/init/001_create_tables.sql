-- Polymarket PnL Indexer Tables
-- Using ReplacingMergeTree for deduplication on restarts

CREATE DATABASE IF NOT EXISTS polymarket;

-- =====================================================
-- TRADES (from OrderFilled events on CTF Exchange)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.trades
(
    -- Primary identifiers
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),

    -- Event data
    order_hash String,
    maker LowCardinality(String),
    taker LowCardinality(String),
    maker_asset_id String,  -- "0" = USDC, otherwise token ID
    taker_asset_id String,
    maker_amount UInt256,   -- Amount maker gives
    taker_amount UInt256,   -- Amount taker gives
    fee UInt256,

    -- Derived fields for easier querying
    is_maker_buy Bool,      -- True if maker is buying tokens (gives USDC)
    is_taker_buy Bool,      -- True if taker is buying tokens (gives USDC)
    token_id String,        -- The outcome token being traded
    usdc_amount UInt256,    -- USDC side of the trade
    token_amount UInt256,   -- Token side of the trade
    price_per_token Float64,-- USDC per token (6 decimals normalized)

    -- Hot/cold tracking
    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

-- =====================================================
-- SPLITS (from PositionSplit events on CTF)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.splits
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),

    stakeholder LowCardinality(String),
    collateral_token LowCardinality(String),
    parent_collection_id String,
    condition_id String,
    partition Array(UInt256),
    amount UInt256,         -- Collateral locked (USDC, 6 decimals)

    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

-- =====================================================
-- MERGES (from PositionsMerge events on CTF)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.merges
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),

    stakeholder LowCardinality(String),
    collateral_token LowCardinality(String),
    parent_collection_id String,
    condition_id String,
    partition Array(UInt256),
    amount UInt256,         -- Collateral unlocked (USDC, 6 decimals)

    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

-- =====================================================
-- REDEMPTIONS (from PayoutRedemption events on CTF)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.redemptions
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),

    redeemer LowCardinality(String),
    collateral_token LowCardinality(String),
    parent_collection_id String,
    condition_id String,
    index_sets Array(UInt256),
    payout UInt256,         -- USDC received (6 decimals)

    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

-- =====================================================
-- CONDITIONS (market preparation and resolution)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.conditions
(
    condition_id String,
    oracle LowCardinality(String),
    question_id String,
    outcome_slot_count UInt8,

    -- Resolution data (null if not resolved)
    is_resolved Bool DEFAULT false,
    payout_numerators Array(UInt256) DEFAULT [],
    payout_denominator UInt256 DEFAULT 0,
    resolved_at DateTime64(3) DEFAULT toDateTime64('1970-01-01 00:00:00', 3),

    -- Metadata
    created_block UInt64,
    created_at DateTime64(3),

    height UInt64
)
ENGINE = ReplacingMergeTree(height)
ORDER BY (condition_id);

-- =====================================================
-- NEG RISK MARKETS (question counts for conversions)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.neg_risk_markets
(
    market_id String,
    question_count UInt32,
    updated_at DateTime64(3),
    height UInt64
)
ENGINE = ReplacingMergeTree(height)
ORDER BY (market_id)
PARTITION BY toYYYYMM(updated_at);

-- =====================================================
-- ERC1155 TRANSFERS (inventory tracking)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.transfers
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),

    operator LowCardinality(String),
    from LowCardinality(String),
    to LowCardinality(String),
    token_id String,
    value UInt256,

    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

-- =====================================================
-- NEGRISK ADAPTER EVENTS
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.adapter_splits
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),
    stakeholder LowCardinality(String),
    condition_id String,
    amount UInt256,
    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

CREATE TABLE IF NOT EXISTS polymarket.adapter_merges
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),
    stakeholder LowCardinality(String),
    condition_id String,
    amount UInt256,
    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

CREATE TABLE IF NOT EXISTS polymarket.adapter_redemptions
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),
    redeemer LowCardinality(String),
    condition_id String,
    amounts Array(UInt256),
    payout UInt256,
    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

CREATE TABLE IF NOT EXISTS polymarket.adapter_conversions
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),
    stakeholder LowCardinality(String),
    market_id String,
    index_set UInt256,
    amount UInt256,
    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

-- =====================================================
-- FEE MODULE EVENTS
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.fee_refunds
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),
    module LowCardinality(String),
    order_hash String,
    to LowCardinality(String),
    token_id UInt256,
    refund UInt256,
    fee_charged UInt256,
    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

CREATE TABLE IF NOT EXISTS polymarket.fee_withdrawals
(
    id String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),
    module LowCardinality(String),
    token LowCardinality(String),
    to LowCardinality(String),
    token_id UInt256,
    amount UInt256,
    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id)
PARTITION BY toYYYYMM(block_timestamp);

-- =====================================================
-- WALLET LEDGER (derived PnL events)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.wallet_ledger
(
    id String,
    wallet LowCardinality(String),
    event_type LowCardinality(String),
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_timestamp DateTime64(3),
    token_id String,
    condition_id String,
    quantity Float64,
    usdc_delta Float64,
    unit_price Float64,
    cost_basis Float64,
    realized_pnl Float64,
    entry_timestamp DateTime64(3),
    metadata String
)
ENGINE = ReplacingMergeTree()
ORDER BY (wallet, block_timestamp, id)
PARTITION BY toYYYYMM(block_timestamp);

-- =====================================================
-- WALLET PNL SNAPSHOTS (daily/hourly)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.wallet_pnl_snapshots
(
    wallet LowCardinality(String),
    snapshot_time DateTime64(3),
    realized_pnl Float64,
    unrealized_pnl Float64,
    open_positions_cost Float64,
    open_positions_value Float64,
    cashflow Float64,
    token_count UInt32,
    height UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (wallet, snapshot_time)
PARTITION BY toYYYYMM(snapshot_time);

-- =====================================================
-- INDEXER STATUS (for tracking sync progress)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.indexer_status
(
    processor_id String,
    last_block UInt64,
    last_timestamp DateTime64(3),
    updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (processor_id);

-- =====================================================
-- USEFUL VIEWS
-- =====================================================

-- View: All wallet activity (trades from both maker and taker perspective)
CREATE VIEW IF NOT EXISTS polymarket.wallet_trades AS
SELECT
    id,
    block_timestamp,
    maker as wallet,
    'maker' as role,
    if(is_maker_buy, 'buy', 'sell') as side,
    token_id,
    token_amount,
    usdc_amount,
    fee,
    price_per_token
FROM polymarket.trades
UNION ALL
SELECT
    id,
    block_timestamp,
    taker as wallet,
    'taker' as role,
    if(is_taker_buy, 'buy', 'sell') as side,
    token_id,
    token_amount,
    usdc_amount,
    fee,
    price_per_token
FROM polymarket.trades;

-- =====================================================
-- USER BALANCES (SummingMergeTree for position tracking)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.user_balances (
    wallet   LowCardinality(String),
    token_id String,
    balance  Int256
) ENGINE = SummingMergeTree()
ORDER BY (wallet, token_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS polymarket.user_balances_mv
TO polymarket.user_balances
AS
SELECT `to` AS wallet, token_id, toInt256(value) AS balance
FROM polymarket.transfers
WHERE `to` != '0x0000000000000000000000000000000000000000'
UNION ALL
SELECT `from` AS wallet, token_id, negate(toInt256(value)) AS balance
FROM polymarket.transfers
WHERE `from` != '0x0000000000000000000000000000000000000000';

-- =====================================================
-- MARKET METADATA (for Gamma API sync)
-- =====================================================
CREATE TABLE IF NOT EXISTS polymarket.market_metadata (
    condition_id String,
    market_id    String,
    question     String,
    slug         String,
    outcomes     Array(String),
    token_ids    Array(String),
    neg_risk     Bool DEFAULT false,
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY condition_id;
