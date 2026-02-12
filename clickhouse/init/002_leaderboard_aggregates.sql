-- =====================================================
-- LEADERBOARD AGGREGATES (hourly + all-time)
-- Speeds up /leaderboard endpoint by avoiding raw wallet_trades scans.
-- =====================================================

CREATE TABLE IF NOT EXISTS polymarket.wallet_leaderboard_stats_1h (
    bucket DateTime,
    wallet LowCardinality(String),
    trades_state AggregateFunction(count),
    volume_state AggregateFunction(sum, Float64),
    pnl_state AggregateFunction(sum, Float64),
    markets_state AggregateFunction(uniqExact, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket)
ORDER BY (bucket, wallet);

CREATE MATERIALIZED VIEW IF NOT EXISTS polymarket.wallet_leaderboard_stats_1h_mv
TO polymarket.wallet_leaderboard_stats_1h
AS
SELECT
    toStartOfHour(block_timestamp) AS bucket,
    tupleElement(participant, 1) AS wallet,
    countState() AS trades_state,
    sumState(usdc_value) AS volume_state,
    sumState(if(tupleElement(participant, 2) = 'sell', usdc_value, -usdc_value)) AS pnl_state,
    uniqExactState(token_id) AS markets_state
FROM (
    SELECT
        block_timestamp,
        token_id,
        toFloat64(usdc_amount) / 1000000 AS usdc_value,
        arrayJoin([
            tuple(maker, if(is_maker_buy, 'buy', 'sell')),
            tuple(taker, if(is_taker_buy, 'buy', 'sell'))
        ]) AS participant
    FROM polymarket.trades
)
GROUP BY bucket, wallet;

CREATE TABLE IF NOT EXISTS polymarket.wallet_leaderboard_stats_all (
    wallet LowCardinality(String),
    trades_state AggregateFunction(count),
    volume_state AggregateFunction(sum, Float64),
    pnl_state AggregateFunction(sum, Float64),
    markets_state AggregateFunction(uniqExact, String)
) ENGINE = AggregatingMergeTree()
ORDER BY wallet;

CREATE MATERIALIZED VIEW IF NOT EXISTS polymarket.wallet_leaderboard_stats_all_mv
TO polymarket.wallet_leaderboard_stats_all
AS
SELECT
    tupleElement(participant, 1) AS wallet,
    countState() AS trades_state,
    sumState(usdc_value) AS volume_state,
    sumState(if(tupleElement(participant, 2) = 'sell', usdc_value, -usdc_value)) AS pnl_state,
    uniqExactState(token_id) AS markets_state
FROM (
    SELECT
        token_id,
        toFloat64(usdc_amount) / 1000000 AS usdc_value,
        arrayJoin([
            tuple(maker, if(is_maker_buy, 'buy', 'sell')),
            tuple(taker, if(is_taker_buy, 'buy', 'sell'))
        ]) AS participant
    FROM polymarket.trades
)
GROUP BY wallet;
