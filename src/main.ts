import { EvmBatchProcessor } from '@subsquid/evm-processor'
import { ClickhouseDatabase } from 'clickhouse-subsquid-store'
import { createClient } from '@clickhouse/client'
import 'dotenv/config'

import * as ctfExchange from './abi/CTFExchange.js'
import * as conditionalTokens from './abi/ConditionalTokens.js'
import * as negRiskAdapter from './abi/NegRiskAdapter.js'
import * as feeModule from './abi/FeeModule.js'
import * as erc1155 from './abi/ERC1155.js'
import {
  CTF_EXCHANGE_BINARY,
  CTF_EXCHANGE_MULTI,
  CONDITIONAL_TOKENS,
  NEGRISK_ADAPTER,
  FEE_MODULE,
  NEGRISK_FEE_MODULE,
  START_BLOCK,
  USDC_ASSET_ID,
  calculatePrice,
} from './constants.js'
import type {
  Trade,
  Split,
  Merge,
  Redemption,
  Condition,
  NegRiskMarket,
  Transfer,
  AdapterSplit,
  AdapterMerge,
  AdapterRedemption,
  AdapterConversion,
  FeeRefund,
  FeeWithdrawal,
} from './tables/index.js'

// Initialize ClickHouse client
const client = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'polymarket',
  username: process.env.CLICKHOUSE_USER || 'default',
  password: process.env.CLICKHOUSE_PASSWORD || '',
})

// Initialize ClickHouse database adapter
const database = new ClickhouseDatabase({
  client,
  processorId: process.env.PROCESSOR_ID || 'polymarket-pnl',
  network: 'polygon',
  supportHotBlocks: true,
  hotBlocksDepth: 50,
  autoMigrate: true,
  migrationInterval: 100,
})

// Configure the EVM processor for Polygon
const processor = new EvmBatchProcessor()
  .setGateway(process.env.SQD_NETWORK_GATEWAY || 'https://v2.archive.subsquid.io/network/polygon-mainnet')
  .setRpcEndpoint({
    url: process.env.RPC_ENDPOINT || 'https://polygon-rpc.com',
    rateLimit: 10,
  })
  .setFinalityConfirmation(75) // Polygon finality
  .setBlockRange({ from: Number(process.env.START_BLOCK) || START_BLOCK })
  .setFields({
    log: {
      transactionHash: true,
      topics: true,
      data: true,
    },
    block: {
      timestamp: true,
    },
  })
  // CTF Exchange events (Binary markets)
  .addLog({
    address: [CTF_EXCHANGE_BINARY],
    topic0: [ctfExchange.events.OrderFilled.topic],
  })
  // CTF Exchange events (Multi-outcome markets)
  .addLog({
    address: [CTF_EXCHANGE_MULTI],
    topic0: [ctfExchange.events.OrderFilled.topic],
  })
  // Conditional Tokens events
  .addLog({
    address: [CONDITIONAL_TOKENS],
    topic0: [
      conditionalTokens.events.PositionSplit.topic,
      conditionalTokens.events.PositionsMerge.topic,
      conditionalTokens.events.PayoutRedemption.topic,
      conditionalTokens.events.ConditionPreparation.topic,
      conditionalTokens.events.ConditionResolution.topic,
      erc1155.events.TransferSingle.topic,
      erc1155.events.TransferBatch.topic,
    ],
  })
  // NegRisk adapter events
  .addLog({
    address: [NEGRISK_ADAPTER],
    topic0: [
      negRiskAdapter.events.MarketPrepared.topic,
      negRiskAdapter.events.QuestionPrepared.topic,
      negRiskAdapter.events.PositionSplit.topic,
      negRiskAdapter.events.PositionsMerge.topic,
      negRiskAdapter.events.PayoutRedemption.topic,
      negRiskAdapter.events.PositionsConverted.topic,
    ],
  })
  // Fee module events
  .addLog({
    address: [FEE_MODULE, NEGRISK_FEE_MODULE],
    topic0: [
      feeModule.events.FeeRefunded.topic,
      feeModule.events.FeeWithdrawn.topic,
    ],
  })

// Process blocks
processor.run(database, async (ctx) => {
  database.setIsAtChainTip(ctx.isHead)

  const trades: Trade[] = []
  const splits: Split[] = []
  const merges: Merge[] = []
  const redemptions: Redemption[] = []
  const conditions: Condition[] = []
  const negRiskMarkets: NegRiskMarket[] = []
  const transfers: Transfer[] = []
  const adapterSplits: AdapterSplit[] = []
  const adapterMerges: AdapterMerge[] = []
  const adapterRedemptions: AdapterRedemption[] = []
  const adapterConversions: AdapterConversion[] = []
  const feeRefunds: FeeRefund[] = []
  const feeWithdrawals: FeeWithdrawal[] = []

  const createdConditionMap = new Map<string, { created_at: Date; created_block: bigint }>()
  const resolvedConditionIds = new Set<string>()

  for (const block of ctx.blocks) {
    const blockTimestamp = new Date(block.header.timestamp)
    const blockNumber = BigInt(block.header.height)

    for (const log of block.logs) {
      const logId = `${log.transactionHash}-${log.logIndex}`

      // Handle OrderFilled from CTF Exchange
      if (
        log.topics[0] === ctfExchange.events.OrderFilled.topic &&
        (log.address.toLowerCase() === CTF_EXCHANGE_BINARY.toLowerCase() ||
          log.address.toLowerCase() === CTF_EXCHANGE_MULTI.toLowerCase())
      ) {
        const event = ctfExchange.events.OrderFilled.decode(log)

        const makerAssetId = event.makerAssetId.toString()
        const takerAssetId = event.takerAssetId.toString()

        // Determine trade direction
        // If makerAssetId is "0" (USDC), maker is BUYING tokens
        // If takerAssetId is "0" (USDC), taker is BUYING tokens
        const isMakerBuy = makerAssetId === USDC_ASSET_ID
        const isTakerBuy = takerAssetId === USDC_ASSET_ID

        // Determine which is the token and which is USDC
        const tokenId = isMakerBuy ? takerAssetId : makerAssetId
        const usdcAmount = isMakerBuy ? event.makerAmountFilled : event.takerAmountFilled
        const tokenAmount = isMakerBuy ? event.takerAmountFilled : event.makerAmountFilled

        trades.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,

          order_hash: event.orderHash,
          maker: event.maker.toLowerCase(),
          taker: event.taker.toLowerCase(),
          maker_asset_id: makerAssetId,
          taker_asset_id: takerAssetId,
          maker_amount: event.makerAmountFilled,
          taker_amount: event.takerAmountFilled,
          fee: event.fee,

          is_maker_buy: isMakerBuy,
          is_taker_buy: isTakerBuy,
          token_id: tokenId,
          usdc_amount: usdcAmount,
          token_amount: tokenAmount,
          price_per_token: calculatePrice(usdcAmount, tokenAmount),

          height: blockNumber,
        })
      }

      // Handle PositionSplit
      if (
        log.topics[0] === conditionalTokens.events.PositionSplit.topic &&
        log.address.toLowerCase() === CONDITIONAL_TOKENS.toLowerCase()
      ) {
        const event = conditionalTokens.events.PositionSplit.decode(log)

        splits.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,

          stakeholder: event.stakeholder.toLowerCase(),
          collateral_token: event.collateralToken.toLowerCase(),
          parent_collection_id: event.parentCollectionId,
          condition_id: event.conditionId,
          partition: event.partition,
          amount: event.amount,

          height: blockNumber,
        })
      }

      // Handle PositionsMerge
      if (
        log.topics[0] === conditionalTokens.events.PositionsMerge.topic &&
        log.address.toLowerCase() === CONDITIONAL_TOKENS.toLowerCase()
      ) {
        const event = conditionalTokens.events.PositionsMerge.decode(log)

        merges.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,

          stakeholder: event.stakeholder.toLowerCase(),
          collateral_token: event.collateralToken.toLowerCase(),
          parent_collection_id: event.parentCollectionId,
          condition_id: event.conditionId,
          partition: event.partition,
          amount: event.amount,

          height: blockNumber,
        })
      }

      // Handle PayoutRedemption
      if (
        log.topics[0] === conditionalTokens.events.PayoutRedemption.topic &&
        log.address.toLowerCase() === CONDITIONAL_TOKENS.toLowerCase()
      ) {
        const event = conditionalTokens.events.PayoutRedemption.decode(log)

        redemptions.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,

          redeemer: event.redeemer.toLowerCase(),
          collateral_token: event.collateralToken.toLowerCase(),
          parent_collection_id: event.parentCollectionId,
          condition_id: event.conditionId,
          index_sets: event.indexSets,
          payout: event.payout,

          height: blockNumber,
        })
      }

      // Handle ConditionPreparation
      if (
        log.topics[0] === conditionalTokens.events.ConditionPreparation.topic &&
        log.address.toLowerCase() === CONDITIONAL_TOKENS.toLowerCase()
      ) {
        const event = conditionalTokens.events.ConditionPreparation.decode(log)

        conditions.push({
          condition_id: event.conditionId,
          oracle: event.oracle.toLowerCase(),
          question_id: event.questionId,
          outcome_slot_count: Number(event.outcomeSlotCount),

          is_resolved: false,
          payout_numerators: [],
          payout_denominator: 0n,
          resolved_at: null,

          created_block: blockNumber,
          created_at: blockTimestamp,

          height: blockNumber,
        })
        createdConditionMap.set(event.conditionId, {
          created_at: blockTimestamp,
          created_block: blockNumber,
        })
      }

      // Handle ConditionResolution
      if (
        log.topics[0] === conditionalTokens.events.ConditionResolution.topic &&
        log.address.toLowerCase() === CONDITIONAL_TOKENS.toLowerCase()
      ) {
        const event = conditionalTokens.events.ConditionResolution.decode(log)
        const payoutDenominator = event.payoutNumerators.reduce((sum, v) => sum + v, 0n)

        // Update or insert condition with resolution data
        conditions.push({
          condition_id: event.conditionId,
          oracle: event.oracle.toLowerCase(),
          question_id: event.questionId,
          outcome_slot_count: Number(event.outcomeSlotCount),

          is_resolved: true,
          payout_numerators: event.payoutNumerators,
          payout_denominator: payoutDenominator,
          resolved_at: blockTimestamp,

          created_block: blockNumber, // Will be overwritten by earlier prep
          created_at: blockTimestamp, // Will be overwritten by earlier prep

          height: blockNumber,
        })
        resolvedConditionIds.add(event.conditionId)
      }

      // Handle ERC1155 TransferSingle
      if (
        log.topics[0] === erc1155.events.TransferSingle.topic &&
        log.address.toLowerCase() === CONDITIONAL_TOKENS.toLowerCase()
      ) {
        const event = erc1155.events.TransferSingle.decode(log)
        transfers.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,

          operator: event.operator.toLowerCase(),
          from: event.from.toLowerCase(),
          to: event.to.toLowerCase(),
          token_id: event.id.toString(),
          value: event.value,

          height: blockNumber,
        })
      }

      // Handle ERC1155 TransferBatch
      if (
        log.topics[0] === erc1155.events.TransferBatch.topic &&
        log.address.toLowerCase() === CONDITIONAL_TOKENS.toLowerCase()
      ) {
        const event = erc1155.events.TransferBatch.decode(log)
        for (let i = 0; i < event.ids.length; i++) {
          transfers.push({
            id: `${logId}-${i}`,
            tx_hash: log.transactionHash,
            log_index: log.logIndex,
            block_number: blockNumber,
            block_timestamp: blockTimestamp,

            operator: event.operator.toLowerCase(),
            from: event.from.toLowerCase(),
            to: event.to.toLowerCase(),
            token_id: event.ids[i].toString(),
            value: event.values[i],

            height: blockNumber,
          })
        }
      }

      // NegRiskAdapter MarketPrepared
      if (
        log.topics[0] === negRiskAdapter.events.MarketPrepared.topic &&
        log.address.toLowerCase() === NEGRISK_ADAPTER.toLowerCase()
      ) {
        const event = negRiskAdapter.events.MarketPrepared.decode(log)
        negRiskMarkets.push({
          market_id: event.marketId,
          question_count: 0,
          updated_at: blockTimestamp,
          height: blockNumber,
        })
      }

      // NegRiskAdapter QuestionPrepared
      if (
        log.topics[0] === negRiskAdapter.events.QuestionPrepared.topic &&
        log.address.toLowerCase() === NEGRISK_ADAPTER.toLowerCase()
      ) {
        const event = negRiskAdapter.events.QuestionPrepared.decode(log)
        const questionCount = Number(event.index) + 1
        negRiskMarkets.push({
          market_id: event.marketId,
          question_count: questionCount,
          updated_at: blockTimestamp,
          height: blockNumber,
        })
      }

      // NegRiskAdapter PositionSplit
      if (
        log.topics[0] === negRiskAdapter.events.PositionSplit.topic &&
        log.address.toLowerCase() === NEGRISK_ADAPTER.toLowerCase()
      ) {
        const event = negRiskAdapter.events.PositionSplit.decode(log)
        adapterSplits.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,
          stakeholder: event.stakeholder.toLowerCase(),
          condition_id: event.conditionId,
          amount: event.amount,
          height: blockNumber,
        })
      }

      // NegRiskAdapter PositionsMerge
      if (
        log.topics[0] === negRiskAdapter.events.PositionsMerge.topic &&
        log.address.toLowerCase() === NEGRISK_ADAPTER.toLowerCase()
      ) {
        const event = negRiskAdapter.events.PositionsMerge.decode(log)
        adapterMerges.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,
          stakeholder: event.stakeholder.toLowerCase(),
          condition_id: event.conditionId,
          amount: event.amount,
          height: blockNumber,
        })
      }

      // NegRiskAdapter PayoutRedemption
      if (
        log.topics[0] === negRiskAdapter.events.PayoutRedemption.topic &&
        log.address.toLowerCase() === NEGRISK_ADAPTER.toLowerCase()
      ) {
        const event = negRiskAdapter.events.PayoutRedemption.decode(log)
        adapterRedemptions.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,
          redeemer: event.redeemer.toLowerCase(),
          condition_id: event.conditionId,
          amounts: event.amounts,
          payout: event.payout,
          height: blockNumber,
        })
      }

      // NegRiskAdapter PositionsConverted
      if (
        log.topics[0] === negRiskAdapter.events.PositionsConverted.topic &&
        log.address.toLowerCase() === NEGRISK_ADAPTER.toLowerCase()
      ) {
        const event = negRiskAdapter.events.PositionsConverted.decode(log)
        adapterConversions.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,
          stakeholder: event.stakeholder.toLowerCase(),
          market_id: event.marketId,
          index_set: event.indexSet,
          amount: event.amount,
          height: blockNumber,
        })
      }

      // FeeModule events (FeeRefunded)
      if (
        log.topics[0] === feeModule.events.FeeRefunded.topic &&
        (log.address.toLowerCase() === FEE_MODULE.toLowerCase() ||
          log.address.toLowerCase() === NEGRISK_FEE_MODULE.toLowerCase())
      ) {
        const event = feeModule.events.FeeRefunded.decode(log)
        feeRefunds.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,
          module: log.address.toLowerCase(),
          order_hash: event.orderHash,
          to: event.to.toLowerCase(),
          token_id: event.id,
          refund: event.refund,
          fee_charged: event.feeCharged,
          height: blockNumber,
        })
      }

      // FeeModule events (FeeWithdrawn)
      if (
        log.topics[0] === feeModule.events.FeeWithdrawn.topic &&
        (log.address.toLowerCase() === FEE_MODULE.toLowerCase() ||
          log.address.toLowerCase() === NEGRISK_FEE_MODULE.toLowerCase())
      ) {
        const event = feeModule.events.FeeWithdrawn.decode(log)
        feeWithdrawals.push({
          id: logId,
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
          block_number: blockNumber,
          block_timestamp: blockTimestamp,
          module: log.address.toLowerCase(),
          token: event.token.toLowerCase(),
          to: event.to.toLowerCase(),
          token_id: event.id,
          amount: event.amount,
          height: blockNumber,
        })
      }
    }
  }

  if (resolvedConditionIds.size > 0) {
    const ids = Array.from(resolvedConditionIds)
    const idList = ids.map((id) => `'${id}'`).join(', ')
    if (idList.length > 0) {
      const result = await client.query({
        query: `
          SELECT
            condition_id,
            min(conditions.created_block) AS created_block,
            argMin(conditions.created_at, conditions.created_block) AS created_at
          FROM conditions FINAL
          WHERE condition_id IN (${idList})
          GROUP BY condition_id
        `,
        format: 'JSONEachRow',
      })
      const rows = await result.json() as { condition_id: string; created_block: string; created_at: string }[]
      for (const row of rows) {
        if (!createdConditionMap.has(row.condition_id)) {
          createdConditionMap.set(row.condition_id, {
            created_block: BigInt(row.created_block),
            created_at: new Date(row.created_at),
          })
        }
      }
    }
  }

  if (createdConditionMap.size > 0) {
    for (const condition of conditions) {
      if (!condition.is_resolved) continue
      const created = createdConditionMap.get(condition.condition_id)
      if (created) {
        condition.created_block = created.created_block
        condition.created_at = created.created_at
      }
    }
  }

  const serializeBigintArray = (arr: bigint[]) => arr.map((value) => value.toString())

  const tradesRows = trades.map((trade) => ({
    ...trade,
    block_number: trade.block_number.toString(),
    maker_amount: trade.maker_amount.toString(),
    taker_amount: trade.taker_amount.toString(),
    fee: trade.fee.toString(),
    usdc_amount: trade.usdc_amount.toString(),
    token_amount: trade.token_amount.toString(),
    height: trade.height.toString(),
  }))

  const splitsRows = splits.map((split) => ({
    ...split,
    block_number: split.block_number.toString(),
    partition: serializeBigintArray(split.partition),
    amount: split.amount.toString(),
    height: split.height.toString(),
  }))

  const mergesRows = merges.map((merge) => ({
    ...merge,
    block_number: merge.block_number.toString(),
    partition: serializeBigintArray(merge.partition),
    amount: merge.amount.toString(),
    height: merge.height.toString(),
  }))

  const redemptionsRows = redemptions.map((redemption) => ({
    ...redemption,
    block_number: redemption.block_number.toString(),
    index_sets: serializeBigintArray(redemption.index_sets),
    payout: redemption.payout.toString(),
    height: redemption.height.toString(),
  }))

  const conditionsRows = conditions.map((condition) => ({
    ...condition,
    payout_numerators: serializeBigintArray(condition.payout_numerators),
    payout_denominator: condition.payout_denominator.toString(),
    resolved_at: condition.resolved_at ?? new Date(0),
    created_block: condition.created_block.toString(),
    height: condition.height.toString(),
  }))

  const negRiskMarketsRows = negRiskMarkets.map((market) => ({
    ...market,
    height: market.height.toString(),
  }))

  const transfersRows = transfers.map((transfer) => ({
    ...transfer,
    block_number: transfer.block_number.toString(),
    value: transfer.value.toString(),
    height: transfer.height.toString(),
  }))

  const adapterSplitsRows = adapterSplits.map((row) => ({
    ...row,
    block_number: row.block_number.toString(),
    amount: row.amount.toString(),
    height: row.height.toString(),
  }))

  const adapterMergesRows = adapterMerges.map((row) => ({
    ...row,
    block_number: row.block_number.toString(),
    amount: row.amount.toString(),
    height: row.height.toString(),
  }))

  const adapterRedemptionsRows = adapterRedemptions.map((row) => ({
    ...row,
    block_number: row.block_number.toString(),
    amounts: serializeBigintArray(row.amounts),
    payout: row.payout.toString(),
    height: row.height.toString(),
  }))

  const adapterConversionsRows = adapterConversions.map((row) => ({
    ...row,
    block_number: row.block_number.toString(),
    index_set: row.index_set.toString(),
    amount: row.amount.toString(),
    height: row.height.toString(),
  }))

  const feeRefundsRows = feeRefunds.map((row) => ({
    ...row,
    block_number: row.block_number.toString(),
    token_id: row.token_id.toString(),
    refund: row.refund.toString(),
    fee_charged: row.fee_charged.toString(),
    height: row.height.toString(),
  }))

  const feeWithdrawalsRows = feeWithdrawals.map((row) => ({
    ...row,
    block_number: row.block_number.toString(),
    token_id: row.token_id.toString(),
    amount: row.amount.toString(),
    height: row.height.toString(),
  }))

  // Insert all data into ClickHouse
  if (tradesRows.length > 0) {
    await client.insert({
      table: 'trades',
      values: tradesRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (splitsRows.length > 0) {
    await client.insert({
      table: 'splits',
      values: splitsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (mergesRows.length > 0) {
    await client.insert({
      table: 'merges',
      values: mergesRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (redemptionsRows.length > 0) {
    await client.insert({
      table: 'redemptions',
      values: redemptionsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (conditionsRows.length > 0) {
    await client.insert({
      table: 'conditions',
      values: conditionsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (negRiskMarketsRows.length > 0) {
    await client.insert({
      table: 'neg_risk_markets',
      values: negRiskMarketsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (transfersRows.length > 0) {
    await client.insert({
      table: 'transfers',
      values: transfersRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (adapterSplitsRows.length > 0) {
    await client.insert({
      table: 'adapter_splits',
      values: adapterSplitsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (adapterMergesRows.length > 0) {
    await client.insert({
      table: 'adapter_merges',
      values: adapterMergesRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (adapterRedemptionsRows.length > 0) {
    await client.insert({
      table: 'adapter_redemptions',
      values: adapterRedemptionsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (adapterConversionsRows.length > 0) {
    await client.insert({
      table: 'adapter_conversions',
      values: adapterConversionsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (feeRefundsRows.length > 0) {
    await client.insert({
      table: 'fee_refunds',
      values: feeRefundsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }
  if (feeWithdrawalsRows.length > 0) {
    await client.insert({
      table: 'fee_withdrawals',
      values: feeWithdrawalsRows,
      format: 'JSONEachRow',
      clickhouse_settings: { date_time_input_format: 'best_effort' },
    })
  }

  const totalEvents =
    trades.length +
    splits.length +
    merges.length +
    redemptions.length +
    conditions.length +
    negRiskMarkets.length +
    transfers.length +
    adapterSplits.length +
    adapterMerges.length +
    adapterRedemptions.length +
    adapterConversions.length +
    feeRefunds.length +
    feeWithdrawals.length
  if (totalEvents > 0) {
    console.log(
      `Block ${ctx.blocks[0]?.header.height}-${ctx.blocks[ctx.blocks.length - 1]?.header.height}: ` +
      `${trades.length} trades, ${splits.length} splits, ${merges.length} merges, ` +
      `${redemptions.length} redemptions, ${conditions.length} conditions, ` +
      `${negRiskMarkets.length} neg_risk_markets, ` +
      `${transfers.length} transfers, ${adapterSplits.length} adapter_splits, ` +
      `${adapterMerges.length} adapter_merges, ${adapterRedemptions.length} adapter_redemptions, ` +
      `${adapterConversions.length} adapter_conversions, ${feeRefunds.length} fee_refunds, ` +
      `${feeWithdrawals.length} fee_withdrawals`
    )
  }
})
