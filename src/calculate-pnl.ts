/**
 * PnL Calculator for Polymarket (Ledger-based)
 *
 * Modes:
 * 1. REALIZED_PERIOD_ONLY: Trades opened AND closed within the period
 * 2. REALIZED_WITH_HISTORY: Trades closed in period, including those opened before
 * 3. REALIZED_PERIOD_PLUS_UNREALIZED: Mode 1 + unrealized from positions opened in period
 * 4. TOTAL_PNL: Mode 2 + unrealized from ALL open positions
 *
 * Usage: tsx src/calculate-pnl.ts <wallet> [mode] [startTimestamp] [endTimestamp]
 */

import { calculatePnl, closeClient, PnlMode } from './ledger-engine.js'

// Format result for display
function formatResult(result: Awaited<ReturnType<typeof calculatePnl>>): string {
  const modeNames = {
    [PnlMode.REALIZED_PERIOD_ONLY]: 'Realized (Period Only)',
    [PnlMode.REALIZED_WITH_HISTORY]: 'Realized (With History)',
    [PnlMode.REALIZED_PERIOD_PLUS_UNREALIZED]: 'Realized + Unrealized (Period)',
    [PnlMode.TOTAL_PNL]: 'Total PnL (All Positions)',
  }

  const fmt = (n: number) => n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

  return `
╔══════════════════════════════════════════════════════════════╗
║  POLYMARKET PnL REPORT                                       ║
╠══════════════════════════════════════════════════════════════╣
  Wallet: ${result.wallet}
  Mode:   ${modeNames[result.mode]}
  Period: ${result.periodStart.toISOString().split('T')[0]} to ${result.periodEnd.toISOString().split('T')[0]}

╠══════════════════════════════════════════════════════════════╣
║  REALIZED PnL                                                ║
╠══════════════════════════════════════════════════════════════╣
  From Sells:          $${fmt(result.realizedFromSells)}
  From Redemptions:    $${fmt(result.realizedFromRedemptions)}
  From Merges:         $${fmt(result.realizedFromMerges)}
  From Resolution Loss $${fmt(result.realizedFromResolutionLosses)}
  From Fee Refunds:    $${fmt(result.realizedFromFees)}
  ─────────────────────────────────────────────────────────────
  Total Realized:      $${fmt(result.totalRealized)}

${result.mode >= 3 ? `
╠══════════════════════════════════════════════════════════════╣
║  UNREALIZED PnL                                              ║
╠══════════════════════════════════════════════════════════════╣
  Open Position Cost:  $${fmt(result.openPositionsCost)}
  Open Position Value: $${fmt(result.openPositionsValue)}
  ─────────────────────────────────────────────────────────────
  Unrealized PnL:       $${fmt(result.unrealizedPnl)}
` : ''}
╠══════════════════════════════════════════════════════════════╣
║  TOTAL                                                       ║
╠══════════════════════════════════════════════════════════════╣
  TOTAL PnL:           $${fmt(result.totalPnl)}

╠══════════════════════════════════════════════════════════════╣
║  STATS                                                       ║
╠══════════════════════════════════════════════════════════════╣
  Total Buy Cost:      $${fmt(result.totalBuyCost)}
  Total Buy Tokens:    ${fmt(result.totalBuyTokens)}
  Total Sell Proceeds: $${fmt(result.totalSellProceeds)}
  Total Sell Tokens:   ${fmt(result.totalSellTokens)}
  Trades Processed:    ${result.tradeCount}
  Redemptions:         ${result.redemptionCount}
  Merges:              ${result.mergeCount}
╚══════════════════════════════════════════════════════════════╝
`
}

// CLI entry point
async function main() {
  const args = process.argv.slice(2)

  if (args.length < 1) {
    console.log(`
Usage: tsx src/calculate-pnl.ts <wallet> [mode] [startTimestamp] [endTimestamp]

Modes:
  1 = Realized PnL (trades opened AND closed in period)
  2 = Realized PnL (trades closed in period, any open date)
  3 = Mode 1 + Unrealized from positions opened in period
  4 = Mode 2 + ALL unrealized positions (Total PnL)

Examples:
  tsx src/calculate-pnl.ts 0x1234...
  tsx src/calculate-pnl.ts 0x1234... 2
  tsx src/calculate-pnl.ts 0x1234... 4 1704067200 1735689600
`)
    process.exit(1)
  }

  const wallet = args[0]
  const mode = (parseInt(args[1]) || 4) as PnlMode
  const startTs = args[2] ? parseInt(args[2]) : undefined
  const endTs = args[3] ? parseInt(args[3]) : undefined

  const periodStart = startTs ? new Date(startTs * 1000) : undefined
  const periodEnd = endTs ? new Date(endTs * 1000) : new Date()

  console.log(`Calculating PnL for ${wallet}...`)
  console.log(`Mode: ${mode}`)
  if (periodStart) console.log(`Period: ${periodStart.toISOString()} to ${periodEnd.toISOString()}`)

  try {
    const result = await calculatePnl(wallet, mode, periodStart, periodEnd)
    console.log(formatResult(result))
  } catch (error) {
    console.error('Error calculating PnL:', error)
    process.exit(1)
  } finally {
    await closeClient()
  }
}

main()
