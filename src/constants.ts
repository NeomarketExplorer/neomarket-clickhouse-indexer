// Polymarket Contract Addresses on Polygon

// CTF Exchange - Binary markets (YES/NO)
export const CTF_EXCHANGE_BINARY = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E'

// CTF Exchange - Multi-outcome markets (NegRisk)
export const CTF_EXCHANGE_MULTI = '0xC5d563A36AE78145C45a50134d48A1215220f80a'

// Gnosis Conditional Tokens Framework on Polygon
export const CONDITIONAL_TOKENS = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'

// USDC on Polygon (6 decimals)
export const USDC_ADDRESS = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'

// Asset ID "0" represents USDC in trades
export const USDC_ASSET_ID = '0'

// NegRisk / fee module addresses (from polymarket-subgraph-main)
export const NEGRISK_ADAPTER = '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296'
export const NEGRISK_WRAPPED_COLLATERAL = '0x3A3BD7bb9528E159577F7C2e685CC81A765002E2'
export const FEE_MODULE = '0xE3f18aCc55091e2c48d883fc8C8413319d4Ab7b0'
export const NEGRISK_FEE_MODULE = '0xB768891e3130F6dF18214Ac804d4DB76c2C37730'

// Decimals
export const USDC_DECIMALS = 6
export const USDC_SCALE = 10n ** 6n
export const TOKEN_DECIMALS = 18
export const TOKEN_SCALE = 10n ** 18n

// Starting block - Polymarket started around this block on Polygon
// Use the earliest relevant contract deployment (ConditionalTokens on Polygon)
export const START_BLOCK = 4023686

// For calculating price: amount is in USDC with 6 decimals
export function toUsdcNumber(amount: bigint): number {
  return Number(amount) / Number(USDC_SCALE)
}

export function toTokenNumber(amount: bigint): number {
  return Number(amount) / Number(TOKEN_SCALE)
}

// Calculate price per token
export function calculatePrice(usdcAmount: bigint, tokenAmount: bigint): number {
  if (tokenAmount === 0n) return 0
  return toUsdcNumber(usdcAmount) / toTokenNumber(tokenAmount)
}
