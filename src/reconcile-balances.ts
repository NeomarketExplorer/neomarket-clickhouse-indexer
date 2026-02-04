/**
 * Reconcile ledger holdings vs on-chain ERC1155 balances.
 *
 * Usage:
 *   tsx src/reconcile-balances.ts <wallet> [--rpc <url>] [--endTs <ts>] [--batch 100] [--tolerance 0.0001]
 */

import { ethers } from 'ethers'
import { CONDITIONAL_TOKENS, toTokenNumber } from './constants.js'
import { getOpenPositionsForWallet, closeClient } from './ledger-engine.js'

async function main() {
  const args = process.argv.slice(2)
  if (args.length < 1) {
    console.log('Usage: tsx src/reconcile-balances.ts <wallet> [--rpc <url>] [--endTs <ts>]')
    process.exit(1)
  }

  const wallet = args[0]
  const rpc = args.includes('--rpc') ? args[args.indexOf('--rpc') + 1] : (process.env.RPC_ENDPOINT || 'https://polygon-rpc.com')
  const endTs = args.includes('--endTs') ? Number(args[args.indexOf('--endTs') + 1]) : undefined
  const batchSize = args.includes('--batch') ? Number(args[args.indexOf('--batch') + 1]) : 100
  const tolerance = args.includes('--tolerance') ? Number(args[args.indexOf('--tolerance') + 1]) : 0.0001

  const positions = await getOpenPositionsForWallet(wallet, endTs)
  const tokenIds = Array.from(positions.keys())
  if (tokenIds.length === 0) {
    console.log('No open positions to reconcile.')
    await closeClient()
    return
  }

  const provider = new ethers.JsonRpcProvider(rpc)
  const abi = [
    'function balanceOfBatch(address[] accounts, uint256[] ids) view returns (uint256[] balances)',
  ]
  const contract = new ethers.Contract(CONDITIONAL_TOKENS, abi, provider)

  let mismatches = 0
  for (let i = 0; i < tokenIds.length; i += batchSize) {
    const batch = tokenIds.slice(i, i + batchSize)
    const accounts = batch.map(() => wallet)
    const balances: bigint[] = await contract.balanceOfBatch(accounts, batch)
    for (let j = 0; j < batch.length; j++) {
      const tokenId = batch[j]
      const onChain = toTokenNumber(BigInt(balances[j]))
      const ledger = positions.get(tokenId) || 0
      const diff = Math.abs(onChain - ledger)
      if (diff > tolerance) {
        mismatches += 1
        console.log(`Mismatch ${tokenId}: ledger=${ledger.toFixed(6)} chain=${onChain.toFixed(6)} diff=${diff.toFixed(6)}`)
      }
    }
  }

  console.log(`Checked ${tokenIds.length} tokenIds. Mismatches: ${mismatches}`)
  await closeClient()
}

main().catch((error) => {
  console.error('Reconcile failed:', error)
  process.exit(1)
})
