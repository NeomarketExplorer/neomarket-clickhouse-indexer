import * as p from '@subsquid/evm-codec'
import { event, fun, viewFun, indexed, ContractBase } from '@subsquid/evm-abi'
import type { EventParams as EParams, FunctionArguments, FunctionReturn } from '@subsquid/evm-abi'

export const events = {
    OrderFilled: event("0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6", "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)", {"orderHash": indexed(p.bytes32), "maker": indexed(p.address), "taker": indexed(p.address), "makerAssetId": p.uint256, "takerAssetId": p.uint256, "makerAmountFilled": p.uint256, "takerAmountFilled": p.uint256, "fee": p.uint256}),
    OrdersMatched: event("0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c", "OrdersMatched(bytes32,address,uint256,uint256,uint256,uint256)", {"takerOrderHash": indexed(p.bytes32), "takerOrderMaker": indexed(p.address), "makerAssetId": p.uint256, "takerAssetId": p.uint256, "makerAmountFilled": p.uint256, "takerAmountFilled": p.uint256}),
}

export class Contract extends ContractBase {
}

/// Event types
export type OrderFilledEventArgs = EParams<typeof events.OrderFilled>
export type OrdersMatchedEventArgs = EParams<typeof events.OrdersMatched>
