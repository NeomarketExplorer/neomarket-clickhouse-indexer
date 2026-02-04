import * as p from '@subsquid/evm-codec'
import { event, fun, viewFun, indexed, ContractBase } from '@subsquid/evm-abi'
import type { EventParams as EParams, FunctionArguments, FunctionReturn } from '@subsquid/evm-abi'

export const events = {
    FeeRefunded: event("0xb608d2bf25d8b4b744ba23ce2ea9802ea955e216c064a62f42152fbf98958d24", "FeeRefunded(bytes32,address,uint256,uint256,uint256)", {"orderHash": indexed(p.bytes32), "to": indexed(p.address), "id": p.uint256, "refund": p.uint256, "feeCharged": indexed(p.uint256)}),
    FeeWithdrawn: event("0x6ce49f8691a80db5eb4f60cd55b14640529346a7ddf9bf8f77a423fa6a10bfdb", "FeeWithdrawn(address,address,uint256,uint256)", {"token": p.address, "to": p.address, "id": p.uint256, "amount": p.uint256}),
    NewAdmin: event("0xf9ffabca9c8276e99321725bcb43fb076a6c66a54b7f21c4e8146d8519b417dc", "NewAdmin(address,address)", {"admin": indexed(p.address), "newAdminAddress": indexed(p.address)}),
    RemovedAdmin: event("0x787a2e12f4a55b658b8f573c32432ee11a5e8b51677d1e1e937aaf6a0bb5776e", "RemovedAdmin(address,address)", {"admin": indexed(p.address), "removedAdmin": indexed(p.address)}),
}

export const functions = {
    addAdmin: fun("0x70480275", "addAdmin(address)", {"admin": p.address}, ),
    admins: viewFun("0x429b62e5", "admins(address)", {"_0": p.address}, p.uint256),
    collateral: viewFun("0xd8dfeb45", "collateral()", {}, p.address),
    ctf: viewFun("0x22a9339f", "ctf()", {}, p.address),
    exchange: viewFun("0xd2f7265a", "exchange()", {}, p.address),
    isAdmin: viewFun("0x24d7806c", "isAdmin(address)", {"addr": p.address}, p.bool),
    matchOrders: fun("0x2287e350", "matchOrders((uint256,address,address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint8,uint8,bytes),(uint256,address,address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint8,uint8,bytes)[],uint256,uint256,uint256[],uint256,uint256[])", {"takerOrder": p.struct({"salt": p.uint256, "maker": p.address, "signer": p.address, "taker": p.address, "tokenId": p.uint256, "makerAmount": p.uint256, "takerAmount": p.uint256, "expiration": p.uint256, "nonce": p.uint256, "feeRateBps": p.uint256, "side": p.uint8, "signatureType": p.uint8, "signature": p.bytes}), "makerOrders": p.array(p.struct({"salt": p.uint256, "maker": p.address, "signer": p.address, "taker": p.address, "tokenId": p.uint256, "makerAmount": p.uint256, "takerAmount": p.uint256, "expiration": p.uint256, "nonce": p.uint256, "feeRateBps": p.uint256, "side": p.uint8, "signatureType": p.uint8, "signature": p.bytes})), "takerFillAmount": p.uint256, "takerReceiveAmount": p.uint256, "makerFillAmounts": p.array(p.uint256), "takerFeeAmount": p.uint256, "makerFeeAmounts": p.array(p.uint256)}, ),
    onERC1155BatchReceived: fun("0xbc197c81", "onERC1155BatchReceived(address,address,uint256[],uint256[],bytes)", {"_0": p.address, "_1": p.address, "_2": p.array(p.uint256), "_3": p.array(p.uint256), "_4": p.bytes}, p.bytes4),
    onERC1155Received: fun("0xf23a6e61", "onERC1155Received(address,address,uint256,uint256,bytes)", {"_0": p.address, "_1": p.address, "_2": p.uint256, "_3": p.uint256, "_4": p.bytes}, p.bytes4),
    removeAdmin: fun("0x1785f53c", "removeAdmin(address)", {"admin": p.address}, ),
    renounceAdmin: fun("0x8bad0c0a", "renounceAdmin()", {}, ),
    withdrawFees: fun("0x425c2096", "withdrawFees(address,uint256,uint256)", {"to": p.address, "id": p.uint256, "amount": p.uint256}, ),
}

export class Contract extends ContractBase {

    admins(_0: AdminsParams["_0"]) {
        return this.eth_call(functions.admins, {_0})
    }

    collateral() {
        return this.eth_call(functions.collateral, {})
    }

    ctf() {
        return this.eth_call(functions.ctf, {})
    }

    exchange() {
        return this.eth_call(functions.exchange, {})
    }

    isAdmin(addr: IsAdminParams["addr"]) {
        return this.eth_call(functions.isAdmin, {addr})
    }
}

/// Event types
export type FeeRefundedEventArgs = EParams<typeof events.FeeRefunded>
export type FeeWithdrawnEventArgs = EParams<typeof events.FeeWithdrawn>
export type NewAdminEventArgs = EParams<typeof events.NewAdmin>
export type RemovedAdminEventArgs = EParams<typeof events.RemovedAdmin>

/// Function types
export type AddAdminParams = FunctionArguments<typeof functions.addAdmin>
export type AddAdminReturn = FunctionReturn<typeof functions.addAdmin>

export type AdminsParams = FunctionArguments<typeof functions.admins>
export type AdminsReturn = FunctionReturn<typeof functions.admins>

export type CollateralParams = FunctionArguments<typeof functions.collateral>
export type CollateralReturn = FunctionReturn<typeof functions.collateral>

export type CtfParams = FunctionArguments<typeof functions.ctf>
export type CtfReturn = FunctionReturn<typeof functions.ctf>

export type ExchangeParams = FunctionArguments<typeof functions.exchange>
export type ExchangeReturn = FunctionReturn<typeof functions.exchange>

export type IsAdminParams = FunctionArguments<typeof functions.isAdmin>
export type IsAdminReturn = FunctionReturn<typeof functions.isAdmin>

export type MatchOrdersParams = FunctionArguments<typeof functions.matchOrders>
export type MatchOrdersReturn = FunctionReturn<typeof functions.matchOrders>

export type OnERC1155BatchReceivedParams = FunctionArguments<typeof functions.onERC1155BatchReceived>
export type OnERC1155BatchReceivedReturn = FunctionReturn<typeof functions.onERC1155BatchReceived>

export type OnERC1155ReceivedParams = FunctionArguments<typeof functions.onERC1155Received>
export type OnERC1155ReceivedReturn = FunctionReturn<typeof functions.onERC1155Received>

export type RemoveAdminParams = FunctionArguments<typeof functions.removeAdmin>
export type RemoveAdminReturn = FunctionReturn<typeof functions.removeAdmin>

export type RenounceAdminParams = FunctionArguments<typeof functions.renounceAdmin>
export type RenounceAdminReturn = FunctionReturn<typeof functions.renounceAdmin>

export type WithdrawFeesParams = FunctionArguments<typeof functions.withdrawFees>
export type WithdrawFeesReturn = FunctionReturn<typeof functions.withdrawFees>

