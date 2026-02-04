import * as p from '@subsquid/evm-codec'
import { event, fun, viewFun, indexed, ContractBase } from '@subsquid/evm-abi'
import type { EventParams as EParams, FunctionArguments, FunctionReturn } from '@subsquid/evm-abi'

export const events = {
    MarketPrepared: event("0xf059ab16d1ca60e123eab60e3c02b68faf060347c701a5d14885a8e1def7b3a8", "MarketPrepared(bytes32,address,uint256,bytes)", {"marketId": indexed(p.bytes32), "oracle": indexed(p.address), "feeBips": p.uint256, "data": p.bytes}),
    NewAdmin: event("0xf9ffabca9c8276e99321725bcb43fb076a6c66a54b7f21c4e8146d8519b417dc", "NewAdmin(address,address)", {"admin": indexed(p.address), "newAdminAddress": indexed(p.address)}),
    OutcomeReported: event("0x9e9fa7fd355160bd4cd3f22d4333519354beff1f5689bde87f2c5e63d8d484b2", "OutcomeReported(bytes32,bytes32,bool)", {"marketId": indexed(p.bytes32), "questionId": indexed(p.bytes32), "outcome": p.bool}),
    PayoutRedemption: event("0x9140a6a270ef945260c03894b3c6b3b2695e9d5101feef0ff24fec960cfd3224", "PayoutRedemption(address,bytes32,uint256[],uint256)", {"redeemer": indexed(p.address), "conditionId": indexed(p.bytes32), "amounts": p.array(p.uint256), "payout": p.uint256}),
    PositionSplit: event("0xbbed930dbfb7907ae2d60ddf78345610214f26419a0128df39b6cc3d9e5df9b0", "PositionSplit(address,bytes32,uint256)", {"stakeholder": indexed(p.address), "conditionId": indexed(p.bytes32), "amount": p.uint256}),
    PositionsConverted: event("0xb03d19dddbc72a87e735ff0ea3b57bef133ebe44e1894284916a84044deb367e", "PositionsConverted(address,bytes32,uint256,uint256)", {"stakeholder": indexed(p.address), "marketId": indexed(p.bytes32), "indexSet": indexed(p.uint256), "amount": p.uint256}),
    PositionsMerge: event("0xba33ac50d8894676597e6e35dc09cff59854708b642cd069d21eb9c7ca072a04", "PositionsMerge(address,bytes32,uint256)", {"stakeholder": indexed(p.address), "conditionId": indexed(p.bytes32), "amount": p.uint256}),
    QuestionPrepared: event("0xaac410f87d423a922a7b226ac68f0c2eaf5bf6d15e644ac0758c7f96e2c253f7", "QuestionPrepared(bytes32,bytes32,uint256,bytes)", {"marketId": indexed(p.bytes32), "questionId": indexed(p.bytes32), "index": p.uint256, "data": p.bytes}),
    RemovedAdmin: event("0x787a2e12f4a55b658b8f573c32432ee11a5e8b51677d1e1e937aaf6a0bb5776e", "RemovedAdmin(address,address)", {"admin": indexed(p.address), "removedAdmin": indexed(p.address)}),
}

export const functions = {
    FEE_DENOMINATOR: viewFun("0xd73792a9", "FEE_DENOMINATOR()", {}, p.uint256),
    NO_TOKEN_BURN_ADDRESS: viewFun("0x7ad7fe36", "NO_TOKEN_BURN_ADDRESS()", {}, p.address),
    addAdmin: fun("0x70480275", "addAdmin(address)", {"admin": p.address}, ),
    admins: viewFun("0x429b62e5", "admins(address)", {"_0": p.address}, p.uint256),
    balanceOf: viewFun("0x00fdd58e", "balanceOf(address,uint256)", {"_owner": p.address, "_id": p.uint256}, p.uint256),
    balanceOfBatch: viewFun("0x4e1273f4", "balanceOfBatch(address[],uint256[])", {"_owners": p.array(p.address), "_ids": p.array(p.uint256)}, p.array(p.uint256)),
    col: viewFun("0xa78695b0", "col()", {}, p.address),
    convertPositions: fun("0xc64748c4", "convertPositions(bytes32,uint256,uint256)", {"_marketId": p.bytes32, "_indexSet": p.uint256, "_amount": p.uint256}, ),
    ctf: viewFun("0x22a9339f", "ctf()", {}, p.address),
    getConditionId: viewFun("0x04329c03", "getConditionId(bytes32)", {"_questionId": p.bytes32}, p.bytes32),
    getDetermined: viewFun("0x7ae2e67b", "getDetermined(bytes32)", {"_marketId": p.bytes32}, p.bool),
    getFeeBips: viewFun("0x2582cb5e", "getFeeBips(bytes32)", {"_marketId": p.bytes32}, p.uint256),
    getMarketData: viewFun("0x30f4f4bb", "getMarketData(bytes32)", {"_marketId": p.bytes32}, p.bytes32),
    getOracle: viewFun("0xdafaf94a", "getOracle(bytes32)", {"_marketId": p.bytes32}, p.address),
    getPositionId: viewFun("0x752b5ba5", "getPositionId(bytes32,bool)", {"_questionId": p.bytes32, "_outcome": p.bool}, p.uint256),
    getQuestionCount: viewFun("0xb7f75d2c", "getQuestionCount(bytes32)", {"_marketId": p.bytes32}, p.uint256),
    getResult: viewFun("0xadd4c784", "getResult(bytes32)", {"_marketId": p.bytes32}, p.uint256),
    isAdmin: viewFun("0x24d7806c", "isAdmin(address)", {"addr": p.address}, p.bool),
    'mergePositions(address,bytes32,bytes32,uint256[],uint256)': fun("0x9e7212ad", "mergePositions(address,bytes32,bytes32,uint256[],uint256)", {"_collateralToken": p.address, "_1": p.bytes32, "_conditionId": p.bytes32, "_3": p.array(p.uint256), "_amount": p.uint256}, ),
    'mergePositions(bytes32,uint256)': fun("0xb10c5c17", "mergePositions(bytes32,uint256)", {"_conditionId": p.bytes32, "_amount": p.uint256}, ),
    onERC1155BatchReceived: fun("0xbc197c81", "onERC1155BatchReceived(address,address,uint256[],uint256[],bytes)", {"_0": p.address, "_1": p.address, "_2": p.array(p.uint256), "_3": p.array(p.uint256), "_4": p.bytes}, p.bytes4),
    onERC1155Received: fun("0xf23a6e61", "onERC1155Received(address,address,uint256,uint256,bytes)", {"_0": p.address, "_1": p.address, "_2": p.uint256, "_3": p.uint256, "_4": p.bytes}, p.bytes4),
    prepareMarket: fun("0x8a0db615", "prepareMarket(uint256,bytes)", {"_feeBips": p.uint256, "_metadata": p.bytes}, p.bytes32),
    prepareQuestion: fun("0x1d69b48d", "prepareQuestion(bytes32,bytes)", {"_marketId": p.bytes32, "_metadata": p.bytes}, p.bytes32),
    redeemPositions: fun("0xdbeccb23", "redeemPositions(bytes32,uint256[])", {"_conditionId": p.bytes32, "_amounts": p.array(p.uint256)}, ),
    removeAdmin: fun("0x1785f53c", "removeAdmin(address)", {"admin": p.address}, ),
    renounceAdmin: fun("0x8bad0c0a", "renounceAdmin()", {}, ),
    reportOutcome: fun("0xe200affd", "reportOutcome(bytes32,bool)", {"_questionId": p.bytes32, "_outcome": p.bool}, ),
    safeTransferFrom: fun("0xf242432a", "safeTransferFrom(address,address,uint256,uint256,bytes)", {"_from": p.address, "_to": p.address, "_id": p.uint256, "_value": p.uint256, "_data": p.bytes}, ),
    'splitPosition(address,bytes32,bytes32,uint256[],uint256)': fun("0x72ce4275", "splitPosition(address,bytes32,bytes32,uint256[],uint256)", {"_collateralToken": p.address, "_1": p.bytes32, "_conditionId": p.bytes32, "_3": p.array(p.uint256), "_amount": p.uint256}, ),
    'splitPosition(bytes32,uint256)': fun("0xa3d7da1d", "splitPosition(bytes32,uint256)", {"_conditionId": p.bytes32, "_amount": p.uint256}, ),
    vault: viewFun("0xfbfa77cf", "vault()", {}, p.address),
    wcol: viewFun("0x7e3b74c3", "wcol()", {}, p.address),
}

export class Contract extends ContractBase {

    FEE_DENOMINATOR() {
        return this.eth_call(functions.FEE_DENOMINATOR, {})
    }

    NO_TOKEN_BURN_ADDRESS() {
        return this.eth_call(functions.NO_TOKEN_BURN_ADDRESS, {})
    }

    admins(_0: AdminsParams["_0"]) {
        return this.eth_call(functions.admins, {_0})
    }

    balanceOf(_owner: BalanceOfParams["_owner"], _id: BalanceOfParams["_id"]) {
        return this.eth_call(functions.balanceOf, {_owner, _id})
    }

    balanceOfBatch(_owners: BalanceOfBatchParams["_owners"], _ids: BalanceOfBatchParams["_ids"]) {
        return this.eth_call(functions.balanceOfBatch, {_owners, _ids})
    }

    col() {
        return this.eth_call(functions.col, {})
    }

    ctf() {
        return this.eth_call(functions.ctf, {})
    }

    getConditionId(_questionId: GetConditionIdParams["_questionId"]) {
        return this.eth_call(functions.getConditionId, {_questionId})
    }

    getDetermined(_marketId: GetDeterminedParams["_marketId"]) {
        return this.eth_call(functions.getDetermined, {_marketId})
    }

    getFeeBips(_marketId: GetFeeBipsParams["_marketId"]) {
        return this.eth_call(functions.getFeeBips, {_marketId})
    }

    getMarketData(_marketId: GetMarketDataParams["_marketId"]) {
        return this.eth_call(functions.getMarketData, {_marketId})
    }

    getOracle(_marketId: GetOracleParams["_marketId"]) {
        return this.eth_call(functions.getOracle, {_marketId})
    }

    getPositionId(_questionId: GetPositionIdParams["_questionId"], _outcome: GetPositionIdParams["_outcome"]) {
        return this.eth_call(functions.getPositionId, {_questionId, _outcome})
    }

    getQuestionCount(_marketId: GetQuestionCountParams["_marketId"]) {
        return this.eth_call(functions.getQuestionCount, {_marketId})
    }

    getResult(_marketId: GetResultParams["_marketId"]) {
        return this.eth_call(functions.getResult, {_marketId})
    }

    isAdmin(addr: IsAdminParams["addr"]) {
        return this.eth_call(functions.isAdmin, {addr})
    }

    vault() {
        return this.eth_call(functions.vault, {})
    }

    wcol() {
        return this.eth_call(functions.wcol, {})
    }
}

/// Event types
export type MarketPreparedEventArgs = EParams<typeof events.MarketPrepared>
export type NewAdminEventArgs = EParams<typeof events.NewAdmin>
export type OutcomeReportedEventArgs = EParams<typeof events.OutcomeReported>
export type PayoutRedemptionEventArgs = EParams<typeof events.PayoutRedemption>
export type PositionSplitEventArgs = EParams<typeof events.PositionSplit>
export type PositionsConvertedEventArgs = EParams<typeof events.PositionsConverted>
export type PositionsMergeEventArgs = EParams<typeof events.PositionsMerge>
export type QuestionPreparedEventArgs = EParams<typeof events.QuestionPrepared>
export type RemovedAdminEventArgs = EParams<typeof events.RemovedAdmin>

/// Function types
export type FEE_DENOMINATORParams = FunctionArguments<typeof functions.FEE_DENOMINATOR>
export type FEE_DENOMINATORReturn = FunctionReturn<typeof functions.FEE_DENOMINATOR>

export type NO_TOKEN_BURN_ADDRESSParams = FunctionArguments<typeof functions.NO_TOKEN_BURN_ADDRESS>
export type NO_TOKEN_BURN_ADDRESSReturn = FunctionReturn<typeof functions.NO_TOKEN_BURN_ADDRESS>

export type AddAdminParams = FunctionArguments<typeof functions.addAdmin>
export type AddAdminReturn = FunctionReturn<typeof functions.addAdmin>

export type AdminsParams = FunctionArguments<typeof functions.admins>
export type AdminsReturn = FunctionReturn<typeof functions.admins>

export type BalanceOfParams = FunctionArguments<typeof functions.balanceOf>
export type BalanceOfReturn = FunctionReturn<typeof functions.balanceOf>

export type BalanceOfBatchParams = FunctionArguments<typeof functions.balanceOfBatch>
export type BalanceOfBatchReturn = FunctionReturn<typeof functions.balanceOfBatch>

export type ColParams = FunctionArguments<typeof functions.col>
export type ColReturn = FunctionReturn<typeof functions.col>

export type ConvertPositionsParams = FunctionArguments<typeof functions.convertPositions>
export type ConvertPositionsReturn = FunctionReturn<typeof functions.convertPositions>

export type CtfParams = FunctionArguments<typeof functions.ctf>
export type CtfReturn = FunctionReturn<typeof functions.ctf>

export type GetConditionIdParams = FunctionArguments<typeof functions.getConditionId>
export type GetConditionIdReturn = FunctionReturn<typeof functions.getConditionId>

export type GetDeterminedParams = FunctionArguments<typeof functions.getDetermined>
export type GetDeterminedReturn = FunctionReturn<typeof functions.getDetermined>

export type GetFeeBipsParams = FunctionArguments<typeof functions.getFeeBips>
export type GetFeeBipsReturn = FunctionReturn<typeof functions.getFeeBips>

export type GetMarketDataParams = FunctionArguments<typeof functions.getMarketData>
export type GetMarketDataReturn = FunctionReturn<typeof functions.getMarketData>

export type GetOracleParams = FunctionArguments<typeof functions.getOracle>
export type GetOracleReturn = FunctionReturn<typeof functions.getOracle>

export type GetPositionIdParams = FunctionArguments<typeof functions.getPositionId>
export type GetPositionIdReturn = FunctionReturn<typeof functions.getPositionId>

export type GetQuestionCountParams = FunctionArguments<typeof functions.getQuestionCount>
export type GetQuestionCountReturn = FunctionReturn<typeof functions.getQuestionCount>

export type GetResultParams = FunctionArguments<typeof functions.getResult>
export type GetResultReturn = FunctionReturn<typeof functions.getResult>

export type IsAdminParams = FunctionArguments<typeof functions.isAdmin>
export type IsAdminReturn = FunctionReturn<typeof functions.isAdmin>

export type MergePositionsParams_0 = FunctionArguments<typeof functions['mergePositions(address,bytes32,bytes32,uint256[],uint256)']>
export type MergePositionsReturn_0 = FunctionReturn<typeof functions['mergePositions(address,bytes32,bytes32,uint256[],uint256)']>

export type MergePositionsParams_1 = FunctionArguments<typeof functions['mergePositions(bytes32,uint256)']>
export type MergePositionsReturn_1 = FunctionReturn<typeof functions['mergePositions(bytes32,uint256)']>

export type OnERC1155BatchReceivedParams = FunctionArguments<typeof functions.onERC1155BatchReceived>
export type OnERC1155BatchReceivedReturn = FunctionReturn<typeof functions.onERC1155BatchReceived>

export type OnERC1155ReceivedParams = FunctionArguments<typeof functions.onERC1155Received>
export type OnERC1155ReceivedReturn = FunctionReturn<typeof functions.onERC1155Received>

export type PrepareMarketParams = FunctionArguments<typeof functions.prepareMarket>
export type PrepareMarketReturn = FunctionReturn<typeof functions.prepareMarket>

export type PrepareQuestionParams = FunctionArguments<typeof functions.prepareQuestion>
export type PrepareQuestionReturn = FunctionReturn<typeof functions.prepareQuestion>

export type RedeemPositionsParams = FunctionArguments<typeof functions.redeemPositions>
export type RedeemPositionsReturn = FunctionReturn<typeof functions.redeemPositions>

export type RemoveAdminParams = FunctionArguments<typeof functions.removeAdmin>
export type RemoveAdminReturn = FunctionReturn<typeof functions.removeAdmin>

export type RenounceAdminParams = FunctionArguments<typeof functions.renounceAdmin>
export type RenounceAdminReturn = FunctionReturn<typeof functions.renounceAdmin>

export type ReportOutcomeParams = FunctionArguments<typeof functions.reportOutcome>
export type ReportOutcomeReturn = FunctionReturn<typeof functions.reportOutcome>

export type SafeTransferFromParams = FunctionArguments<typeof functions.safeTransferFrom>
export type SafeTransferFromReturn = FunctionReturn<typeof functions.safeTransferFrom>

export type SplitPositionParams_0 = FunctionArguments<typeof functions['splitPosition(address,bytes32,bytes32,uint256[],uint256)']>
export type SplitPositionReturn_0 = FunctionReturn<typeof functions['splitPosition(address,bytes32,bytes32,uint256[],uint256)']>

export type SplitPositionParams_1 = FunctionArguments<typeof functions['splitPosition(bytes32,uint256)']>
export type SplitPositionReturn_1 = FunctionReturn<typeof functions['splitPosition(bytes32,uint256)']>

export type VaultParams = FunctionArguments<typeof functions.vault>
export type VaultReturn = FunctionReturn<typeof functions.vault>

export type WcolParams = FunctionArguments<typeof functions.wcol>
export type WcolReturn = FunctionReturn<typeof functions.wcol>

