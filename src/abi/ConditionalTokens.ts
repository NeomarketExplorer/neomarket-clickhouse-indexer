import * as p from '@subsquid/evm-codec'
import { event, fun, viewFun, indexed, ContractBase } from '@subsquid/evm-abi'
import type { EventParams as EParams, FunctionArguments, FunctionReturn } from '@subsquid/evm-abi'

export const events = {
    PositionSplit: event("0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298", "PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)", {"stakeholder": indexed(p.address), "collateralToken": p.address, "parentCollectionId": indexed(p.bytes32), "conditionId": indexed(p.bytes32), "partition": p.array(p.uint256), "amount": p.uint256}),
    PositionsMerge: event("0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca", "PositionsMerge(address,address,bytes32,bytes32,uint256[],uint256)", {"stakeholder": indexed(p.address), "collateralToken": p.address, "parentCollectionId": indexed(p.bytes32), "conditionId": indexed(p.bytes32), "partition": p.array(p.uint256), "amount": p.uint256}),
    PayoutRedemption: event("0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d", "PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)", {"redeemer": indexed(p.address), "collateralToken": indexed(p.address), "parentCollectionId": indexed(p.bytes32), "conditionId": p.bytes32, "indexSets": p.array(p.uint256), "payout": p.uint256}),
    ConditionPreparation: event("0xab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177", "ConditionPreparation(bytes32,address,bytes32,uint256)", {"conditionId": indexed(p.bytes32), "oracle": indexed(p.address), "questionId": indexed(p.bytes32), "outcomeSlotCount": p.uint256}),
    ConditionResolution: event("0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894", "ConditionResolution(bytes32,address,bytes32,uint256,uint256[])", {"conditionId": indexed(p.bytes32), "oracle": indexed(p.address), "questionId": indexed(p.bytes32), "outcomeSlotCount": p.uint256, "payoutNumerators": p.array(p.uint256)}),
}

export class Contract extends ContractBase {
}

/// Event types
export type PositionSplitEventArgs = EParams<typeof events.PositionSplit>
export type PositionsMergeEventArgs = EParams<typeof events.PositionsMerge>
export type PayoutRedemptionEventArgs = EParams<typeof events.PayoutRedemption>
export type ConditionPreparationEventArgs = EParams<typeof events.ConditionPreparation>
export type ConditionResolutionEventArgs = EParams<typeof events.ConditionResolution>
