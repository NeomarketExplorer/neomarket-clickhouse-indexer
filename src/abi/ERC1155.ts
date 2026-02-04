import { event, indexed } from '@subsquid/evm-abi'
import * as p from '@subsquid/evm-codec'

export const events = {
  TransferSingle: event(
    '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62',
    'TransferSingle(address,address,address,uint256,uint256)',
    {
      operator: indexed(p.address),
      from: indexed(p.address),
      to: indexed(p.address),
      id: p.uint256,
      value: p.uint256,
    }
  ),
  TransferBatch: event(
    '0x4a39dc06d4c0dbc64b70baf90fd698a233a518aa5d07e595d983b8c0526c8f7fb',
    'TransferBatch(address,address,address,uint256[],uint256[])',
    {
      operator: indexed(p.address),
      from: indexed(p.address),
      to: indexed(p.address),
      ids: p.array(p.uint256),
      values: p.array(p.uint256),
    }
  ),
}
