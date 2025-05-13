import * as p from '@subsquid/evm-codec'
import { event, fun, viewFun, indexed, ContractBase } from '@subsquid/evm-abi'
import type { EventParams as EParams, FunctionArguments, FunctionReturn } from '@subsquid/evm-abi'

export const events = {
    SignatureSubmittion: event("0x8b3975e4768e70d323e926e2cef0676fc9a3250437d9b8f90b52c770f0d7545f", "SignatureSubmittion(uint256,address,address,uint256,bytes,bytes)", {"chainId": indexed(p.uint256), "channel": indexed(p.address), "signer": indexed(p.address), "msgIndex": p.uint256, "signature": p.bytes, "data": p.bytes}),
}

export const functions = {
    submit: fun("0x84525c8e", "submit(uint256,address,uint256,bytes,bytes)", {"chainId": p.uint256, "channel": p.address, "msgIndex": p.uint256, "signature": p.bytes, "data": p.bytes}, ),
}

export class Contract extends ContractBase {
}

/// Event types
export type SignatureSubmittionEventArgs = EParams<typeof events.SignatureSubmittion>

/// Function types
export type SubmitParams = FunctionArguments<typeof functions.submit>
export type SubmitReturn = FunctionReturn<typeof functions.submit>

