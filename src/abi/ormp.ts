import * as p from '@subsquid/evm-codec'
import { event, fun, viewFun, indexed, ContractBase } from '@subsquid/evm-abi'
import type { EventParams as EParams, FunctionArguments, FunctionReturn } from '@subsquid/evm-abi'

export const events = {
    AppConfigUpdated: event("0x86b44d92ad94c11b25d0f08d0f55e8e235b4d165e62e541c02e8ee19f551c97b", "AppConfigUpdated(address,address,address)", {"ua": indexed(p.address), "oracle": p.address, "relayer": p.address}),
    DefaultConfigUpdated: event("0x504f152883e6158786ddfcce63f4d4d95ce8e404b1f6e6365a06f63849b7cb95", "DefaultConfigUpdated(address,address)", {"oracle": p.address, "relayer": p.address}),
    HashImported: event("0xa931ec14fe958397dcb26e285e56292c13d77907712b51bbaa24cfc9349b789d", "HashImported(address,uint256,address,uint256,bytes32)", {"oracle": indexed(p.address), "chainId": p.uint256, "channel": p.address, "msgIndex": p.uint256, "hash": p.bytes32}),
    MessageAccepted: event("0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18", "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))", {"msgHash": indexed(p.bytes32), "message": p.struct({"channel": p.address, "index": p.uint256, "fromChainId": p.uint256, "from": p.address, "toChainId": p.uint256, "to": p.address, "gasLimit": p.uint256, "encoded": p.bytes})}),
    MessageAssigned: event("0x3832f95736b288316c84b775a004a9d17177362548ce253cba9acb4801875f4d", "MessageAssigned(bytes32,address,address,uint256,uint256,bytes)", {"msgHash": indexed(p.bytes32), "oracle": indexed(p.address), "relayer": indexed(p.address), "oracleFee": p.uint256, "relayerFee": p.uint256, "params": p.bytes}),
    MessageDispatched: event("0x62b1dc20fd6f1518626da5b6f9897e8cd4ebadbad071bb66dc96a37c970087a8", "MessageDispatched(bytes32,bool)", {"msgHash": indexed(p.bytes32), "dispatchResult": p.bool}),
    SetterChanged: event("0xb0999394a8202229d1e14c6290230863233f18473f7504b3a7b1a27f9b014c9d", "SetterChanged(address,address)", {"oldSetter": indexed(p.address), "newSetter": indexed(p.address)}),
}

export const functions = {
    LOCAL_CHAINID: viewFun("0xb0ff5e72", "LOCAL_CHAINID()", {}, p.uint256),
    changeSetter: fun("0x0f988a73", "changeSetter(address)", {"newSetter": p.address}, ),
    count: viewFun("0x06661abd", "count()", {}, p.uint256),
    defaultUC: viewFun("0x4872824f", "defaultUC()", {}, {"oracle": p.address, "relayer": p.address}),
    dones: viewFun("0xf67c907c", "dones(bytes32)", {"_0": p.bytes32}, p.bool),
    fee: viewFun("0x1c2b2da6", "fee(uint256,address,uint256,bytes,bytes)", {"toChainId": p.uint256, "ua": p.address, "gasLimit": p.uint256, "encoded": p.bytes, "params": p.bytes}, p.uint256),
    getAppConfig: viewFun("0x41342249", "getAppConfig(address)", {"ua": p.address}, p.struct({"oracle": p.address, "relayer": p.address})),
    hashLookup: viewFun("0x559e7230", "hashLookup(address,bytes32)", {"_0": p.address, "_1": p.bytes32}, p.bytes32),
    importHash: fun("0x5f3abe96", "importHash(uint256,address,uint256,bytes32)", {"chainId": p.uint256, "channel": p.address, "msgIndex": p.uint256, "hash_": p.bytes32}, ),
    recv: fun("0xee6ad167", "recv((address,uint256,uint256,address,uint256,address,uint256,bytes),bytes)", {"message": p.struct({"channel": p.address, "index": p.uint256, "fromChainId": p.uint256, "from": p.address, "toChainId": p.uint256, "to": p.address, "gasLimit": p.uint256, "encoded": p.bytes}), "proof": p.bytes}, p.bool),
    send: fun("0x5e26462c", "send(uint256,address,uint256,bytes,address,bytes)", {"toChainId": p.uint256, "to": p.address, "gasLimit": p.uint256, "encoded": p.bytes, "refund": p.address, "params": p.bytes}, p.bytes32),
    setAppConfig: fun("0x8b229091", "setAppConfig(address,address)", {"oracle": p.address, "relayer": p.address}, ),
    setDefaultConfig: fun("0x4ea59979", "setDefaultConfig(address,address)", {"oracle": p.address, "relayer": p.address}, ),
    setter: viewFun("0x3f3108f7", "setter()", {}, p.address),
    ucOf: viewFun("0x3c50e178", "ucOf(address)", {"_0": p.address}, {"oracle": p.address, "relayer": p.address}),
    version: viewFun("0x54fd4d50", "version()", {}, p.string),
}

export class Contract extends ContractBase {

    LOCAL_CHAINID() {
        return this.eth_call(functions.LOCAL_CHAINID, {})
    }

    count() {
        return this.eth_call(functions.count, {})
    }

    defaultUC() {
        return this.eth_call(functions.defaultUC, {})
    }

    dones(_0: DonesParams["_0"]) {
        return this.eth_call(functions.dones, {_0})
    }

    fee(toChainId: FeeParams["toChainId"], ua: FeeParams["ua"], gasLimit: FeeParams["gasLimit"], encoded: FeeParams["encoded"], params: FeeParams["params"]) {
        return this.eth_call(functions.fee, {toChainId, ua, gasLimit, encoded, params})
    }

    getAppConfig(ua: GetAppConfigParams["ua"]) {
        return this.eth_call(functions.getAppConfig, {ua})
    }

    hashLookup(_0: HashLookupParams["_0"], _1: HashLookupParams["_1"]) {
        return this.eth_call(functions.hashLookup, {_0, _1})
    }

    setter() {
        return this.eth_call(functions.setter, {})
    }

    ucOf(_0: UcOfParams["_0"]) {
        return this.eth_call(functions.ucOf, {_0})
    }

    version() {
        return this.eth_call(functions.version, {})
    }
}

/// Event types
export type AppConfigUpdatedEventArgs = EParams<typeof events.AppConfigUpdated>
export type DefaultConfigUpdatedEventArgs = EParams<typeof events.DefaultConfigUpdated>
export type HashImportedEventArgs = EParams<typeof events.HashImported>
export type MessageAcceptedEventArgs = EParams<typeof events.MessageAccepted>
export type MessageAssignedEventArgs = EParams<typeof events.MessageAssigned>
export type MessageDispatchedEventArgs = EParams<typeof events.MessageDispatched>
export type SetterChangedEventArgs = EParams<typeof events.SetterChanged>

/// Function types
export type LOCAL_CHAINIDParams = FunctionArguments<typeof functions.LOCAL_CHAINID>
export type LOCAL_CHAINIDReturn = FunctionReturn<typeof functions.LOCAL_CHAINID>

export type ChangeSetterParams = FunctionArguments<typeof functions.changeSetter>
export type ChangeSetterReturn = FunctionReturn<typeof functions.changeSetter>

export type CountParams = FunctionArguments<typeof functions.count>
export type CountReturn = FunctionReturn<typeof functions.count>

export type DefaultUCParams = FunctionArguments<typeof functions.defaultUC>
export type DefaultUCReturn = FunctionReturn<typeof functions.defaultUC>

export type DonesParams = FunctionArguments<typeof functions.dones>
export type DonesReturn = FunctionReturn<typeof functions.dones>

export type FeeParams = FunctionArguments<typeof functions.fee>
export type FeeReturn = FunctionReturn<typeof functions.fee>

export type GetAppConfigParams = FunctionArguments<typeof functions.getAppConfig>
export type GetAppConfigReturn = FunctionReturn<typeof functions.getAppConfig>

export type HashLookupParams = FunctionArguments<typeof functions.hashLookup>
export type HashLookupReturn = FunctionReturn<typeof functions.hashLookup>

export type ImportHashParams = FunctionArguments<typeof functions.importHash>
export type ImportHashReturn = FunctionReturn<typeof functions.importHash>

export type RecvParams = FunctionArguments<typeof functions.recv>
export type RecvReturn = FunctionReturn<typeof functions.recv>

export type SendParams = FunctionArguments<typeof functions.send>
export type SendReturn = FunctionReturn<typeof functions.send>

export type SetAppConfigParams = FunctionArguments<typeof functions.setAppConfig>
export type SetAppConfigReturn = FunctionReturn<typeof functions.setAppConfig>

export type SetDefaultConfigParams = FunctionArguments<typeof functions.setDefaultConfig>
export type SetDefaultConfigReturn = FunctionReturn<typeof functions.setDefaultConfig>

export type SetterParams = FunctionArguments<typeof functions.setter>
export type SetterReturn = FunctionReturn<typeof functions.setter>

export type UcOfParams = FunctionArguments<typeof functions.ucOf>
export type UcOfReturn = FunctionReturn<typeof functions.ucOf>

export type VersionParams = FunctionArguments<typeof functions.version>
export type VersionReturn = FunctionReturn<typeof functions.version>

