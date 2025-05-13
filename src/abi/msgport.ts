import * as p from '@subsquid/evm-codec'
import { event, fun, viewFun, indexed, ContractBase } from '@subsquid/evm-abi'
import type { EventParams as EParams, FunctionArguments, FunctionReturn } from '@subsquid/evm-abi'

export const events = {
    HistoryORMPAdded: event("0x8af9a700c7743999640a0e8ccf8c7a29e0c3100288901cccd7a9a6b371edbab3", "HistoryORMPAdded(address)", {"ormp": p.address}),
    HistoryORMPDeleted: event("0x933e5878af9aa566b0832891947f17052b71c97fff8a7c8a1553af62bcc2239d", "HistoryORMPDeleted(address)", {"ormp": p.address}),
    MessageRecv: event("0xea087580bb17f433441f3b6c0c0b80cae92ee74a8d7f50050388646d9ffd1431", "MessageRecv(bytes32,bool,bytes)", {"msgId": indexed(p.bytes32), "result": p.bool, "returnData": p.bytes}),
    MessageSent: event("0x40195d26d027672e04e23e34282d68c3d43ea138415b24c54fcdb9c2573e5975", "MessageSent(bytes32,address,uint256,address,bytes,bytes)", {"msgId": indexed(p.bytes32), "fromDapp": p.address, "toChainId": p.uint256, "toDapp": p.address, "message": p.bytes, "params": p.bytes}),
    OwnershipTransferStarted: event("0x38d16b8cac22d99fc7c124b9cd0de2d3fa1faef420bfe791d8c362d765e22700", "OwnershipTransferStarted(address,address)", {"previousOwner": indexed(p.address), "newOwner": indexed(p.address)}),
    OwnershipTransferred: event("0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0", "OwnershipTransferred(address,address)", {"previousOwner": indexed(p.address), "newOwner": indexed(p.address)}),
    PeerSet: event("0xcd44d3edde5f7e508840db73b2e39b71d725825fca31c7b3b5f154fe805554db", "PeerSet(uint256,address)", {"chainId": p.uint256, "peer": p.address}),
    SetORMP: event("0x2546b320a5e5d5577ddc36cd3d83b9f37f6c4d73a7d940d473cd72e52c1fd894", "SetORMP(address,address)", {"previousORMP": p.address, "currentORMP": p.address}),
    URI: event("0x3d7a9962f6da134f6896430d6867bd08e3546dbf9570df877e7cec39ba4305f0", "URI(string)", {"uri": p.string}),
}

export const functions = {
    LOCAL_CHAINID: viewFun("0xb0ff5e72", "LOCAL_CHAINID()", {}, p.uint256),
    acceptOwnership: fun("0x79ba5097", "acceptOwnership()", {}, ),
    delORMP: fun("0xea3de3d3", "delORMP(address)", {"ormp_": p.address}, ),
    fee: viewFun("0xfd937499", "fee(uint256,address,address,bytes,bytes)", {"toChainId": p.uint256, "fromDapp": p.address, "toDapp": p.address, "message": p.bytes, "params": p.bytes}, p.uint256),
    historyORMPAt: viewFun("0x4d16220d", "historyORMPAt(uint256)", {"index": p.uint256}, p.address),
    historyORMPContains: viewFun("0x968c4441", "historyORMPContains(address)", {"ormp_": p.address}, p.bool),
    historyORMPLength: viewFun("0x66c16c90", "historyORMPLength()", {}, p.uint256),
    historyORMPs: viewFun("0xfd0d5a19", "historyORMPs()", {}, p.array(p.address)),
    name: viewFun("0x06fdde03", "name()", {}, p.string),
    ormp: viewFun("0x37645118", "ormp()", {}, p.address),
    owner: viewFun("0x8da5cb5b", "owner()", {}, p.address),
    peerOf: viewFun("0xff020222", "peerOf(uint256)", {"chainId": p.uint256}, p.address),
    pendingOwner: viewFun("0xe30c3978", "pendingOwner()", {}, p.address),
    recv: fun("0x394d1bca", "recv(address,address,bytes)", {"fromDapp": p.address, "toDapp": p.address, "message": p.bytes}, ),
    renounceOwnership: fun("0x715018a6", "renounceOwnership()", {}, ),
    send: fun("0x16fe55ae", "send(uint256,address,bytes,bytes)", {"toChainId": p.uint256, "toDapp": p.address, "message": p.bytes, "params": p.bytes}, p.bytes32),
    setAppConfig: fun("0x7e136c64", "setAppConfig(address,address,address)", {"ormp_": p.address, "oracle": p.address, "relayer": p.address}, ),
    setORMP: fun("0x4d8464a6", "setORMP(address)", {"ormp_": p.address}, ),
    setPeer: fun("0x8069c497", "setPeer(uint256,address)", {"chainId": p.uint256, "peer": p.address}, ),
    setURI: fun("0x02fe5305", "setURI(string)", {"uri": p.string}, ),
    transferOwnership: fun("0xf2fde38b", "transferOwnership(address)", {"newOwner": p.address}, ),
    uri: viewFun("0xeac989f8", "uri()", {}, p.string),
}

export class Contract extends ContractBase {

    LOCAL_CHAINID() {
        return this.eth_call(functions.LOCAL_CHAINID, {})
    }

    fee(toChainId: FeeParams["toChainId"], fromDapp: FeeParams["fromDapp"], toDapp: FeeParams["toDapp"], message: FeeParams["message"], params: FeeParams["params"]) {
        return this.eth_call(functions.fee, {toChainId, fromDapp, toDapp, message, params})
    }

    historyORMPAt(index: HistoryORMPAtParams["index"]) {
        return this.eth_call(functions.historyORMPAt, {index})
    }

    historyORMPContains(ormp_: HistoryORMPContainsParams["ormp_"]) {
        return this.eth_call(functions.historyORMPContains, {ormp_})
    }

    historyORMPLength() {
        return this.eth_call(functions.historyORMPLength, {})
    }

    historyORMPs() {
        return this.eth_call(functions.historyORMPs, {})
    }

    name() {
        return this.eth_call(functions.name, {})
    }

    ormp() {
        return this.eth_call(functions.ormp, {})
    }

    owner() {
        return this.eth_call(functions.owner, {})
    }

    peerOf(chainId: PeerOfParams["chainId"]) {
        return this.eth_call(functions.peerOf, {chainId})
    }

    pendingOwner() {
        return this.eth_call(functions.pendingOwner, {})
    }

    uri() {
        return this.eth_call(functions.uri, {})
    }
}

/// Event types
export type HistoryORMPAddedEventArgs = EParams<typeof events.HistoryORMPAdded>
export type HistoryORMPDeletedEventArgs = EParams<typeof events.HistoryORMPDeleted>
export type MessageRecvEventArgs = EParams<typeof events.MessageRecv>
export type MessageSentEventArgs = EParams<typeof events.MessageSent>
export type OwnershipTransferStartedEventArgs = EParams<typeof events.OwnershipTransferStarted>
export type OwnershipTransferredEventArgs = EParams<typeof events.OwnershipTransferred>
export type PeerSetEventArgs = EParams<typeof events.PeerSet>
export type SetORMPEventArgs = EParams<typeof events.SetORMP>
export type URIEventArgs = EParams<typeof events.URI>

/// Function types
export type LOCAL_CHAINIDParams = FunctionArguments<typeof functions.LOCAL_CHAINID>
export type LOCAL_CHAINIDReturn = FunctionReturn<typeof functions.LOCAL_CHAINID>

export type AcceptOwnershipParams = FunctionArguments<typeof functions.acceptOwnership>
export type AcceptOwnershipReturn = FunctionReturn<typeof functions.acceptOwnership>

export type DelORMPParams = FunctionArguments<typeof functions.delORMP>
export type DelORMPReturn = FunctionReturn<typeof functions.delORMP>

export type FeeParams = FunctionArguments<typeof functions.fee>
export type FeeReturn = FunctionReturn<typeof functions.fee>

export type HistoryORMPAtParams = FunctionArguments<typeof functions.historyORMPAt>
export type HistoryORMPAtReturn = FunctionReturn<typeof functions.historyORMPAt>

export type HistoryORMPContainsParams = FunctionArguments<typeof functions.historyORMPContains>
export type HistoryORMPContainsReturn = FunctionReturn<typeof functions.historyORMPContains>

export type HistoryORMPLengthParams = FunctionArguments<typeof functions.historyORMPLength>
export type HistoryORMPLengthReturn = FunctionReturn<typeof functions.historyORMPLength>

export type HistoryORMPsParams = FunctionArguments<typeof functions.historyORMPs>
export type HistoryORMPsReturn = FunctionReturn<typeof functions.historyORMPs>

export type NameParams = FunctionArguments<typeof functions.name>
export type NameReturn = FunctionReturn<typeof functions.name>

export type OrmpParams = FunctionArguments<typeof functions.ormp>
export type OrmpReturn = FunctionReturn<typeof functions.ormp>

export type OwnerParams = FunctionArguments<typeof functions.owner>
export type OwnerReturn = FunctionReturn<typeof functions.owner>

export type PeerOfParams = FunctionArguments<typeof functions.peerOf>
export type PeerOfReturn = FunctionReturn<typeof functions.peerOf>

export type PendingOwnerParams = FunctionArguments<typeof functions.pendingOwner>
export type PendingOwnerReturn = FunctionReturn<typeof functions.pendingOwner>

export type RecvParams = FunctionArguments<typeof functions.recv>
export type RecvReturn = FunctionReturn<typeof functions.recv>

export type RenounceOwnershipParams = FunctionArguments<typeof functions.renounceOwnership>
export type RenounceOwnershipReturn = FunctionReturn<typeof functions.renounceOwnership>

export type SendParams = FunctionArguments<typeof functions.send>
export type SendReturn = FunctionReturn<typeof functions.send>

export type SetAppConfigParams = FunctionArguments<typeof functions.setAppConfig>
export type SetAppConfigReturn = FunctionReturn<typeof functions.setAppConfig>

export type SetORMPParams = FunctionArguments<typeof functions.setORMP>
export type SetORMPReturn = FunctionReturn<typeof functions.setORMP>

export type SetPeerParams = FunctionArguments<typeof functions.setPeer>
export type SetPeerReturn = FunctionReturn<typeof functions.setPeer>

export type SetURIParams = FunctionArguments<typeof functions.setURI>
export type SetURIReturn = FunctionReturn<typeof functions.setURI>

export type TransferOwnershipParams = FunctionArguments<typeof functions.transferOwnership>
export type TransferOwnershipReturn = FunctionReturn<typeof functions.transferOwnership>

export type UriParams = FunctionArguments<typeof functions.uri>
export type UriReturn = FunctionReturn<typeof functions.uri>

