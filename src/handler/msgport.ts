import {
  DataHandlerContext as EvmTronDataHandlerContext,
  Log as EvmLog,
} from "@subsquid/evm-processor";
import { Store } from "@subsquid/typeorm-store";
import {
  EventInfo,
  EvmFieldSelection,
  HandlerLifecycle,
  TronFieldSelection,
} from "../types";
import {
  DataHandlerContext as TronDataHandlerContext,
  Log as TronLog,
} from "@subsquid/tron-processor";
import * as msgportAbi from "../abi/msgport";
import * as helpers from "../helpers";
import { MsgportMessageRecv, MsgportMessageSent } from "../model";

export class MsgportEvmHandler {
  private readonly msgportHandler: MsgportHandler;

  constructor(
    private readonly ctx: EvmTronDataHandlerContext<Store, EvmFieldSelection>,
    private readonly lifecycle: HandlerLifecycle
  ) {
    this.msgportHandler = new MsgportHandler(this.ctx.store, this.lifecycle);
  }

  async handle(eventLog: EvmLog<EvmFieldSelection>) {
    const { ormpContractChain } = this.lifecycle;
    const isMessageRecv =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, msgportAbi.events.MessageRecv.topic)
      ) !== -1;
    const isMessageSend =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, msgportAbi.events.MessageSent.topic)
      ) !== -1;
    const eventInfo: EventInfo = {
      id: eventLog.id,
      chainId: BigInt(ormpContractChain.chainId),
      logIndex: eventLog.logIndex,
      address: helpers.stdHashString(eventLog.address),
      transactionIndex: eventLog.transactionIndex,
      transactionFrom: helpers.stdHashString(eventLog.getTransaction().from),
    };
    if (isMessageRecv) {
      const event = msgportAbi.events.MessageRecv.decode(eventLog);
      const entity = new MsgportMessageRecv({
        id: eventLog.id,
        chainId: eventInfo.chainId,
        msgId: helpers.stdHashString(event.msgId),
        result: event.result,
        returnData: event.returnData,
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(eventLog.transactionHash),
        transactionIndex: eventInfo.transactionIndex,
        logIndex: eventInfo.logIndex,
        portAddress: eventInfo.address,
      });
      await this.msgportHandler.storeMessageRecv(entity, eventInfo);
    }
    if (isMessageSend) {
      const event = msgportAbi.events.MessageSent.decode(eventLog);
      const entity = new MsgportMessageSent({
        id: eventLog.id,
        chainId: eventInfo.chainId,
        msgId: helpers.stdHashString(event.msgId),
        fromChainId: eventInfo.chainId,
        fromDapp: event.fromDapp,
        toChainId: event.toChainId,
        toDapp: event.toDapp,
        message: event.message,
        params: event.params,
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(eventLog.transactionHash),
        transactionIndex: eventInfo.transactionIndex,
        transactionFrom: eventInfo.transactionFrom,
        logIndex: eventInfo.logIndex,
        portAddress: eventInfo.address,
      });
      await this.msgportHandler.storeMessageSent(entity, eventInfo);
    }
  }
}

export class MsgportTronHandler {
  private readonly msgportHandler: MsgportHandler;

  constructor(
    private readonly ctx: TronDataHandlerContext<Store, TronFieldSelection>,
    private readonly lifecycle: HandlerLifecycle
  ) {
    this.msgportHandler = new MsgportHandler(this.ctx.store, this.lifecycle);
  }

  async handle(eventLog: TronLog<TronFieldSelection>) {
    const { ormpContractChain } = this.lifecycle;
    if (!eventLog.topics) {
      this.ctx.log.warn(`[msgport] no topics in event log: ${eventLog}`);
      return;
    }
    if (!eventLog.data) {
      this.ctx.log.warn(`[msgport] no event log data: ${eventLog}`);
      return;
    }
    const isMessageRecv =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, msgportAbi.events.MessageRecv.topic)
      ) !== -1;
    const isMessageSend =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, msgportAbi.events.MessageSent.topic)
      ) !== -1;

    let tx = eventLog.getTransaction();
    let eventEvm = {
      topics: eventLog.topics.map((t) => helpers.stdHashString(t)),
      data: helpers.stdHashString(eventLog.data),
    };
    const internalTx = tx.internalTransactions[eventLog.logIndex];
    const eventInfo: EventInfo = {
      id: eventLog.id,
      chainId: BigInt(ormpContractChain.chainId),
      logIndex: eventLog.logIndex,
      address: helpers.stdHashString(eventLog.address),
      transactionIndex: tx.transactionIndex,
      transactionFrom: internalTx?.callerAddress
        ? helpers.stdHashString(internalTx.callerAddress)
        : undefined,
    };
    if (isMessageRecv) {
      const event = msgportAbi.events.MessageRecv.decode(eventEvm);
      const entity = new MsgportMessageRecv({
        id: eventLog.id,
        chainId: eventInfo.chainId,
        msgId: helpers.stdHashString(event.msgId),
        result: event.result,
        returnData: event.returnData,
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(tx.hash),
        transactionIndex: eventInfo.transactionIndex,
        logIndex: eventInfo.logIndex,
        portAddress: eventInfo.address,
      });
      await this.msgportHandler.storeMessageRecv(entity, eventInfo);
    }
    if (isMessageSend) {
      const event = msgportAbi.events.MessageSent.decode(eventEvm);
      const entity = new MsgportMessageSent({
        id: eventLog.id,
        chainId: eventInfo.chainId,
        msgId: helpers.stdHashString(event.msgId),
        fromChainId: eventInfo.chainId,
        fromDapp: event.fromDapp,
        toChainId: event.toChainId,
        toDapp: event.toDapp,
        message: event.message,
        params: event.params,
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(tx.hash),
        transactionIndex: eventInfo.transactionIndex,
        transactionFrom: eventInfo.transactionFrom,
        logIndex: eventInfo.logIndex,
        portAddress: eventInfo.address,
      });
      await this.msgportHandler.storeMessageSent(entity, eventInfo);
    }
  }
}

class MsgportHandler {
  constructor(
    private readonly store: Store,
    private readonly lifecycle: HandlerLifecycle
  ) {}
  async storeMessageRecv(event: MsgportMessageRecv, eventInfo: EventInfo) {
    await this.store.insert(event);
  }

  async storeMessageSent(event: MsgportMessageSent, eventInfo: EventInfo) {
    await this.store.insert(event);
  }
}
