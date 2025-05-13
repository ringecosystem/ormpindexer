import {
  DataHandlerContext as EvmTronDataHandlerContext,
  Log as EvmLog,
} from "@subsquid/evm-processor";
import { Store } from "@subsquid/typeorm-store";
import {
  ADDRESS_ORACLE,
  ADDRESS_RELAYER,
  EventInfo,
  EvmFieldSelection,
  HandlerLifecycle,
  TronFieldSelection,
} from "../types";
import {
  DataHandlerContext as TronDataHandlerContext,
  Log as TronLog,
} from "@subsquid/tron-processor";
import * as ormpAbi from "../abi/ormp";
import {
  ORMPHashImported,
  ORMPMessageAccepted,
  ORMPMessageAssigned,
  ORMPMessageDispatched,
} from "../model";
import * as helpers from "../helpers";

export class OrmpEvmHandler {
  private readonly ormpHandler: OrmpHandler;
  constructor(
    private readonly ctx: EvmTronDataHandlerContext<Store, EvmFieldSelection>,
    private readonly lifecycle: HandlerLifecycle
  ) {
    this.ormpHandler = new OrmpHandler(this.ctx.store, this.lifecycle);
  }

  async handle(eventLog: EvmLog<EvmFieldSelection>) {
    const { ormpContractChain } = this.lifecycle;
    const isHashImported =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, ormpAbi.events.HashImported.topic)
      ) !== -1;
    const isMessageAccepted =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, ormpAbi.events.MessageAccepted.topic)
      ) !== -1;
    const isMessageAssigned =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, ormpAbi.events.MessageAssigned.topic)
      ) !== -1;
    const isMessageDispatched =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, ormpAbi.events.MessageDispatched.topic)
      ) !== -1;
    const eventInfo: EventInfo = {
      id: eventLog.id,
      chainId: BigInt(ormpContractChain.chainId),
      logIndex: eventLog.logIndex,
      address: eventLog.address,
      transactionIndex: eventLog.transactionIndex,
      transactionFrom: helpers.stdHashString(eventLog.getTransaction().from),
    };

    if (isHashImported) {
      const event = ormpAbi.events.HashImported.decode(eventLog);
      const entity = new ORMPHashImported({
        id: helpers.stdHashString(event.hash),
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(eventLog.transactionHash),
        chainId: eventInfo.chainId,

        srcChainId: event.chainId,
        channel: event.channel,
        msgIndex: event.msgIndex,
        targetChainId: BigInt(ormpContractChain.chainId),
        oracle: event.oracle,
        hash: helpers.stdHashString(event.hash),
      });
      await this.ormpHandler.storeHashImported(entity);
    }
    if (isMessageAccepted) {
      const event = ormpAbi.events.MessageAccepted.decode(eventLog);
      const entity = new ORMPMessageAccepted({
        id: helpers.stdHashString(event.msgHash),
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(eventLog.transactionHash),
        logIndex: eventLog.logIndex,
        chainId: eventInfo.chainId,

        msgHash: helpers.stdHashString(event.msgHash),
        channel: event.message.channel,
        index: event.message.index,
        fromChainId: event.message.fromChainId,
        from: helpers.stdHashString(event.message.from),
        toChainId: event.message.toChainId,
        to: helpers.stdHashString(event.message.to),
        gasLimit: event.message.gasLimit,
        encoded: event.message.encoded,
        // oracle: undefined,
        // oracleAssigned: undefined,
        // oracleAssignedFee: undefined,
        // relayer: undefined,
        // relayerAssigned: undefined,
        // relayerAssignedFee: undefined,
      });
      await this.ormpHandler.storeMessageAccepted(entity);
    }
    if (isMessageAssigned) {
      const event = ormpAbi.events.MessageAssigned.decode(eventLog);
      const entity = new ORMPMessageAssigned({
        id: eventLog.id,
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(eventLog.transactionHash),
        chainId: eventInfo.chainId,

        msgHash: helpers.stdHashString(event.msgHash),
        oracle: event.oracle,
        relayer: helpers.stdHashString(event.relayer),
        oracleFee: event.oracleFee,
        relayerFee: event.relayerFee,
        params: event.params,
      });
      await this.ormpHandler.storeMessageAssigned(entity);
    }
    if (isMessageDispatched) {
      const event = ormpAbi.events.MessageDispatched.decode(eventLog);
      const entity = new ORMPMessageDispatched({
        id: helpers.stdHashString(event.msgHash),
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(eventLog.transactionHash),
        chainId: eventInfo.chainId,

        targetChainId: BigInt(ormpContractChain.chainId),
        msgHash: helpers.stdHashString(event.msgHash),
        dispatchResult: event.dispatchResult,
      });
      await this.ormpHandler.storeMessageDispatched(entity, eventInfo);
    }
  }
}

export class OrmpTronHandler {
  private readonly ormpHandler: OrmpHandler;
  constructor(
    private readonly ctx: TronDataHandlerContext<Store, TronFieldSelection>,
    private readonly lifecycle: HandlerLifecycle
  ) {
    this.ormpHandler = new OrmpHandler(this.ctx.store, this.lifecycle);
  }

  async handle(eventLog: TronLog<TronFieldSelection>) {
    if (!eventLog.topics) {
      this.ctx.log.warn(`[ormp] no topics in event log: ${eventLog}`);
      return;
    }
    if (!eventLog.data) {
      this.ctx.log.warn(`[msgport] no event log data: ${eventLog}`);
      return;
    }
    const { ormpContractChain } = this.lifecycle;
    const isHashImported =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, ormpAbi.events.HashImported.topic)
      ) !== -1;
    const isMessageAccepted =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, ormpAbi.events.MessageAccepted.topic)
      ) !== -1;
    const isMessageAssigned =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, ormpAbi.events.MessageAssigned.topic)
      ) !== -1;
    const isMessageDispatched =
      eventLog.topics.findIndex((item) =>
        helpers.compareHashString(item, ormpAbi.events.MessageDispatched.topic)
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

    if (isHashImported) {
      const event = ormpAbi.events.HashImported.decode(eventEvm);
      const entity = new ORMPHashImported({
        id: helpers.stdHashString(event.hash),
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(tx.hash),
        chainId: eventInfo.chainId,

        srcChainId: event.chainId,
        channel: event.channel,
        msgIndex: event.msgIndex,
        targetChainId: BigInt(ormpContractChain.chainId),
        oracle: event.oracle,
        hash: helpers.stdHashString(event.hash),
      });
      await this.ormpHandler.storeHashImported(entity);
    }
    if (isMessageAccepted) {
      const event = ormpAbi.events.MessageAccepted.decode(eventEvm);
      const entity = new ORMPMessageAccepted({
        id: helpers.stdHashString(event.msgHash),
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(tx.hash),
        logIndex: eventLog.logIndex,
        chainId: eventInfo.chainId,

        msgHash: helpers.stdHashString(event.msgHash),
        channel: event.message.channel,
        index: event.message.index,
        fromChainId: event.message.fromChainId,
        from: helpers.stdHashString(event.message.from),
        toChainId: event.message.toChainId,
        to: helpers.stdHashString(event.message.to),
        gasLimit: event.message.gasLimit,
        encoded: event.message.encoded,
        // oracle: undefined,
        // oracleAssigned: undefined,
        // oracleAssignedFee: undefined,
        // relayer: undefined,
        // relayerAssigned: undefined,
        // relayerAssignedFee: undefined,
      });
      await this.ormpHandler.storeMessageAccepted(entity);
    }
    if (isMessageAssigned) {
      const event = ormpAbi.events.MessageAssigned.decode(eventEvm);
      const entity = new ORMPMessageAssigned({
        id: eventLog.id,
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(tx.hash),
        chainId: eventInfo.chainId,

        msgHash: helpers.stdHashString(event.msgHash),
        oracle: event.oracle,
        relayer: helpers.stdHashString(event.relayer),
        oracleFee: event.oracleFee,
        relayerFee: event.relayerFee,
        params: event.params,
      });
      await this.ormpHandler.storeMessageAssigned(entity);
    }
    if (isMessageDispatched) {
      const event = ormpAbi.events.MessageDispatched.decode(eventEvm);
      const entity = new ORMPMessageDispatched({
        id: event.msgHash,
        blockNumber: BigInt(eventLog.block.height),
        blockTimestamp: BigInt(eventLog.block.timestamp),
        transactionHash: helpers.stdHashString(tx.hash),
        chainId: eventInfo.chainId,

        targetChainId: BigInt(ormpContractChain.chainId),
        msgHash: helpers.stdHashString(event.msgHash),
        dispatchResult: event.dispatchResult,
      });
      await this.ormpHandler.storeMessageDispatched(entity, eventInfo);
    }
  }
}

class OrmpHandler {
  constructor(
    private readonly store: Store,
    private readonly lifecycle: HandlerLifecycle
  ) {}

  async storeHashImported(event: ORMPHashImported) {
    await this.store.insert(event);
  }

  async storeMessageAccepted(event: ORMPMessageAccepted) {
    await this.store.insert(event);
  }

  async storeMessageAssigned(event: ORMPMessageAssigned) {
    await this.store.insert(event);

    if (ADDRESS_RELAYER.includes(event.relayer.toLowerCase())) {
      const storedMessageAccepted = await this.store.findOne(
        ORMPMessageAccepted,
        {
          where: { id: event.msgHash },
        }
      );
      if (storedMessageAccepted) {
        storedMessageAccepted.relayer = event.relayer;
        storedMessageAccepted.relayerAssigned = true;
        storedMessageAccepted.relayerAssignedFee = event.relayerFee;
        await this.store.save(storedMessageAccepted);
      }
    }

    if (ADDRESS_ORACLE.includes(event.oracle.toLowerCase())) {
      const storedMessageAccepted = await this.store.findOne(
        ORMPMessageAccepted,
        {
          where: { id: event.msgHash },
        }
      );
      if (storedMessageAccepted) {
        storedMessageAccepted.oracle = event.oracle;
        storedMessageAccepted.oracleAssigned = true;
        storedMessageAccepted.oracleAssignedFee = event.oracleFee;
        await this.store.save(storedMessageAccepted);
      }
    }
  }

  async storeMessageDispatched(
    event: ORMPMessageDispatched,
    eventInfo: EventInfo
  ) {
    await this.store.insert(event);
  }
}
