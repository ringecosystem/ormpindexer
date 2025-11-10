import { OrmpContractChain, OrmpContractConfig } from "./config";

export const evmFieldSelection = {
  transaction: {
    from: true,
    value: true,
    hash: true,
  },
  log: {
    transactionHash: true,
  },
};

export const tronFieldSelection = {
  block: {
    timestamp: true,
  },
  transaction: {
    hash: true,
  },
  log: {
    address: true,
    data: true,
    topics: true,
  },
  internalTransaction: {
    transferToAddress: true,
  },
};

export const ADDRESS_RELAYER = [
  "0x114890eB7386F94eae410186F20968bFAf66142a",
  "0xB607762F43F1A72593715497d4A7dDD754c62a6A", // TSZgvR9xTGeG3RXcUKnWWcUAAAEskXdCHj
].map((a) => a.toLowerCase());
export const ADDRESS_ORACLE = [
  "0x8d8a2Bd991c1d900C59a82a2EEb0DF44e0671aaB",
  "0x2cDc7178013de451ED99607AC15dEF6BAB8C37E6", // TE4QpAUb7vJYJRxsfxfBEcUPP9CS1aoTi4
].map((a) => a.toLowerCase());
export const ADDRESS_SIGNATURE = [
  "0x13b2211a7cA45Db2808F6dB05557ce5347e3634e",
  "0x5C5c383FEbE62F377F8c0eA1de97F2a2Ba102e98", // TJPZeFEdc4TBEcNbku5xVZLQ6B2Q1oGnd1
  "0x924A4b87900A8CE8F8Cf62360Db047C4e4fFC1a3", // Tron Shasta TPJifBA5MvFf918VYnajd2XmEept4iBX55
].map((a) => a.toLowerCase());

export type EvmFieldSelection = typeof evmFieldSelection;
export type TronFieldSelection = typeof tronFieldSelection;

export interface EventInfo {
  id: string;
  chainId: bigint;
  logIndex: number;
  address: string;
  transactionIndex: number;
  transactionFrom?: string;
}

export interface HandlerLifecycle {
  ormpContractChain: OrmpContractChain;
  ormpContractConfig: OrmpContractConfig;
}

