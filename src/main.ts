import { EvmBatchProcessor } from "@subsquid/evm-processor";
import { MsgscanIndexerEvmRunner, MsgscanIndexerTronRunner } from "./runner";
import { OrmpContractChain, ormpContractChains } from "./config";
import { TronBatchProcessor } from "@subsquid/tron-processor";
import { evmFieldSelection, tronFieldSelection } from "./types";

import * as helpers from "./helpers";

interface RunProcessorOptions {
  ormpContract: OrmpContractChain;
}

async function runProcessorEvm(options: RunProcessorOptions) {
  const { ormpContract } = options;
  const processor = new EvmBatchProcessor()
    .setRpcEndpoint(ormpContract.rpcs[0])
    .setFields(evmFieldSelection)
    .setFinalityConfirmation(ormpContract.finalityConfirmation ?? 50);
  if (ormpContract.gateway) {
    processor.setGateway(ormpContract.gateway);
  }
  processor.addLog({
    range: { from: ormpContract.startBlock ?? 0 },
    address: ormpContract.contracts.map((c) => c.address.toLowerCase()),
    transaction: true,
  });

  const runner = new MsgscanIndexerEvmRunner(processor);
  await runner.run({ ormpContractChain: ormpContract });
}

async function runProcessorTron(options: RunProcessorOptions) {
  const { ormpContract } = options;
  const processor = new TronBatchProcessor()
    .setHttpApi({
      url: ormpContract.rpcs[0],
      strideConcurrency: 1,
      strideSize: 1,
    })
    .setFields(tronFieldSelection);
  if (ormpContract.gateway) {
    processor.setGateway(ormpContract.gateway);
  }
  processor.addLog({
    range: { from: ormpContract.startBlock ?? 0 },
    where: {
      address: ormpContract.contracts.map((c) =>
        helpers.stdHashString(c.address).replace("0x", "")
      ),
    },
    include: {
      transaction: true,
    },
  });
  processor.addInternalTransaction({
    range: { from: ormpContract.startBlock ?? 0 },
    include: {
      transaction: true,
    },
  });
  const runner = new MsgscanIndexerTronRunner(processor);
  await runner.run({ ormpContractChain: ormpContract });
}

async function main() {
  const enableChains = process.env.ORMPINDEX_ENABLE_CHAINS;
  const allowedChains = enableChains
    ? enableChains.split(",").map((c) => parseInt(c))
    : [];
  for (const ormpContract of ormpContractChains) {
    if (allowedChains.length) {
      if (!allowedChains.includes(ormpContract.chainId)) {
        continue;
      }
    }
    const isTron =
      ormpContract.chainId === 728126428 || ormpContract.chainId === 2494104990;
    if (isTron) {
      await runProcessorTron({ ormpContract });
    } else {
      await runProcessorEvm({ ormpContract });
    }
  }
}

main()
  .then(() => console.log("done"))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });

process.on("uncaughtException", (error) => {
  console.error(error);
});
