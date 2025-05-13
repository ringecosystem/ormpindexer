import { EvmBatchProcessor } from "@subsquid/evm-processor";
import { TypeormDatabase } from "@subsquid/typeorm-store";
import { OrmpContractChain } from "./config";
import { TronBatchProcessor } from "@subsquid/tron-processor";
import {
  EvmFieldSelection,
  HandlerLifecycle,
  TronFieldSelection,
} from "./types";
import { MsgportEvmHandler, MsgportTronHandler } from "./handler/msgport";
import { OrmpEvmHandler, OrmpTronHandler } from "./handler/ormp";
import { SigncribeEvmHandler } from "./handler/signcribe";
import * as helpers from "./helpers";

export interface RunnterOptions {
  ormpContractChain: OrmpContractChain;
}

export class MsgscanIndexerTronRunner {
  private readonly processedMetrics: Record<number, RunnerProgress> = {};

  constructor(
    private readonly processor: TronBatchProcessor<TronFieldSelection>
  ) {}

  async run(options: RunnterOptions) {
    const { ormpContractChain } = options;
    const db = new TypeormDatabase({
      supportHotBlocks: true,
      stateSchema: `chain_${ormpContractChain.chainId}_processor`,
    });
    let controlRerunLog: Record<number, boolean> = {};

    this.processor.run(db, async (ctx) => {
      if (controlRerunLog[ormpContractChain.chainId]) {
        ctx.log.info(
          `(evm) [${ormpContractChain.chainId}] restart from ${ctx.blocks[0].header.height}`
        );

        controlRerunLog[ormpContractChain.chainId] = false;
      }

      // console.log('----->', ctx.blocks);
      for (const block of ctx.blocks) {
        for (const event of block.logs) {
          const ormpContractConfig = ormpContractChain.contracts.find((item) =>
            helpers.compareHashString(item.address, event.address)
          );

          if (!ormpContractConfig) {
            continue;
          }
          const lifecycle: HandlerLifecycle = {
            ormpContractChain,
            ormpContractConfig,
            // messageProgressCount,
          };

          // console.log(event);
          // console.log('=======================', ormpContractConfig.name);
          try {
            switch (ormpContractConfig.name.toLowerCase()) {
              case "ormpupgradeableport":
                await new MsgportTronHandler(ctx, lifecycle).handle(event);
                break;
              case "ormp":
                await new OrmpTronHandler(ctx, lifecycle).handle(event);
                break;
            }
          } catch (e) {
            ctx.log.warn(
              `(tron) [${ormpContractChain.chainId}] unhandled contract ${
                ormpContractConfig.name
              } at ${event.block.height} ${
                event.getTransaction().id
              }, reason: ${e}, stopped from ${
                ctx.blocks[0].header.height
              } block`
            );
            controlRerunLog[ormpContractChain.chainId] = true;
            throw e;
          }
        }
      }

      const cachedProgressRunnerProgress =
        this.processedMetrics[ormpContractChain.chainId];
      const lastHeight = ctx.blocks[ctx.blocks.length - 1].header.height;
      const currentRunnerProgress: RunnerProgress = {
        fromHeight: cachedProgressRunnerProgress
          ? cachedProgressRunnerProgress.fromHeight
          : lastHeight - ctx.blocks.length,
        lastHeight,
        lastPrintTime:
          cachedProgressRunnerProgress?.lastPrintTime ?? new Date(),
      };
      const showProgressLog =
        !cachedProgressRunnerProgress ||
        +new Date() - +currentRunnerProgress.lastPrintTime > 1000 * 10;
      if (showProgressLog) {
        ctx.log.info(
          `[${ormpContractChain.chainId}] processed ${
            currentRunnerProgress.lastHeight - currentRunnerProgress.fromHeight
          } blocks from ${currentRunnerProgress.fromHeight} to ${
            currentRunnerProgress.lastHeight
          } block`
        );
        currentRunnerProgress.lastPrintTime = new Date();
        currentRunnerProgress.fromHeight = lastHeight;
      }
      this.processedMetrics[ormpContractChain.chainId] = currentRunnerProgress;
    });
  }
}

export class MsgscanIndexerEvmRunner {
  private readonly processedMetrics: Record<number, RunnerProgress> = {};

  constructor(
    private readonly processor: EvmBatchProcessor<EvmFieldSelection>
  ) {}

  async run(options: RunnterOptions) {
    const { ormpContractChain } = options;
    const db = new TypeormDatabase({
      supportHotBlocks: true,
      stateSchema: `chain_${ormpContractChain.chainId}_processor`,
    });
    let controlRerunLog: Record<number, boolean> = {};

    this.processor.run(db, async (ctx) => {
      if (controlRerunLog[ormpContractChain.chainId]) {
        ctx.log.info(
          `(evm) [${ormpContractChain.chainId}] restart from ${ctx.blocks[0].header.height}`
        );

        controlRerunLog[ormpContractChain.chainId] = false;
      }

      for (const block of ctx.blocks) {
        for (const event of block.logs) {
          const ormpContractConfig = ormpContractChain.contracts.find((item) =>
            helpers.compareHashString(item.address, event.address)
          );

          if (!ormpContractConfig) {
            continue;
          }
          const lifecycle: HandlerLifecycle = {
            ormpContractChain,
            ormpContractConfig,
            // messageProgressCount,
          };

          try {
            switch (ormpContractConfig.name.toLowerCase()) {
              case "msgport":
                await new MsgportEvmHandler(ctx, lifecycle).handle(event);
                break;
              case "ormp":
                await new OrmpEvmHandler(ctx, lifecycle).handle(event);
                break;
              case "signaturepub":
                await new SigncribeEvmHandler(ctx, lifecycle).handle(event);
                break;
            }
          } catch (e) {
            ctx.log.warn(
              `(evm) [${ormpContractChain.chainId}] unhandled contract ${ormpContractConfig.name} at ${event.block.height} ${event.transactionHash}, reason: ${e}, stopped from ${ctx.blocks[0].header.height} block`
            );
            controlRerunLog[ormpContractChain.chainId] = true;
            throw e;
          }
        }
      }

      const cachedProgressRunnerProgress =
        this.processedMetrics[ormpContractChain.chainId];
      const lastHeight = ctx.blocks[ctx.blocks.length - 1].header.height;
      const currentRunnerProgress: RunnerProgress = {
        fromHeight: cachedProgressRunnerProgress
          ? cachedProgressRunnerProgress.fromHeight
          : lastHeight - ctx.blocks.length,
        lastHeight,
        lastPrintTime:
          cachedProgressRunnerProgress?.lastPrintTime ?? new Date(),
      };
      const showProgressLog =
        !cachedProgressRunnerProgress ||
        +new Date() - +currentRunnerProgress.lastPrintTime > 1000 * 10;
      if (showProgressLog) {
        ctx.log.info(
          `[${ormpContractChain.chainId}] processed ${
            currentRunnerProgress.lastHeight - currentRunnerProgress.fromHeight
          } blocks from ${currentRunnerProgress.fromHeight} to ${
            currentRunnerProgress.lastHeight
          } block`
        );
        currentRunnerProgress.lastPrintTime = new Date();
        currentRunnerProgress.fromHeight = lastHeight;
      }
      this.processedMetrics[ormpContractChain.chainId] = currentRunnerProgress;
    });
  }
}

interface RunnerProgress {
  fromHeight: number;
  lastHeight: number;
  lastPrintTime: Date;
}
