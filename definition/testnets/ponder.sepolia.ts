import { createConfig, loadBalance } from "@ponder/core";
import { http } from "viem";

import { ORMPAbi as ORMPAbiV2 } from "./abis/v2/ORMPAbi";
import { MsgportAbi } from "./abis/v2/MsgportAbi";

const INFURA_API_KEY = process.env.INFURA_API_KEY;
// const INFURA_API_KEY = null;
const MAX_REQUESTS_PER_SECOND = 5;
const FAST_MAX_REQUESTS_PER_SECOND = 10;

export default createConfig({
  networks: {
    sepolia: {
      chainId: 11155111,
      transport: loadBalance([
        // http("https://rpc2.sepolia.org"),
        // http("https://hrpc.darwinia.network/sepolia"),
        http(`https://sepolia.infura.io/v3/${INFURA_API_KEY}`),
      ]),
      maxRequestsPerSecond: MAX_REQUESTS_PER_SECOND,
    },
  },
  contracts: {
    Msgport: {
      abi: MsgportAbi,
      address: "0x2cd1867Fb8016f93710B6386f7f9F1D540A60812",
      network: {
        sepolia: {
          startBlock: 6083800,
        },
      },
      filter: {
        event: ["MessageSent", "MessageRecv"],
      },
    },
    // === V2
    ORMPV2: {
      abi: ORMPAbiV2,
      address: "0x13b2211a7cA45Db2808F6dB05557ce5347e3634e",
      network: {
        sepolia: {
          startBlock: 6083800,
        },
      },
      filter: {
        event: [
          "MessageAccepted",
          "MessageDispatched",
          "MessageAssigned",
          "HashImported",
        ],
      },
    },
  },
});
