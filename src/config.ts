export interface OrmpContractChain {
  chainId: number;
  gateway?: string;
  finalityConfirmation?: number;
  rpcs: string[];
  startBlock: number;
  contracts: OrmpContractConfig[];
}

export interface OrmpContractConfig {
  name: string;
  address: string;
}

const defaultOrmpContracts: OrmpContractConfig[] = [
  {
    name: "Msgport",
    address: "0x2cd1867Fb8016f93710B6386f7f9F1D540A60812",
  },
  {
    name: "ORMP",
    address: "0x13b2211a7cA45Db2808F6dB05557ce5347e3634e",
  },
];

const signaturePub: OrmpContractConfig = {
  name: "SignaturePub",
  address: "0x57Aa601A0377f5AB313C5A955ee874f5D495fC92",
};

function extractRpcEndpoint(
  chainId: number,
  defaultEndpoint: string
): string[] {
  const envEndpoints = process.env[`CHAIN_ENDPOINT_${chainId}`];
  if (!envEndpoints) {
    return [defaultEndpoint];
  }
  const ees = envEndpoints.split(",");
  if (!ees.length) {
    return [defaultEndpoint];
  }
  return [...ees, defaultEndpoint];
}

export const ormpContractChains: OrmpContractChain[] = [
  //######## mainnets
  // ethereum
  {
    chainId: 1,
    rpcs: extractRpcEndpoint(1, "wss://ethereum-rpc.publicnode.com"),
    gateway: "https://v2.archive.subsquid.io/network/ethereum-mainnet",
    finalityConfirmation: 75,
    startBlock: 20009590,
    contracts: defaultOrmpContracts,
  },
  // crab
  {
    chainId: 44,
    rpcs: extractRpcEndpoint(44, "wss://crab-rpc.darwinia.network"),
    finalityConfirmation: 10,
    startBlock: 2900604,
    contracts: defaultOrmpContracts,
  },
  // darwinia
  {
    chainId: 46,
    rpcs: extractRpcEndpoint(46, "wss://rpc.darwinia.network"),
    finalityConfirmation: 10,
    startBlock: 2830100,
    contracts: [...defaultOrmpContracts, signaturePub],
  },
  // polygon
  {
    chainId: 137,
    rpcs: extractRpcEndpoint(137, "wss://polygon.gateway.tenderly.co"),
    gateway: "https://v2.archive.subsquid.io/network/polygon-mainnet",
    finalityConfirmation: 10,
    startBlock: 57244567,
    contracts: defaultOrmpContracts,
  },
  // moobean
  {
    chainId: 1284,
    rpcs: extractRpcEndpoint(1284, "wss://moonbeam-rpc.dwellir.com"),
    gateway: "https://v2.archive.subsquid.io/network/moonbeam-mainnet",
    finalityConfirmation: 10,
    startBlock: 6294138,
    contracts: defaultOrmpContracts,
  },
  // arbitrum
  {
    chainId: 42161,
    rpcs: extractRpcEndpoint(42161, "wss://arbitrum.callstaticrpc.com"),
    gateway: "https://v2.archive.subsquid.io/network/arbitrum-one",
    finalityConfirmation: 50,
    startBlock: 217891600,
    contracts: defaultOrmpContracts,
  },
  // blast
  {
    chainId: 81457,
    rpcs: extractRpcEndpoint(81457, "wss://blast.drpc.org"),
    gateway: "https://v2.archive.subsquid.io/network/blast-l2-mainnet",
    finalityConfirmation: 20,
    startBlock: 4293849,
    contracts: defaultOrmpContracts,
  },
  // morph
  {
    chainId: 2818,
    rpcs: extractRpcEndpoint(2818, "wss://rpc.morphl2.io:8443"),
    finalityConfirmation: 50,
    startBlock: 59565,
    contracts: defaultOrmpContracts,
  },
  // tron
  {
    chainId: 728126428,
    rpcs: extractRpcEndpoint(728126428, "https://api.trongrid.io"),
    gateway: "https://v2.archive.subsquid.io/network/tron-mainnet",
    startBlock: 62251337, // start block: 62251337
    contracts: [
      {
        name: "Msgport",
        address: "0x3Bc5362EC3a3DBc07292aEd4ef18Be18De02DA3a", // TFRF7t9m7pGLnwwX8TFsZvj85EvQ6gSBCm
      },
      {
        name: "ORMP",
        address: "0x5C5c383FEbE62F377F8c0eA1de97F2a2Ba102e98", // TJPZeFEdc4TBEcNbku5xVZLQ6B2Q1oGnd1
      },
    ],
  },

  //######## testnets
  // // hekala-taiko
  // {
  //   chainId: 167009,
  //   rpcs: extractRpcEndpoint(167009, "wss://ws.hekla.taiko.xyz"),
  //   finalityConfirmation: 20,
  //   startBlock: 311800,
  //   contracts: defaultOrmpContracts,
  // },
  // // arbitrum-sepolia
  // {
  //   chainId: 421614,
  //   rpcs: extractRpcEndpoint(421614, "wss://arbitrum-sepolia.drpc.org"),
  //   gateway: "https://v2.archive.subsquid.io/network/arbitrum-sepolia",
  //   finalityConfirmation: 50,
  //   startBlock: 53486700,
  //   contracts: defaultOrmpContracts,
  // },
  // // sepolia
  // {
  //   chainId: 11155111,
  //   rpcs: extractRpcEndpoint(
  //     11155111,
  //     "wss://ethereum-sepolia-rpc.publicnode.com"
  //   ),
  //   gateway: "https://v2.archive.subsquid.io/network/ethereum-sepolia",
  //   finalityConfirmation: 25,
  //   startBlock: 6083800,
  //   contracts: defaultOrmpContracts,
  // },
  // // 2494104990 tron-shasta
  // {
  //   chainId: 2494104990,
  //   rpcs: extractRpcEndpoint(2494104990, "https://api.shasta.trongrid.io"),
  //   startBlock: 44847100,
  //   contracts: [
  //     {
  //       name: "Msgport",
  //       address: "0x9a80B8a27Ea73BD584336C9c200bb97190865482",
  //     },
  //     {
  //       name: "ORMP",
  //       address: "0x841B6b2F3148131Ac161d88edFb2C11F146e189F",
  //     },
  //   ],
  // },
];
