import {Entity as Entity_, Column as Column_, PrimaryColumn as PrimaryColumn_, BigIntColumn as BigIntColumn_, StringColumn as StringColumn_} from "@subsquid/typeorm-store"

@Entity_()
export class ORMPMessageAssigned {
    constructor(props?: Partial<ORMPMessageAssigned>) {
        Object.assign(this, props)
    }

    @PrimaryColumn_()
    id!: string

    @BigIntColumn_({nullable: false})
    blockNumber!: bigint

    @StringColumn_({nullable: false})
    transactionHash!: string

    @BigIntColumn_({nullable: false})
    blockTimestamp!: bigint

    @BigIntColumn_({nullable: false})
    chainId!: bigint

    @StringColumn_({nullable: false})
    msgHash!: string

    @StringColumn_({nullable: false})
    oracle!: string

    @StringColumn_({nullable: false})
    relayer!: string

    @BigIntColumn_({nullable: false})
    oracleFee!: bigint

    @BigIntColumn_({nullable: false})
    relayerFee!: bigint

    @StringColumn_({nullable: false})
    params!: string
}
