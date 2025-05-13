import {Entity as Entity_, Column as Column_, PrimaryColumn as PrimaryColumn_, BigIntColumn as BigIntColumn_, StringColumn as StringColumn_, IntColumn as IntColumn_, BooleanColumn as BooleanColumn_} from "@subsquid/typeorm-store"

@Entity_()
export class ORMPMessageAccepted {
    constructor(props?: Partial<ORMPMessageAccepted>) {
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

    @IntColumn_({nullable: false})
    logIndex!: number

    @StringColumn_({nullable: false})
    msgHash!: string

    @StringColumn_({nullable: false})
    channel!: string

    @BigIntColumn_({nullable: false})
    index!: bigint

    @BigIntColumn_({nullable: false})
    fromChainId!: bigint

    @StringColumn_({nullable: false})
    from!: string

    @BigIntColumn_({nullable: false})
    toChainId!: bigint

    @StringColumn_({nullable: false})
    to!: string

    @BigIntColumn_({nullable: false})
    gasLimit!: bigint

    @StringColumn_({nullable: false})
    encoded!: string

    @StringColumn_({nullable: true})
    oracle!: string | undefined | null

    @BooleanColumn_({nullable: true})
    oracleAssigned!: boolean | undefined | null

    @BigIntColumn_({nullable: true})
    oracleAssignedFee!: bigint | undefined | null

    @StringColumn_({nullable: true})
    relayer!: string | undefined | null

    @BooleanColumn_({nullable: true})
    relayerAssigned!: boolean | undefined | null

    @BigIntColumn_({nullable: true})
    relayerAssignedFee!: bigint | undefined | null
}
