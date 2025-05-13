import {Entity as Entity_, Column as Column_, PrimaryColumn as PrimaryColumn_, BigIntColumn as BigIntColumn_, StringColumn as StringColumn_} from "@subsquid/typeorm-store"

@Entity_()
export class ORMPHashImported {
    constructor(props?: Partial<ORMPHashImported>) {
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

    @BigIntColumn_({nullable: false})
    srcChainId!: bigint

    @BigIntColumn_({nullable: false})
    targetChainId!: bigint

    @StringColumn_({nullable: false})
    oracle!: string

    @StringColumn_({nullable: false})
    channel!: string

    @BigIntColumn_({nullable: false})
    msgIndex!: bigint

    @StringColumn_({nullable: false})
    hash!: string
}
