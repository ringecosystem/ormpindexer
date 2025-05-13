import {Entity as Entity_, Column as Column_, PrimaryColumn as PrimaryColumn_, BigIntColumn as BigIntColumn_, StringColumn as StringColumn_, BooleanColumn as BooleanColumn_} from "@subsquid/typeorm-store"

@Entity_()
export class ORMPMessageDispatched {
    constructor(props?: Partial<ORMPMessageDispatched>) {
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
    targetChainId!: bigint

    @StringColumn_({nullable: false})
    msgHash!: string

    @BooleanColumn_({nullable: false})
    dispatchResult!: boolean
}
