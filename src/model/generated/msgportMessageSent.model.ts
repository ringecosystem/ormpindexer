import {Entity as Entity_, Column as Column_, PrimaryColumn as PrimaryColumn_, BigIntColumn as BigIntColumn_, StringColumn as StringColumn_, IntColumn as IntColumn_} from "@subsquid/typeorm-store"

@Entity_()
export class MsgportMessageSent {
    constructor(props?: Partial<MsgportMessageSent>) {
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

    @IntColumn_({nullable: false})
    transactionIndex!: number

    @IntColumn_({nullable: false})
    logIndex!: number

    @BigIntColumn_({nullable: false})
    chainId!: bigint

    @StringColumn_({nullable: false})
    portAddress!: string

    @StringColumn_({nullable: true})
    transactionFrom!: string | undefined | null

    @BigIntColumn_({nullable: false})
    fromChainId!: bigint

    @StringColumn_({nullable: false})
    msgId!: string

    @StringColumn_({nullable: false})
    fromDapp!: string

    @BigIntColumn_({nullable: false})
    toChainId!: bigint

    @StringColumn_({nullable: false})
    toDapp!: string

    @StringColumn_({nullable: false})
    message!: string

    @StringColumn_({nullable: false})
    params!: string
}
