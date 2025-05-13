module.exports = class Data1745888239541 {
    name = 'Data1745888239541'

    async up(db) {
        await db.query(`CREATE TABLE "ormp_hash_imported" ("id" character varying NOT NULL, "block_number" numeric NOT NULL, "transaction_hash" text NOT NULL, "block_timestamp" numeric NOT NULL, "chain_id" numeric NOT NULL, "src_chain_id" numeric NOT NULL, "target_chain_id" numeric NOT NULL, "oracle" text NOT NULL, "channel" text NOT NULL, "msg_index" numeric NOT NULL, "hash" text NOT NULL, CONSTRAINT "PK_f77b5e871e898bb5858336c2e28" PRIMARY KEY ("id"))`)
        await db.query(`CREATE TABLE "ormp_message_accepted" ("id" character varying NOT NULL, "block_number" numeric NOT NULL, "transaction_hash" text NOT NULL, "block_timestamp" numeric NOT NULL, "chain_id" numeric NOT NULL, "log_index" integer NOT NULL, "msg_hash" text NOT NULL, "channel" text NOT NULL, "index" numeric NOT NULL, "from_chain_id" numeric NOT NULL, "from" text NOT NULL, "to_chain_id" numeric NOT NULL, "to" text NOT NULL, "gas_limit" numeric NOT NULL, "encoded" text NOT NULL, "oracle" text, "oracle_assigned" boolean, "oracle_assigned_fee" numeric, "relayer" text, "relayer_assigned" boolean, "relayer_assigned_fee" numeric, CONSTRAINT "PK_7b118199a6f5a209125bcd1f150" PRIMARY KEY ("id"))`)
        await db.query(`CREATE TABLE "ormp_message_assigned" ("id" character varying NOT NULL, "block_number" numeric NOT NULL, "transaction_hash" text NOT NULL, "block_timestamp" numeric NOT NULL, "chain_id" numeric NOT NULL, "msg_hash" text NOT NULL, "oracle" text NOT NULL, "relayer" text NOT NULL, "oracle_fee" numeric NOT NULL, "relayer_fee" numeric NOT NULL, "params" text NOT NULL, CONSTRAINT "PK_16d7e06a3c8a8aedd8ac420e694" PRIMARY KEY ("id"))`)
        await db.query(`CREATE TABLE "ormp_message_dispatched" ("id" character varying NOT NULL, "block_number" numeric NOT NULL, "transaction_hash" text NOT NULL, "block_timestamp" numeric NOT NULL, "chain_id" numeric NOT NULL, "target_chain_id" numeric NOT NULL, "msg_hash" text NOT NULL, "dispatch_result" boolean NOT NULL, CONSTRAINT "PK_2454489075c18ebbb272ccabfd0" PRIMARY KEY ("id"))`)
        await db.query(`CREATE TABLE "msgport_message_recv" ("id" character varying NOT NULL, "block_number" numeric NOT NULL, "transaction_hash" text NOT NULL, "block_timestamp" numeric NOT NULL, "transaction_index" integer NOT NULL, "log_index" integer NOT NULL, "chain_id" numeric NOT NULL, "port_address" text NOT NULL, "msg_id" text NOT NULL, "result" boolean NOT NULL, "return_data" text NOT NULL, CONSTRAINT "PK_d81f8fc6605f54b7fbb9f9af22d" PRIMARY KEY ("id"))`)
        await db.query(`CREATE TABLE "msgport_message_sent" ("id" character varying NOT NULL, "block_number" numeric NOT NULL, "transaction_hash" text NOT NULL, "block_timestamp" numeric NOT NULL, "transaction_index" integer NOT NULL, "log_index" integer NOT NULL, "chain_id" numeric NOT NULL, "port_address" text NOT NULL, "transaction_from" text, "from_chain_id" numeric NOT NULL, "msg_id" text NOT NULL, "from_dapp" text NOT NULL, "to_chain_id" numeric NOT NULL, "to_dapp" text NOT NULL, "message" text NOT NULL, "params" text NOT NULL, CONSTRAINT "PK_d7c7a6a316903b6ecec10a6ed8f" PRIMARY KEY ("id"))`)
        await db.query(`CREATE TABLE "signature_pub_signature_submittion" ("id" character varying NOT NULL, "block_number" numeric NOT NULL, "transaction_hash" text NOT NULL, "block_timestamp" numeric NOT NULL, "chain_id" numeric NOT NULL, "channel" text NOT NULL, "signer" text NOT NULL, "msg_index" numeric NOT NULL, "signature" text NOT NULL, "data" text NOT NULL, CONSTRAINT "PK_706c9af05be7ea394eec4bec3d9" PRIMARY KEY ("id"))`)
    }

    async down(db) {
        await db.query(`DROP TABLE "ormp_hash_imported"`)
        await db.query(`DROP TABLE "ormp_message_accepted"`)
        await db.query(`DROP TABLE "ormp_message_assigned"`)
        await db.query(`DROP TABLE "ormp_message_dispatched"`)
        await db.query(`DROP TABLE "msgport_message_recv"`)
        await db.query(`DROP TABLE "msgport_message_sent"`)
        await db.query(`DROP TABLE "signature_pub_signature_submittion"`)
    }
}
