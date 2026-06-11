-- ORMP legacy schema compatibility contract.
--
-- This initializes the tables exposed by the previous Subsquid GraphQL schema.
-- It does not migrate old rows or define the future event ingestion runner.

CREATE TABLE IF NOT EXISTS ormp_hash_imported (
  id VARCHAR NOT NULL PRIMARY KEY,
  block_number NUMERIC NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_timestamp NUMERIC NOT NULL,
  chain_id NUMERIC NOT NULL,
  src_chain_id NUMERIC NOT NULL,
  target_chain_id NUMERIC NOT NULL,
  oracle TEXT NOT NULL,
  channel TEXT NOT NULL,
  msg_index NUMERIC NOT NULL,
  hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ormp_message_accepted (
  id VARCHAR NOT NULL PRIMARY KEY,
  block_number NUMERIC NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_timestamp NUMERIC NOT NULL,
  chain_id NUMERIC NOT NULL,
  log_index INTEGER NOT NULL,
  msg_hash TEXT NOT NULL,
  channel TEXT NOT NULL,
  "index" NUMERIC NOT NULL,
  from_chain_id NUMERIC NOT NULL,
  "from" TEXT NOT NULL,
  to_chain_id NUMERIC NOT NULL,
  "to" TEXT NOT NULL,
  gas_limit NUMERIC NOT NULL,
  encoded TEXT NOT NULL,
  oracle TEXT,
  oracle_assigned BOOLEAN,
  oracle_assigned_fee NUMERIC,
  relayer TEXT,
  relayer_assigned BOOLEAN,
  relayer_assigned_fee NUMERIC
);

CREATE TABLE IF NOT EXISTS ormp_message_assigned (
  id VARCHAR NOT NULL PRIMARY KEY,
  block_number NUMERIC NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_timestamp NUMERIC NOT NULL,
  chain_id NUMERIC NOT NULL,
  msg_hash TEXT NOT NULL,
  oracle TEXT NOT NULL,
  relayer TEXT NOT NULL,
  oracle_fee NUMERIC NOT NULL,
  relayer_fee NUMERIC NOT NULL,
  params TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ormp_message_dispatched (
  id VARCHAR NOT NULL PRIMARY KEY,
  block_number NUMERIC NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_timestamp NUMERIC NOT NULL,
  chain_id NUMERIC NOT NULL,
  target_chain_id NUMERIC NOT NULL,
  msg_hash TEXT NOT NULL,
  dispatch_result BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS msgport_message_recv (
  id VARCHAR NOT NULL PRIMARY KEY,
  block_number NUMERIC NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_timestamp NUMERIC NOT NULL,
  transaction_index INTEGER NOT NULL,
  log_index INTEGER NOT NULL,
  chain_id NUMERIC NOT NULL,
  port_address TEXT NOT NULL,
  msg_id TEXT NOT NULL,
  result BOOLEAN NOT NULL,
  return_data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS msgport_message_sent (
  id VARCHAR NOT NULL PRIMARY KEY,
  block_number NUMERIC NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_timestamp NUMERIC NOT NULL,
  transaction_index INTEGER NOT NULL,
  log_index INTEGER NOT NULL,
  chain_id NUMERIC NOT NULL,
  port_address TEXT NOT NULL,
  transaction_from TEXT,
  from_chain_id NUMERIC NOT NULL,
  msg_id TEXT NOT NULL,
  from_dapp TEXT NOT NULL,
  to_chain_id NUMERIC NOT NULL,
  to_dapp TEXT NOT NULL,
  message TEXT NOT NULL,
  params TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signature_pub_signature_submittion (
  id VARCHAR NOT NULL PRIMARY KEY,
  block_number NUMERIC NOT NULL,
  transaction_hash TEXT NOT NULL,
  block_timestamp NUMERIC NOT NULL,
  chain_id NUMERIC NOT NULL,
  channel TEXT NOT NULL,
  signer TEXT NOT NULL,
  msg_index NUMERIC NOT NULL,
  signature TEXT NOT NULL,
  data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ormp_message_accepted_msg_hash_idx
  ON ormp_message_accepted (msg_hash);
CREATE INDEX IF NOT EXISTS ormp_message_assigned_msg_hash_idx
  ON ormp_message_assigned (msg_hash);
CREATE INDEX IF NOT EXISTS msgport_message_sent_msg_id_idx
  ON msgport_message_sent (msg_id);
CREATE INDEX IF NOT EXISTS msgport_message_recv_msg_id_idx
  ON msgport_message_recv (msg_id);
