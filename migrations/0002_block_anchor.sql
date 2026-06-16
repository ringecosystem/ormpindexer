CREATE TABLE IF NOT EXISTS ormp_indexer_block_anchor (
  chain_id NUMERIC NOT NULL,
  dataset TEXT NOT NULL,
  block_number NUMERIC NOT NULL,
  block_hash TEXT NOT NULL,
  parent_hash TEXT,
  finality TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chain_id, dataset, block_number)
);
