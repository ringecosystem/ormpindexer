CREATE TABLE IF NOT EXISTS ormp_indexer_checkpoint (
  chain_id NUMERIC NOT NULL,
  dataset TEXT NOT NULL,
  next_block NUMERIC NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (chain_id, dataset)
);
