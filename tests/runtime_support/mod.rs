use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use ormpindexer::{
    config::FinalityMode,
    database::EventWriter,
    datalens::{
        DatalensBlock, DatalensBlockQuery, DatalensBlockQueryResult, DatalensLog, DatalensLogQuery,
        DatalensLogQueryResult, DatalensLogReader, IncompleteReason,
    },
    decoder::EventDecoder,
    schema::{ChainLogMetadata, EventSource, LegacyOrmPEvent},
};

#[derive(Clone)]
struct PartialLogServe {
    chain_id: u64,
    from_block: u64,
    to_block: u64,
    served_start: Option<u64>,
    served_end: Option<u64>,
    incomplete_reason: Option<IncompleteReason>,
}

#[derive(Clone)]
pub struct RecordingDatalensReader {
    blocks: Vec<DatalensBlock>,
    logs: Vec<DatalensLog>,
    heads: BTreeMap<u64, u64>,
    finalized_heads: BTreeMap<u64, u64>,
    query_delays: BTreeMap<u64, Duration>,
    query_failures: BTreeMap<u64, String>,
    partials: Vec<PartialLogServe>,
    block_incomplete: Option<IncompleteReason>,
    pub queries: Arc<Mutex<Vec<DatalensLogQuery>>>,
    block_queries: Arc<Mutex<Vec<DatalensBlockQuery>>>,
}

impl RecordingDatalensReader {
    pub fn new(logs: Vec<DatalensLog>) -> Self {
        Self {
            blocks: Vec::new(),
            logs,
            heads: BTreeMap::new(),
            finalized_heads: BTreeMap::new(),
            query_delays: BTreeMap::new(),
            query_failures: BTreeMap::new(),
            partials: Vec::new(),
            block_incomplete: None,
            queries: Arc::new(Mutex::new(Vec::new())),
            block_queries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_head(mut self, chain_id: u64, head: u64) -> Self {
        self.heads.insert(chain_id, head);
        self
    }

    pub fn with_finalized_head(mut self, chain_id: u64, finalized: u64) -> Self {
        self.finalized_heads.insert(chain_id, finalized);
        self
    }

    pub fn with_partial(
        mut self,
        chain_id: u64,
        from_block: u64,
        to_block: u64,
        served_end: Option<u64>,
        incomplete_reason: IncompleteReason,
    ) -> Self {
        self.partials.push(PartialLogServe {
            chain_id,
            from_block,
            to_block,
            served_start: None,
            served_end,
            incomplete_reason: Some(incomplete_reason),
        });
        self
    }

    pub fn with_served_window(
        mut self,
        chain_id: u64,
        from_block: u64,
        to_block: u64,
        served_start: u64,
        served_end: u64,
    ) -> Self {
        self.partials.push(PartialLogServe {
            chain_id,
            from_block,
            to_block,
            served_start: Some(served_start),
            served_end: Some(served_end),
            incomplete_reason: None,
        });
        self
    }

    pub fn with_blocks(mut self, blocks: Vec<DatalensBlock>) -> Self {
        self.blocks = blocks;
        self
    }

    pub fn with_block_incomplete(mut self, reason: IncompleteReason) -> Self {
        self.block_incomplete = Some(reason);
        self
    }

    pub fn block_queries(&self) -> Arc<Mutex<Vec<DatalensBlockQuery>>> {
        self.block_queries.clone()
    }

    pub fn with_query_delay(mut self, chain_id: u64, delay: Duration) -> Self {
        self.query_delays.insert(chain_id, delay);
        self
    }

    pub fn with_query_failure(mut self, chain_id: u64, error: &str) -> Self {
        self.query_failures.insert(chain_id, error.to_owned());
        self
    }
}

impl DatalensLogReader for RecordingDatalensReader {
    async fn latest_block(
        &self,
        chain_id: u64,
        _finality_mode: FinalityMode,
    ) -> anyhow::Result<u64> {
        Ok(*self.heads.get(&chain_id).unwrap_or(&u64::MAX))
    }

    async fn query_logs(&self, query: DatalensLogQuery) -> anyhow::Result<DatalensLogQueryResult> {
        if let Some(delay) = self.query_delays.get(&query.chain_id) {
            tokio::time::sleep(*delay).await;
        }
        self.queries
            .lock()
            .expect("queries lock")
            .push(query.clone());
        if let Some(error) = self.query_failures.get(&query.chain_id) {
            anyhow::bail!("{error}");
        }
        if let Some(partial) = self.partials.iter().find(|partial| {
            partial.chain_id == query.chain_id
                && partial.from_block == query.from_block
                && partial.to_block == query.to_block
        }) {
            let served_end = partial.served_end;
            let served_start = served_end.map(|_| partial.served_start.unwrap_or(query.from_block));
            return Ok(DatalensLogQueryResult {
                logs: self
                    .logs
                    .iter()
                    .filter(|log| {
                        log.chain_id == query.chain_id
                            && served_end.is_some_and(|end| {
                                log.block_number >= query.from_block && log.block_number <= end
                            })
                    })
                    .cloned()
                    .collect(),
                served_start,
                served_end,
                incomplete_reason: partial.incomplete_reason,
            });
        }
        Ok(DatalensLogQueryResult {
            logs: self
                .logs
                .iter()
                .filter(|log| {
                    log.chain_id == query.chain_id
                        && log.block_number >= query.from_block
                        && log.block_number <= query.to_block
                })
                .cloned()
                .collect(),
            served_start: Some(query.from_block),
            served_end: Some(query.to_block),
            incomplete_reason: None,
        })
    }

    async fn finalized_head(&self, chain_id: u64) -> anyhow::Result<u64> {
        Ok(self.finalized_heads.get(&chain_id).copied().unwrap_or(0))
    }

    async fn query_blocks(
        &self,
        query: DatalensBlockQuery,
    ) -> anyhow::Result<DatalensBlockQueryResult> {
        self.block_queries
            .lock()
            .expect("block queries lock")
            .push(query.clone());
        if let Some(reason) = self.block_incomplete {
            return Ok(DatalensBlockQueryResult {
                blocks: Vec::new(),
                served_start: None,
                served_end: None,
                incomplete_reason: Some(reason),
            });
        }
        Ok(DatalensBlockQueryResult {
            blocks: self
                .blocks
                .iter()
                .filter(|block| {
                    block.chain_id == query.chain_id
                        && block.block_number >= query.from_block
                        && block.block_number <= query.to_block
                })
                .cloned()
                .collect(),
            served_start: Some(query.from_block),
            served_end: Some(query.to_block),
            incomplete_reason: None,
        })
    }
}

pub struct EchoTransactionFromDecoder;

impl EventDecoder for EchoTransactionFromDecoder {
    async fn decode(&self, log: &DatalensLog) -> anyhow::Result<Vec<LegacyOrmPEvent>> {
        Ok(vec![LegacyOrmPEvent::MsgportMessageSent {
            metadata: ChainLogMetadata {
                id: log.id.clone().expect("test log id"),
                source: EventSource::Evm,
                chain_id: log.chain_id.into(),
                block_number: log.block_number.into(),
                block_hash: log.block_hash.clone(),
                block_timestamp: log.block_timestamp.expect("test timestamp").into(),
                transaction_hash: log.transaction_hash.clone(),
                transaction_index: log.transaction_index.expect("test transaction index"),
                log_index: i32::try_from(log.log_index).expect("test log index"),
                contract_address: log.address.clone(),
                transaction_from: log.transaction_from.clone(),
            },
            msg_id: "0xmsgid".to_owned(),
            from_dapp: "0xfromdapp".to_owned(),
            to_chain_id: 1,
            to_dapp: "0xtodapp".to_owned(),
            message: "0xmessage".to_owned(),
            params: "0xparams".to_owned(),
        }])
    }
}

#[derive(Clone, Default)]
pub struct RecordingEventWriter {
    events: Arc<Mutex<Vec<LegacyOrmPEvent>>>,
}

impl RecordingEventWriter {
    pub fn events(&self) -> Vec<LegacyOrmPEvent> {
        self.events.lock().expect("events lock").clone()
    }
}

impl EventWriter for RecordingEventWriter {
    async fn write_events(&self, events: &[LegacyOrmPEvent]) -> anyhow::Result<usize> {
        self.events
            .lock()
            .expect("events lock")
            .extend_from_slice(events);
        Ok(events.len())
    }
}

pub struct FailingEventWriter;

impl EventWriter for FailingEventWriter {
    async fn write_events(&self, _events: &[LegacyOrmPEvent]) -> anyhow::Result<usize> {
        anyhow::bail!("write failed");
    }
}
