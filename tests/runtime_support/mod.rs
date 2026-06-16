use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use ormpindexer::{
    config::FinalityMode,
    database::EventWriter,
    datalens::{
        DatalensLog, DatalensLogQuery, DatalensLogQueryResult, DatalensLogReader,
        DatalensTransaction, DatalensTransactionQuery, DatalensTransactionQueryResult,
    },
    decoder::EventDecoder,
    schema::{ChainLogMetadata, EventSource, LegacyOrmPEvent},
};

#[derive(Clone)]
pub struct RecordingDatalensReader {
    logs: Vec<DatalensLog>,
    transactions: Vec<DatalensTransaction>,
    heads: BTreeMap<u64, u64>,
    query_delays: BTreeMap<u64, Duration>,
    query_failures: BTreeMap<u64, String>,
    range_query_failures: BTreeMap<(u64, u64, u64), String>,
    pub queries: Arc<Mutex<Vec<DatalensLogQuery>>>,
    transaction_queries: Arc<Mutex<Vec<DatalensTransactionQuery>>>,
}

impl RecordingDatalensReader {
    pub fn new(logs: Vec<DatalensLog>) -> Self {
        Self {
            logs,
            transactions: Vec::new(),
            heads: BTreeMap::new(),
            query_delays: BTreeMap::new(),
            query_failures: BTreeMap::new(),
            range_query_failures: BTreeMap::new(),
            queries: Arc::new(Mutex::new(Vec::new())),
            transaction_queries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_head(mut self, chain_id: u64, head: u64) -> Self {
        self.heads.insert(chain_id, head);
        self
    }

    pub fn with_transactions(mut self, transactions: Vec<DatalensTransaction>) -> Self {
        self.transactions = transactions;
        self
    }

    pub fn transaction_queries(&self) -> Arc<Mutex<Vec<DatalensTransactionQuery>>> {
        self.transaction_queries.clone()
    }

    pub fn with_query_delay(mut self, chain_id: u64, delay: Duration) -> Self {
        self.query_delays.insert(chain_id, delay);
        self
    }

    pub fn with_query_failure(mut self, chain_id: u64, error: &str) -> Self {
        self.query_failures.insert(chain_id, error.to_owned());
        self
    }

    pub fn with_range_query_failure(
        mut self,
        chain_id: u64,
        from_block: u64,
        to_block: u64,
        error: &str,
    ) -> Self {
        self.range_query_failures
            .insert((chain_id, from_block, to_block), error.to_owned());
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
        if let Some(error) =
            self.range_query_failures
                .get(&(query.chain_id, query.from_block, query.to_block))
        {
            anyhow::bail!("{error}");
        }
        if let Some(error) = self.query_failures.get(&query.chain_id) {
            anyhow::bail!("{error}");
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
        })
    }

    async fn query_transactions(
        &self,
        query: DatalensTransactionQuery,
    ) -> anyhow::Result<DatalensTransactionQueryResult> {
        self.transaction_queries
            .lock()
            .expect("transaction queries lock")
            .push(query.clone());
        Ok(DatalensTransactionQueryResult {
            transactions: self
                .transactions
                .iter()
                .filter(|transaction| {
                    transaction.block_number >= query.from_block
                        && transaction.block_number <= query.to_block
                })
                .cloned()
                .collect(),
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

pub struct FailingEventWriterWithMessage(pub &'static str);

impl EventWriter for FailingEventWriterWithMessage {
    async fn write_events(&self, _events: &[LegacyOrmPEvent]) -> anyhow::Result<usize> {
        anyhow::bail!(self.0);
    }
}
