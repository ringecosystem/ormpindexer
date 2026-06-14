use crate::{
    datalens::DatalensLog,
    decoder::{decode_evm_log, decode_tron_event},
    planner::{TRON_CHAIN_ID, TRON_MESSAGE_RECV_EVENT, TRON_MESSAGE_SENT_EVENT},
    schema::LegacyOrmPEvent,
};

#[allow(async_fn_in_trait)]
pub trait EventDecoder {
    async fn decode(&self, log: &DatalensLog) -> anyhow::Result<Vec<LegacyOrmPEvent>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopDecoder;

impl EventDecoder for NoopDecoder {
    async fn decode(&self, _log: &DatalensLog) -> anyhow::Result<Vec<LegacyOrmPEvent>> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EvmEventDecoder;

impl EventDecoder for EvmEventDecoder {
    async fn decode(&self, log: &DatalensLog) -> anyhow::Result<Vec<LegacyOrmPEvent>> {
        if log.chain_id == TRON_CHAIN_ID {
            if matches!(
                log.event_name.as_deref(),
                Some(TRON_MESSAGE_RECV_EVENT | TRON_MESSAGE_SENT_EVENT)
            ) {
                return Ok(Vec::new());
            }
            return decode_tron_event(log).map(|event| vec![event]);
        }

        decode_evm_log(log).map(|event| vec![event])
    }
}
