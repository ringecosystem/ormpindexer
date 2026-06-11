use crate::datalens::DatalensLog;
use crate::schema::LegacyOrmPEvent;

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
