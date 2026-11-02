use o3::depot::{arena::hash, capacity};

use crate::{
    conn::options::plan,
    manifold::{
        requests::book::settings,
        server::{Config, ConfigError},
    },
};

#[derive(Clone)]
pub(in crate::manifold::server) struct Setup {
    pub(in crate::manifold::server) protocol: plan::Config,
    pub(in crate::manifold::server) requests: settings::Plan,
}

impl Setup {
    pub(in crate::manifold::server) fn try_new(config: Config) -> Result<Self, ConfigError> {
        let protocol = plan::Config::new(config.protocol).map_err(ConfigError::Protocol)?;
        let slots = hash::Plan::try_from(protocol.stream_plan()).map_err(ConfigError::Requests)?;
        let chunks = capacity::Count::try_from(config.max_connection_body_chunks)
            .map_err(ConfigError::Chunks)?;
        let requests = settings::Plan::new(
            slots,
            chunks,
            settings::Limits {
                max_request_body_bytes: config.max_request_body_bytes,
                max_body_chunks: config.max_body_chunks,
                max_connection_body_bytes: config.max_connection_body_bytes,
                max_connection_body_chunks: config.max_connection_body_chunks,
            },
        );
        Ok(Self { protocol, requests })
    }
}
