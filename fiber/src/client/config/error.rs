use dope::{
    core::driver::settings::errors,
    manifold::{client::port::error, service::health},
};
use dope_fiber::reply::layout;
use o3::{buffer::pool, depot::capacity};
use sark_http::uri::target;

#[derive(Debug)]
pub enum Error {
    ConnectorConfig(health::Error),
    ConnectorClaim(errors::Claim),
    ResponseEntries,
    ResponseItems,
    ResponseBytes,
    ResponseLayout(layout::Error),
    RequestSlots,
    RequestLayout(pool::LayoutError),
    RequestResidentCapacity,
    ConnectorCapacity(error::Plan),
    FailureReports {
        live: usize,
        retained: usize,
    },
    FailureReportCapacity(capacity::Error),
    ConnectorConnections {
        connections: usize,
        response_lanes: usize,
    },
    HostUserInfo,
    HostUri(target::Error),
    HostPath,
    HostAuthority(target::Error),
}
