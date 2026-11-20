use dope::{
    core::{BranchError, driver::settings::errors::Claim, io::socket::Listener},
    runtime::shutdown::ReadySource,
};
use sark_h1::server::run::prepare;
use sark_http::payload::response::status;

use crate::common::{route::body::OwnedReply, server};

#[sark_fiber::handler(pub)]
#[gzip(required)]
fn gzip() -> OwnedReply {
    let mut body = Vec::new();
    body.extend_from_slice(b"hello");
    OwnedReply {
        status: status::Code::OK,
        body,
    }
}

sark_fiber::define_route! {
    pub HelloDispatch: () => {
        GET "/hello" => gzip,
    }
}

pub fn try_serve(
    plan: server::Http1Plan,
    context: ReadySource<'_>,
    listener: Listener,
) -> Result<(), BranchError<prepare::Error, Claim>> {
    let mut reports = server::Plan::try_reports(&plan).map_err(BranchError::Second)?;
    server::serve::<HelloDispatch, _>(
        plan.with_bound(listener),
        context,
        (),
        server::MAX_CONNECTIONS,
        &mut reports,
    )?;
    server::reclaimed(&reports);
    Ok(())
}
