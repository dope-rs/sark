//! Canonical Sark protocol and generated-routing facade.

pub use sark_client as client;
pub use sark_core as core;
pub use sark_core::{
    manifold::EmptyState,
    request,
    route::guard::{Guard, Head},
};
pub use sark_gen::{
    define_cleartext_route, define_route, define_router, define_task_route, duplex, handler,
    header_name, request, response,
};
pub use sark_http as http;
