#[doc(hidden)]
pub use o3;
#[doc(hidden)]
pub use pin_project_lite::pin_project as __pin_project;
#[doc(hidden)]
pub use sark_core;
pub use sark_core::{error, http, utils};
pub mod app;
#[doc(hidden)]
pub mod date;
#[doc(hidden)]
pub mod dispatch;
#[doc(hidden)]
pub mod fiber;
#[doc(hidden)]
pub mod parser;
#[doc(hidden)]
pub use parser::framer;
pub mod fs;
pub mod middleware;
pub mod request;
#[doc(hidden)]
pub mod timer;
pub use timer::{Timer, TimerHost};

pub mod routes;
pub mod service;

pub use dope::manifold::listener;
#[doc(hidden)]
pub use dope::manifold::listener::Application;
pub use dope_fiber::fiber_fn;
pub use dope_net::{tcp, tcp::Tcp};
pub use sark_gen::body;

#[macro_export]
macro_rules! fiber {
    ($($token:tt)*) => {
        ::dope_fiber::fiber!($($token)*)
    };
}

#[doc(hidden)]
pub const CANNED_400: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
#[doc(hidden)]
pub const CANNED_404: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
#[doc(hidden)]
pub const CANNED_408: &[u8] = b"HTTP/1.1 408 Request Timeout\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
#[doc(hidden)]
pub const CANNED_413: &[u8] = b"HTTP/1.1 413 Payload Too Large\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
#[doc(hidden)]
pub const CANNED_500: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
#[doc(hidden)]
pub const CANNED_503: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nServer: sark\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

pub use app::{
    Balanced, Executor, HttpServer, HttpsServer, LowLatency, RuntimeProfile, Throughput, driver,
};
pub use sark_json as json;

pub struct EmptyState;

impl EmptyState {
    pub const REF: &'static Self = &EmptyState;
}
