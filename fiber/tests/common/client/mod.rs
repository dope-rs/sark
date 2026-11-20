use dope::core::io::Reclaimed;

pub use super::server::recover;

pub fn assert_shutdown(event: Reclaimed) {
    std::assert_matches!(event, Reclaimed::Shutdown);
}

pub mod config;

pub mod gzip;

pub mod response;

pub mod pool;

pub mod routes;

pub mod retry;

pub mod tls;

pub struct Cause<'a>(pub &'a str);

pub mod runtime;
