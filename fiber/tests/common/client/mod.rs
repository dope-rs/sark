use dope::core::driver::life::recovery::Recovery;

pub fn recover(failure: Recovery<'_, '_>) -> std::convert::Infallible {
    panic!("native client failure: {:?}", failure.cause())
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
