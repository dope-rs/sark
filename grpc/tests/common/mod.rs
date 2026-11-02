pub mod codec;
pub mod grpc;
pub mod headers;
pub mod messages;

use dope::core::driver::life::recovery::Recovery;
use std::convert::Infallible;

pub fn recover(failure: Recovery<'_, '_>) -> Infallible {
    panic!("native recovery: {:?}", failure.cause())
}
