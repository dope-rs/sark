//! Zero-copy synchronous HTTP/2 execution.

use dope::core::BranchError;
use o3::alloc;

use crate::{
    conn::{Build, Error},
    frame::numbers,
};

mod book;
mod conn;
mod dispatch;
mod endpoint;
mod route;

pub use book::Book;
pub use conn::Conn;
pub use dispatch::Dispatch;
pub use endpoint::Endpoint;
#[doc(hidden)]
pub use route::Route;

#[derive(Eq, PartialEq, Debug)]
pub enum Create {
    Connection(Build),
    Book(alloc::Error),
}

fn response(
    error: BranchError<Error, numbers::ErrorCode>,
    report: &mut impl FnMut(Error),
) -> numbers::ErrorCode {
    match error {
        BranchError::First(error) => {
            let code = numbers::ErrorCode::from(&error);
            report(error);
            code
        }
        BranchError::Second(code) => code,
    }
}
