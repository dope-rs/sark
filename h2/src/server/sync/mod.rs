//! Zero-copy synchronous HTTP/2 execution.

use o3::alloc;

use crate::conn::Build;

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
