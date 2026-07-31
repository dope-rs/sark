mod flags;
mod known_header;

pub use flags::{Flags, SeenHeaderHandler};
pub use known_header::{
    CSV_CHUNKED_BIT, CSV_CLOSE_BIT, CSV_CONTINUE_BIT, CSV_KEEP_ALIVE_BIT, KnownHeader,
};
pub use sark_protocol::is_header_name_byte;

pub const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
