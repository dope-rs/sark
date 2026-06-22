use std::ops::Range;

use http::{Method, Version};

pub struct ParsedRequest {
    pub method: Method,
    pub version: Version,
    pub path_range: Range<usize>,
    pub uri_path_end: usize,
}
