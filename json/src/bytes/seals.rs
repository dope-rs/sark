pub trait Owner {}
pub trait Text {}
pub trait Source {}

use o3::buffer::storage;
use sark_http::request;

use crate::{Bytes, Token};

impl Owner for &[u8] {}
impl<const N: usize> Owner for &[u8; N] {}
impl Owner for &str {}
impl Owner for String {}
impl Owner for Vec<u8> {}
impl Owner for storage::Shared {}
impl Owner for request::Bytes<'_> {}
impl Owner for Bytes<'_> {}
impl<const N: usize> Owner for Token<N> {}
impl Text for str {}
impl Text for String {}
impl Source for &[u8] {}
impl Source for &storage::Shared {}
