use o3::buffer::storage;

use crate::{
    payload::{fixed, owned},
    request,
    request::body,
};

pub trait Sealed {}

impl Sealed for [u8; 0] {}
impl Sealed for &[u8] {}
impl Sealed for &str {}
impl Sealed for Vec<u8> {}
impl Sealed for String {}
impl Sealed for storage::Owned {}
impl Sealed for storage::Shared {}
impl Sealed for fixed::Body<'_> {}
impl Sealed for owned::Segment<'_> {}
impl Sealed for owned::Segments<'_> {}
impl Sealed for request::Bytes<'_> {}
impl Sealed for request::Segments<'_> {}
impl Sealed for body::Ref<'_, '_> {}
