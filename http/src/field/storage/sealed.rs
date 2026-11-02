use std::borrow::Cow;

use o3::buffer::storage::Shared;

use crate::field::storage::{Bytes, Constant, Static};

pub trait Sealed {}

impl<T: Constant> Sealed for Static<T> {}
impl Sealed for str {}
impl Sealed for String {}
impl Sealed for [u8] {}
impl<const N: usize> Sealed for [u8; N] {}
impl Sealed for Vec<u8> {}
impl Sealed for Shared {}
impl Sealed for Cow<'_, str> {}
impl Sealed for Cow<'_, [u8]> {}
impl<T: Bytes + ?Sized> Sealed for &T {}
impl<T: Bytes + ?Sized> Sealed for Box<T> {}
