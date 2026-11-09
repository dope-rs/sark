use o3::buffer::storage;

use crate::field::metadata::{packed, section::storage::Bytes};

pub trait Seal {}

impl Seal for &[u8] {}
impl<const N: usize> Seal for &[u8; N] {}
impl Seal for storage::Shared {}
impl<S: AsRef<[u8]>> Seal for Bytes<S> {}
impl<S: AsRef<[u8]>, const PREFIX: usize> Seal for packed::Decoded<S, PREFIX> {}
