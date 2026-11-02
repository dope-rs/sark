use o3::buffer::storage;

use crate::{Plain, Raw, Text};

pub trait Sealed {}
impl Sealed for Text<storage::Shared> {}
impl Sealed for Plain<storage::Shared> {}
impl Sealed for Raw<storage::Shared> {}
