use std::convert::Infallible;

use crate::{
    owner::handoff::{input, stage},
    request::body,
};

pub trait Seal {}

impl Seal for Infallible {}
impl Seal for () {}
impl Seal for body::Retained<'_, '_> {}
impl<P: stage::Proof> Seal for input::Bound<'_, '_, P> {}
impl Seal for input::Selected<'_> {}
