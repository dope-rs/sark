use o3::buffer::{resident, segments};
use sark_http::payload::owned;

use crate::conn::output::WritePayload;

pub trait Sealed {}

impl Sealed for WritePayload<'_> {}
impl Sealed for Vec<u8> {}
impl Sealed for owned::Segments<'_> {}
impl Sealed for segments::Nonempty<resident::Shared<'_>> {}
