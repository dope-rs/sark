use o3::base::cell::region;

use crate::body::credit;

pub trait Seal {}

impl Seal for credit::raw::Stored {}
impl<'id> Seal for region::Proof<region::Identity<'id>> {}
