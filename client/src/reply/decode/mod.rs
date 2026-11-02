use std::convert::Infallible;

use o3::{base::cell::region, buffer::resident};
use sark_http::{
    h1::head::response::Coding,
    payload::compress::inflate::{self, Checked},
};

use crate::reply::{Scoped, body};

mod policy;

pub use policy::Policy;

/// Reusable response content decoder bound to one resident domain.
pub struct Decoder<'d> {
    gunzip: region::Value<region::Identity<'d>, inflate::Gzip>,
    budget: resident::Budget<'d>,
    limit: usize,
    policy: Policy,
}

impl<'d> Decoder<'d> {
    pub fn new(
        policy: Policy,
        limit: usize,
        budget: resident::Budget<'d>,
        token: &region::Token<region::Identity<'d>>,
    ) -> Self {
        Self {
            gunzip: token.bind(inflate::Gzip::default()),
            budget,
            limit,
            policy,
        }
    }

    pub fn decode<'a>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        response: Scoped<'a, 'd>,
    ) -> Result<Scoped<'a, 'd>, crate::Error> {
        self.decode_inner::<false, Infallible>(token, response, &mut || Ok(()))
            .map_err(Checked::into_work)
    }

    /// Checks gzip progress and successful identity or lenient-preserved responses.
    /// Footer lookup traverses admitted segments; allocation/reallocation are indivisible.
    /// Checkpoint failures stay distinct from decode failures; checking is cooperative.
    pub fn decode_with<'a, E>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        response: Scoped<'a, 'd>,
        mut check: impl FnMut() -> Result<(), E>,
    ) -> Result<Scoped<'a, 'd>, Checked<crate::Error, E>> {
        check().map_err(Checked::Check)?;
        self.decode_inner::<true, E>(token, response, &mut check)
    }

    fn decode_inner<'a, const CHECK: bool, E>(
        &self,
        token: &mut region::Token<region::Identity<'d>>,
        mut response: Scoped<'a, 'd>,
        check: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Scoped<'a, 'd>, Checked<crate::Error, E>> {
        if response.coding != Coding::Gzip {
            return Ok(response);
        }
        let decoded = apply::<CHECK, E>(
            token.borrow_mut(&self.gunzip),
            response.body(),
            self.limit,
            &self.budget,
            check,
        );
        match decoded {
            Ok(body) => {
                response
                    .body
                    .replace_with(body)
                    .map_err(|error| Checked::Work(crate::Error::Capacity(error)))?;
                response.omit_coding();
            }
            Err(Checked::Work(error)) if self.policy == Policy::Strict => {
                return Err(Checked::Work(crate::Error::Decompression(error)));
            }
            Err(Checked::Work(_)) => {}
            Err(Checked::Check(error)) => return Err(Checked::Check(error)),
        }
        if CHECK {
            check().map_err(Checked::Check)?;
        }
        Ok(response)
    }
}

fn apply<'d, const CHECK: bool, E>(
    gunzip: &mut inflate::Gzip,
    body: &body::Scoped<'_, 'd>,
    limit: usize,
    budget: &resident::Budget<'d>,
    check: &mut impl FnMut() -> Result<(), E>,
) -> Result<resident::Shared<'d>, Checked<inflate::DecodeError, E>> {
    let expected = body
        .suffix::<4>()
        .map_or(0, |footer| u32::from_le_bytes(footer) as usize);
    let plan = if CHECK {
        gunzip.plan_with(body.segments(), expected, &mut *check)
    } else {
        gunzip
            .try_plan(body.segments(), expected)
            .map_err(Checked::Work)
    }
    .map_err(|error| error.map_work(inflate::DecodeError::Stream))?;
    let output = if CHECK {
        gunzip.owned_with(plan, limit, budget, check)
    } else {
        gunzip
            .decode_owned(plan, limit, budget)
            .map_err(Checked::Work)
    }?;
    Ok(output.freeze())
}
