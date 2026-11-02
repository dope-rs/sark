use std::ops;

use o3::buffer::{bytes::scope, pool, pool::owners, storage};
use sark_http::{
    field::metadata::section::storage::Bytes,
    payload::owned,
    request,
    request::{Id, Layout, Source},
};

use crate::{
    body,
    request::{BindError, Head, Parts, Retained, frame::proof},
    route,
};

pub trait Bind: request::raw::Bind {
    /// # Safety
    /// `self` and all ranges must satisfy [`request::raw::Bind::try_bind`]'s native decoder,
    /// closed policy, and exact original field-range requirements.
    unsafe fn try_head<P: proof::Proof>(
        self,
        path: Option<ops::Range<usize>>,
        path_len: Option<usize>,
        pseudo: Option<ops::Range<usize>>,
        host: Option<ops::Range<usize>>,
    ) -> Result<Head<Self::Owner, P>, BindError> {
        // SAFETY: the caller preserves the exact native head and field ranges.
        let bound = unsafe { request::raw::Bind::try_bind(self, path, path_len, pseudo, host) }
            .map_err(BindError::Layout)?;
        Head::try_bound(bound)
    }
}

impl<H: request::raw::Bind> Bind for H {}

/// A parsed schema bound to the immutable request that produced its fields.
pub struct Parsed<H, R: route::Schema> {
    owner: H,
    params: R::RawParams,
    headers: R::RawHeaders,
}

impl<O: Source, F: proof::Proof, R: route::Schema<Frame = F>> Parsed<Head<O, F>, R> {
    /// # Safety
    /// `layout` must be valid for the exact bytes retained by `owner`, and both field sets must be produced by `R` from those bytes.
    /// Replacing the owner must preserve the complete head bytes and their offsets.
    pub unsafe fn try_bind(
        owner: O,
        layout: Layout,
        params: R::RawParams,
        headers: R::RawHeaders,
    ) -> Result<Self, BindError> {
        // SAFETY: the caller carries the admitted layout with its exact owner.
        let owner = unsafe { Head::try_parts(owner, layout) }?;
        // SAFETY: the same owner retains both schema field sets' byte origin.
        Ok(unsafe { Self::new(owner, params, headers) })
    }

    /// # Safety
    /// Both field sets must be produced by `R` from this exact head.
    pub const unsafe fn new(
        owner: Head<O, F>,
        params: R::RawParams,
        headers: R::RawHeaders,
    ) -> Self {
        Self {
            owner,
            params,
            headers,
        }
    }

    pub fn borrow(&self) -> Parsed<Head<&[u8], F>, R> {
        Parsed {
            owner: self.owner.borrow(),
            params: self.params,
            headers: self.headers,
        }
    }
}

impl<H, R: route::Schema> Parsed<H, R> {
    pub const fn owner(&self) -> &H {
        &self.owner
    }

    pub fn into_owner(self) -> H {
        self.owner
    }

    pub const fn params(&self) -> &R::RawParams {
        &self.params
    }

    pub const fn headers(&self) -> &R::RawHeaders {
        &self.headers
    }
}

impl<F: proof::Proof, R: route::Schema<Frame = F>> Parsed<Head<Bytes, F>, R> {
    pub fn retain(self) -> Parsed<Head<storage::Shared, F>, R> {
        Parsed {
            owner: self.owner.retain(),
            params: self.params,
            headers: self.headers,
        }
    }
}

impl<F: proof::Proof, R: route::Schema<Frame = F>> Parsed<Head<storage::Shared, F>, R> {
    pub fn with_id<'d>(self, id: Id<'d>) -> Parsed<Parts<'d, F>, R> {
        Parsed {
            owner: Parts::from_head(id, self.owner),
            params: self.params,
            headers: self.headers,
        }
    }
}

impl<'d, F: proof::Proof, R: route::Schema<Frame = F>> Parsed<Parts<'d, F>, R> {
    pub fn with_body(self, body: body::Body<owned::Segments<'d>>) -> Parsed<Retained<'d, F>, R> {
        Parsed {
            owner: Retained {
                parts: self.owner,
                body,
            },
            params: self.params,
            headers: self.headers,
        }
    }
}

impl<'d, F: proof::Proof, R: route::Schema<Frame = F>> Parsed<Retained<'d, F>, R> {
    pub fn into_parts(self) -> (Parsed<Parts<'d, F>, R>, body::Body<owned::Segments<'d>>) {
        let (parts, body) = self.owner.into_parts();
        (
            Parsed {
                owner: parts,
                params: self.params,
                headers: self.headers,
            },
            body,
        )
    }
}

impl<F: proof::Proof, R: route::Schema<Frame = F>> Parsed<Head<Bytes, F>, R> {
    pub fn scope<T>(
        self,
        use_head: impl for<'id> FnOnce(
            Parsed<
                Head<
                    Bytes<
                        scope::Owner<
                            'id,
                            pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
                        >,
                    >,
                    F,
                >,
                R,
            >,
        ) -> T,
    ) -> T {
        let Self {
            owner,
            params,
            headers,
        } = self;
        owner.scope(|owner| {
            use_head(Parsed {
                owner,
                params,
                headers,
            })
        })
    }
}

impl<F: proof::Proof, R: route::Schema<Frame = F>>
    Parsed<
        Head<
            Bytes<
                scope::Owner<
                    '_,
                    pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>,
                >,
            >,
            F,
        >,
        R,
    >
{
    pub fn into_inner(self) -> Parsed<Head<Bytes, F>, R> {
        Parsed {
            owner: self.owner.into_inner(),
            params: self.params,
            headers: self.headers,
        }
    }
}
