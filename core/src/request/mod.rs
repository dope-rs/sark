//! Validated request ownership and projection.

use std::{mem, ops, ops::Deref};

use o3::{
    buffer::{bytes::scope, pool, pool::owners, storage},
    mem::rc,
};
use sark_http::{
    field::metadata::{
        packed,
        section::storage::{Block, Bytes, Scope},
    },
    payload::owned,
    request,
    request::{Bound, Error, Form, Id, Layout, span},
};

use crate::{body, request::frame::proof};

pub mod field;
pub mod frame;
#[doc(hidden)]
pub mod mode;
pub mod path;
pub mod raw;
mod view;

use request::Source;
pub use view::View;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BindError {
    Layout(request::Error),
    Body(body::Error),
    Retain(rc::Error),
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct BodyLen {
    len: usize,
}

impl BodyLen {
    pub const fn new(len: usize) -> Self {
        Self { len }
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[doc(hidden)]
pub struct Parts<'d, P = proof::Maybe> {
    head: Head<storage::Shared, P>,
    id: Id<'d>,
}

impl<'d, P: proof::Proof> Parts<'d, P> {
    pub fn from_head(id: Id<'d>, head: Head<storage::Shared, P>) -> Self {
        Self { head, id }
    }

    pub fn try_frame_at(
        &self,
        range: ops::Range<usize>,
    ) -> Result<request::Bytes<'d>, frame::Error<rc::Error>> {
        let start = range.start;
        let end = range.end;
        let capacity = self.head.as_ref().len();
        if start > end {
            return Err(frame::Error::Order {
                start,
                end,
                capacity,
            });
        }
        let frame = self
            .head
            .owner()
            .try_get(start..end)
            .map_err(frame::Error::Retain)?
            .ok_or(frame::Error::Bounds {
                start,
                end,
                capacity,
            })?;
        Ok(request::Bytes::from_shared(self.id, frame))
    }

    #[doc(hidden)]
    pub const fn id(&self) -> Id<'d> {
        self.id
    }
}

#[doc(hidden)]
pub struct Retained<'d, P = proof::Maybe> {
    parts: Parts<'d, P>,
    body: body::Body<owned::Segments<'d>>,
}

impl<'d, P: proof::Proof> Retained<'d, P> {
    pub fn from_head(
        id: Id<'d>,
        head: Head<storage::Shared, P>,
        body: body::Body<owned::Segments<'d>>,
    ) -> Self {
        Self {
            parts: Parts::from_head(id, head),
            body,
        }
    }

    pub const fn body_len(&self) -> usize {
        self.body.len()
    }

    pub fn into_body(self) -> request::Segments<'d> {
        request::Segments::new(self.parts.id, self.body.into_body())
    }

    pub fn into_parts(self) -> (Parts<'d, P>, body::Body<owned::Segments<'d>>) {
        (self.parts, self.body)
    }
}

impl<'d, P> Deref for Retained<'d, P> {
    type Target = Parts<'d, P>;

    fn deref(&self) -> &Self::Target {
        &self.parts
    }
}

const _: () = assert!(mem::size_of::<View<'static, 'static>>() <= 10 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Parts<'static>>() <= 10 * mem::size_of::<usize>());
const _: () = assert!(mem::size_of::<Retained<'static>>() <= 14 * mem::size_of::<usize>());
const _: () =
    assert!(mem::size_of::<Parts<'static, proof::Peer>>() <= mem::size_of::<Parts<'static>>());
const _: () = assert!(
    mem::size_of::<Retained<'static, proof::Peer>>() <= mem::size_of::<Retained<'static>>()
);

/// A validated head over immutable wire storage.
///
pub struct Head<O, P = proof::Maybe> {
    owner: O,
    target: request::Target,
    authority: P,
    route: Route,
}

const _: () = {
    assert!(mem::size_of::<Head<&'static [u8]>>() == 5 * mem::size_of::<usize>());
    assert!(mem::size_of::<Option<Head<&'static [u8]>>>() == mem::size_of::<Head<&[u8]>>());
    assert!(
        mem::size_of::<Head<&'static [u8], proof::Peer>>() <= mem::size_of::<Head<&'static [u8]>>()
    );
    assert!(!mem::needs_drop::<Head<&'static [u8]>>());
};

impl<O: Source, P: proof::Proof> Head<O, P> {
    pub fn bind(owner: O, layout: request::Layout) -> Result<Self, BindError> {
        Self::try_bound(request::Bound::try_bind(owner, layout).map_err(BindError::Layout)?)
    }

    pub fn borrow(&self) -> Head<&[u8], P> {
        Head {
            owner: self.owner.as_ref(),
            target: self.target,
            authority: self.authority,
            route: self.route,
        }
    }
}

impl<O: Source, P> AsRef<[u8]> for Head<O, P> {
    fn as_ref(&self) -> &[u8] {
        self.owner.as_ref()
    }
}

impl<O, P> Head<O, P> {
    pub const fn form(&self) -> request::Form {
        self.target.form()
    }

    pub(in crate::request) const fn route(&self) -> Projection {
        self.route.path()
    }
    pub fn into_owner(self) -> O {
        self.owner
    }

    pub const fn owner(&self) -> &O {
        &self.owner
    }
}

impl<O> Head<O, proof::Peer> {
    pub(in crate::request) const fn peer(&self) -> span::Span {
        self.authority.get()
    }
}

impl<P, const PREFIX: usize> From<Head<packed::Decoded<o3::buffer::pool::Span, PREFIX>, P>>
    for Head<Bytes, P>
{
    fn from(head: Head<packed::Decoded<o3::buffer::pool::Span, PREFIX>, P>) -> Self {
        Self {
            owner: head.owner.into_bytes(),
            target: head.target,
            authority: head.authority,
            route: head.route,
        }
    }
}

impl<P: proof::Proof> Head<Bytes, P> {
    pub fn from_path(
        owner: Bytes,
        target: ops::Range<usize>,
        path_len: usize,
        peer: Option<ops::Range<usize>>,
    ) -> Result<Self, BindError> {
        let error = |cause| BindError::Layout(request::Error::Span(cause));
        let target = span::Span::from_range(target).map_err(error)?;
        let route = span::Span::checked(target.start(), path_len).map_err(error)?;
        let target =
            request::Target::from_span(request::Form::Path, target).map_err(BindError::Layout)?;
        let peer = peer
            .map(span::Span::from_range)
            .transpose()
            .map_err(error)?;
        let layout = request::Layout::new(target, peer, Some(route));
        Self::bind(owner, layout)
    }

    pub fn retain(self) -> Head<storage::Shared, P> {
        Head {
            owner: self.owner.into(),
            target: self.target,
            authority: self.authority,
            route: self.route,
        }
    }
}

#[derive(Clone, Copy)]
enum Route {
    Frame(span::Span),
    Literal(Literal),
}

#[derive(Clone, Copy)]
enum Literal {
    Root,
    Star,
}

pub(in crate::request) enum Projection {
    Frame(ops::Range<usize>),
    Root,
    Star,
}

impl Route {
    const fn path(self) -> Projection {
        match self {
            Self::Frame(span) => Projection::Frame(span.start()..span.end()),
            Self::Literal(Literal::Root) => Projection::Root,
            Self::Literal(Literal::Star) => Projection::Star,
        }
    }
}

const _: () = assert!(mem::size_of::<Route>() == mem::size_of::<span::Span>());

impl<O: Scope, P> Head<O, P> {
    pub fn scope<T>(self, use_head: impl for<'id> FnOnce(Head<O::Scoped<'id>, P>) -> T) -> T {
        let Self {
            owner,
            target,
            authority: peer,
            route,
        } = self;
        owner.scope(|owner| {
            use_head(Head {
                owner,
                target,
                authority: peer,
                route,
            })
        })
    }
}

impl<P>
    Head<
        Bytes<scope::Owner<'_, pool::Span<owners::Plain<pool::Layout>, pool::Owned, pool::Frozen>>>,
        P,
    >
{
    pub fn into_inner(self) -> Head<Bytes, P> {
        Head {
            owner: self.owner.into_inner(),
            target: self.target,
            authority: self.authority,
            route: self.route,
        }
    }
}

impl<O: Source, P: proof::Proof> Head<O, P> {
    pub fn try_bound(bound: Bound<O>) -> Result<Self, BindError> {
        let (owner, layout) = bound.into_parts();
        // SAFETY: Bound retains the immutable owner validated with this layout.
        unsafe { Self::try_parts(owner, layout) }
    }

    /// # Safety
    /// `layout` must be valid for the exact immutable bytes retained by `owner`.
    unsafe fn try_parts(owner: O, layout: Layout) -> Result<Self, BindError> {
        let target = layout.target();
        let proof = P::try_from(layout).map_err(BindError::Layout)?;
        let route = match target.form() {
            Form::Path | Form::Extended => {
                let route = layout
                    .route()
                    .ok_or(BindError::Layout(Error::InvalidRange))?;
                Route::Frame(route)
            }
            Form::Root => Route::Literal(Literal::Root),
            Form::Asterisk | Form::Authority => Route::Literal(Literal::Star),
        };
        Ok(Self {
            owner,
            target,
            authority: proof,
            route,
        })
    }
}

impl<'a, P> Head<&'a [u8], P> {
    pub fn path(&self) -> &'a [u8] {
        match self.route() {
            Projection::Frame(range) => {
                // SAFETY: bind validates this route against the retained owner.
                unsafe { self.owner.get_unchecked(range) }
            }
            Projection::Root => b"/",
            Projection::Star => b"*",
        }
    }
}

impl<'a, P: proof::Proof> Head<&'a [u8], P> {
    pub fn authority(&self) -> Option<&'a [u8]> {
        self.authority.span().map(|span| {
            // SAFETY: bind validates this authority against the retained owner.
            unsafe { self.owner.get_unchecked(span.start()..span.end()) }
        })
    }
}
