use core::{convert::Infallible, marker::PhantomData};

use o3::mem::Rejected;
use request::body::Source;
use sark_http::{method::Method, owner::handoff::input, payload::owned, request};

use crate::{
    body,
    decode::{Decode, Scoped, exec, state},
    manifold::stream::Boundary,
};

pub mod admit;
mod project;

pub use project::Project;

/// A selected request whose guard has admitted it.
#[repr(C)]
pub struct Admitted<K, P, B> {
    prepared: P,
    body: B,
    kind: PhantomData<fn() -> K>,
}

impl<K, P, B> Admitted<K, P, B> {
    pub(in crate::decode) const fn new(prepared: P, body: B) -> Self {
        Self {
            prepared,
            body,
            kind: PhantomData,
        }
    }

    pub const fn body(&self) -> &B {
        &self.body
    }

    pub const fn body_mut(&mut self) -> &mut B {
        &mut self.body
    }

    pub fn into_body(self) -> B {
        self.body
    }

    pub fn into_parts(self) -> (P, B) {
        (self.prepared, self.body)
    }

    pub fn try_map_body<T, E>(
        self,
        f: impl FnOnce(B) -> Result<T, E>,
    ) -> Result<Admitted<K, P, T>, Failure<K, P, E>> {
        match f(self.body) {
            Ok(body) => Ok(Admitted::new(self.prepared, body)),
            Err(error) => Err(Failure::new(error, self.prepared)),
        }
    }
}

impl<P, B> Admitted<exec::Duplex, P, B> {
    pub fn build<T, E>(self, f: impl FnOnce(P) -> Result<T, E>) -> Result<(T, B), (E, B)> {
        match f(self.prepared) {
            Ok(value) => Ok((value, self.body)),
            Err(error) => Err((error, self.body)),
        }
    }
}

impl<K, P, S> Admitted<K, P, body::Body<S>> {
    pub fn ready(self) -> Ready<K, P, S> {
        Ready::new(self.prepared, self.body)
    }
}

/// A selected, admitted request coupled to its completed body.
#[must_use]
#[repr(C)]
#[derive(Debug)]
pub struct Ready<K, P, S> {
    prepared: P,
    body: body::Body<S>,
    kind: PhantomData<fn() -> K>,
}

impl<K, P, S> Ready<K, P, S> {
    pub(in crate::decode) const fn new(prepared: P, body: body::Body<S>) -> Self {
        Self {
            prepared,
            body,
            kind: PhantomData,
        }
    }

    pub const fn body(&self) -> &body::Body<S> {
        &self.body
    }

    pub const fn prepared(&self) -> &P {
        &self.prepared
    }

    pub fn into_body(self) -> body::Body<S> {
        self.body
    }
}

impl<P, S> Ready<exec::Task, P, S> {
    pub fn into_parts(self) -> (P, body::Body<S>) {
        (self.prepared, self.body)
    }
}

impl<P, S> Ready<exec::Sync, P, S> {
    /// Borrows this completed body only for its matching selected request.
    ///
    pub fn try_project<'d, D, T, F>(
        self,
        decoder: &D,
        f: F,
    ) -> Result<(T, body::Body<S>), Project<exec::Sync, P, S>>
    where
        S: Source<'d>,
        D: Decode<Prepared = P>,
        F: for<'a> FnOnce(&D, Scoped<'a, 'd, D>) -> T,
    {
        let Self {
            prepared, mut body, ..
        } = self;
        let output = match decoder.try_project(prepared, body.input(), |view| f(decoder, view)) {
            Ok((output, ())) => output,
            Err(failure) => {
                let ((prepared, _), error) = failure.into_parts();
                return Err(Project::new(error, Ready::new(prepared, body)));
            }
        };
        Ok((output, body))
    }
}

impl<'d, P> Ready<exec::Sync, P, owned::Segments<'d>> {
    /// Keeps response selection and the matching final owner in one generative scope.
    pub fn with_input<T>(
        self,
        run: impl for<'owner> FnOnce(P, body::Input<input::Input<'owner, 'd>>) -> T,
    ) -> T {
        let Self { prepared, body, .. } = self;
        let (source, len) = body.into_parts();
        input::Input::with(source, |source| run(prepared, body::Input { source, len }))
    }
}

/// Failed body transformation that returns the exact selected request owner.
#[must_use]
#[derive(Debug)]
pub struct Failure<K, P, E> {
    error: E,
    prepared: P,
    kind: PhantomData<fn() -> K>,
}

impl<K, P, E> Failure<K, P, E> {
    const fn new(error: E, prepared: P) -> Self {
        Self {
            error,
            prepared,
            kind: PhantomData,
        }
    }

    pub const fn error(&self) -> &E {
        &self.error
    }

    /// Ends the rejected request owner and returns the transformation failure.
    pub fn discard(self) -> E {
        self.error
    }

    pub fn into_parts(self) -> (E, P) {
        (self.error, self.prepared)
    }
}

pub enum Body<O, F = O> {
    Open(O),
    Finished(F),
}

/// A routed request proved not to require response-capable admission.
#[must_use]
pub struct Plain<T>(T);

impl<T> Plain<T> {
    #[doc(hidden)]
    pub const fn new(state: T) -> Self {
        Self(state)
    }

    pub fn into_state(self) -> T {
        self.0
    }
}

/// A routed request carrying the exact method required by its selected guard.
#[must_use]
pub struct Guarded<T> {
    state: T,
    method: Method,
}

impl<T> Guarded<T> {
    #[doc(hidden)]
    pub const fn new(state: T, method: Method) -> Self {
        Self { state, method }
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Guarded<U> {
        Guarded::new(f(self.state), self.method)
    }

    pub fn fold<U>(self, f: impl FnOnce(T) -> U) -> U {
        f(self.state)
    }

    pub fn state_mut(&mut self) -> &mut T {
        &mut self.state
    }

    pub fn into_parts(self) -> (T, Method) {
        (self.state, self.method)
    }
}

/// Route-selected admission contract for an application containing both
/// guarded and unguarded routes.
#[must_use]
pub enum Start<U, G> {
    Plain(Plain<U>),
    Guarded(Guarded<G>),
}

impl<U, G> Start<U, G> {
    #[doc(hidden)]
    pub const fn plain(state: U) -> Self {
        Self::Plain(Plain::new(state))
    }

    #[doc(hidden)]
    pub const fn guarded(state: G, method: Method) -> Self {
        Self::Guarded(Guarded::new(state, method))
    }
}

/// Generated route family that owns a selected guarded request.
pub trait Selection<P> {
    type Guard: state::Family<P>;
    type Routed: state::Flow<P, state::Routed, Family = Self::Guard>;
    type Deferred<T>;
}

impl<P, U> Selection<P> for Plain<U>
where
    U: state::Flow<P, state::Routed>,
{
    type Guard = U::Family;
    type Routed = U;
    type Deferred<T> = Infallible;
}

impl<P, G> Selection<P> for Guarded<G>
where
    G: state::Flow<P, state::Routed>,
{
    type Guard = G::Family;
    type Routed = G;
    type Deferred<T> = T;
}

impl<P, U, G> Selection<P> for Start<U, G>
where
    G: state::Flow<P, state::Routed>,
{
    type Guard = G::Family;
    type Routed = G;
    type Deferred<T> = T;
}

/// Selected head that consumes its body plan into an executable request.
pub trait Head: Sized {
    type Prepared: Prepared;
    type Start: Selection<Self::Prepared>;

    fn try_start(
        self,
        limit: usize,
        boundary: Boundary,
    ) -> Result<Self::Start, Rejected<Self, body::Error>>;
}

/// Request facts that remain after body-plan admission.
pub trait Prepared: Sized {
    type Execution: exec::Set;
    type Family: state::Family<Self>;
}

/// A prepared request whose generated family contains only synchronous arms.
pub trait Synchronous:
    Prepared<Execution = exec::Sync, Family: state::Homogeneous<Self, exec::Sync>>
{
}

impl<P> Synchronous for P
where
    P: Prepared<Execution = exec::Sync>,
    P::Family: state::Homogeneous<P, exec::Sync>,
{
}

const _: () = {
    type Body = body::Body<()>;
    type ReadyProof = Ready<exec::Sync, (), ()>;
    assert!(core::mem::size_of::<ReadyProof>() == core::mem::size_of::<Body>());
    assert!(core::mem::align_of::<ReadyProof>() == core::mem::align_of::<Body>());
    assert!(
        core::mem::size_of::<Admitted<exec::Task, usize, usize>>()
            == 2 * core::mem::size_of::<usize>()
    );
};
