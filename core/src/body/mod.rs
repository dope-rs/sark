use core::{fmt, marker, num::NonZero};

use o3::mem::Rejected;
use sark_http::{
    owner::handoff::{
        self,
        input::{Borrowed, Bound},
    },
    payload::owned,
    request::body,
};

pub mod admit;
pub mod buffered;
pub mod credit;
pub mod flow;
pub mod ingress;
pub mod input;
pub mod stream;

pub const DEFAULT_MAX_REQUEST_CHUNKS: usize = 4 * 1024;
pub const DEFAULT_MAX_CONNECTION_CHUNKS: usize = 16 * 1024;
pub const DEFAULT_BUILD_STEP_BUDGET: NonZero<usize> =
    NonZero::<usize>::MIN.saturating_add(4 * 1024 - 1);

/// Projection keeps the exact final owner or returns the original source on rejection.
pub type Projection<T, O, Source> = Result<(T, O), Rejected<Source, body::Error>>;

pub trait Project: Sized {
    type View<'req, 'd>: Into<body::Ref<'req, 'd>> + Copy
    where
        'd: 'req;

    const RETENTION: admit::Retention;

    fn empty<'req, 'd: 'req>() -> Scoped<'req, 'd, Self>;

    fn try_project<'a, 'd, S, V, T>(
        body: &'a mut Body<S>,
        value: V,
        project: impl for<'view> FnOnce(Scoped<'view, 'd, Self, V>) -> T,
    ) -> Result<T, Rejected<V, body::Error>>
    where
        S: body::Source<'d>,
        'd: 'a,
    {
        let input = body.input();
        Self::try_input(input, value, |scope| {
            let (value, body) = scope.into_parts();
            project(Scoped::new(body.value, value))
        })
        .map(|(output, ())| output)
        .map_err(|failure| {
            let ((_, value), error) = failure.into_parts();
            Rejected::new(value, error)
        })
    }

    /// Projects one affine source and returns its exact final owner.
    fn try_input<'d, I, V, T>(
        input: Input<I>,
        value: V,
        project: impl for<'view> FnOnce(
            Scoped<'view, 'd, Self, V, handoff::Slot<'view, 'd, (), Bound<'view, 'd, I::Proof>>>,
        ) -> T,
    ) -> Projection<T, I::Owner, (Input<I>, V)>
    where
        I: handoff::Project<'d>;
}

pub enum Contiguous {}

impl Project for Contiguous {
    type View<'req, 'd>
        = body::View<'req, 'd>
    where
        'd: 'req;

    const RETENTION: admit::Retention = admit::Retention::Retained;

    fn empty<'req, 'd: 'req>() -> Scoped<'req, 'd, Self> {
        Scoped::<Self>::new(body::View::Slice(&[]), ())
    }

    fn try_input<'d, I, V, T>(
        input: Input<I>,
        value: V,
        project: impl for<'view> FnOnce(
            Scoped<'view, 'd, Self, V, handoff::Slot<'view, 'd, (), Bound<'view, 'd, I::Proof>>>,
        ) -> T,
    ) -> Projection<T, I::Owner, (Input<I>, V)>
    where
        I: handoff::Project<'d>,
    {
        let Input { source, len } = input;
        source
            .contiguous(value, |value, view, source| {
                project(Scoped::from_source(view, value, source))
            })
            .map_err(|(source, value, error)| Rejected::new((Input { source, len }, value), error))
    }
}

pub enum Discarded {}

impl Project for Discarded {
    type View<'req, 'd>
        = Len
    where
        'd: 'req;

    const RETENTION: admit::Retention = admit::Retention::Discarded;

    fn empty<'req, 'd: 'req>() -> Scoped<'req, 'd, Self> {
        Scoped::<Self>::new(Len(0), ())
    }

    fn try_input<'d, I, V, T>(
        input: Input<I>,
        value: V,
        project: impl for<'view> FnOnce(
            Scoped<'view, 'd, Self, V, handoff::Slot<'view, 'd, (), Bound<'view, 'd, I::Proof>>>,
        ) -> T,
    ) -> Projection<T, I::Owner, (Input<I>, V)>
    where
        I: handoff::Project<'d>,
    {
        let Input { source, len } = input;
        Ok(source.discarded(value, |value, source| {
            project(Scoped::from_source(Len(len), value, source))
        }))
    }
}

pub enum Segmented {}

impl Project for Segmented {
    type View<'req, 'd>
        = body::Ref<'req, 'd>
    where
        'd: 'req;

    const RETENTION: admit::Retention = admit::Retention::Retained;

    fn empty<'req, 'd: 'req>() -> Scoped<'req, 'd, Self> {
        Scoped::<Self>::new(body::Ref::from_slice(&[]), ())
    }

    fn try_input<'d, I, V, T>(
        input: Input<I>,
        value: V,
        project: impl for<'view> FnOnce(
            Scoped<'view, 'd, Self, V, handoff::Slot<'view, 'd, (), Bound<'view, 'd, I::Proof>>>,
        ) -> T,
    ) -> Projection<T, I::Owner, (Input<I>, V)>
    where
        I: handoff::Project<'d>,
    {
        Ok(input.source.segmented(value, |value, view, source| {
            project(Scoped::from_source(view, value, source))
        }))
    }
}

/// Projection coupled to its storage lifetime and the owned call context.
///
#[repr(C)]
pub struct Scoped<'req, 'd: 'req, P: Project, V: 'req = (), Q = ()> {
    value: P::View<'req, 'd>,
    context: V,
    source: Q,
    proof: marker::PhantomData<(&'req &'d (), &'req V)>,
}

impl<'req, 'd: 'req, P: Project, V: 'req> Scoped<'req, 'd, P, V> {
    const fn new(value: P::View<'req, 'd>, context: V) -> Self {
        Self::from_source(value, context, ())
    }
}

impl<'req, 'd: 'req, P: Project, V: 'req, Q> Scoped<'req, 'd, P, V, Q> {
    const fn from_source(value: P::View<'req, 'd>, context: V, source: Q) -> Self {
        Self {
            value,
            context,
            source,
            proof: marker::PhantomData,
        }
    }

    pub fn into_parts(self) -> (V, Scoped<'req, 'd, P>) {
        (self.context, Scoped::new(self.value, ()))
    }

    pub fn into_source(self) -> (V, Scoped<'req, 'd, P>, Q) {
        (self.context, Scoped::new(self.value, ()), self.source)
    }
}

impl<'req, 'd: 'req, P: Project> Scoped<'req, 'd, P> {
    pub fn into_inner(self) -> P::View<'req, 'd> {
        self.value
    }

    pub fn body(&self) -> body::Ref<'req, 'd> {
        self.value.into()
    }
}

const _: () = {
    use core::mem::{align_of, size_of};

    assert!(size_of::<Scoped<'static, 'static, Discarded>>() == size_of::<Len>());
    assert!(
        size_of::<Scoped<'static, 'static, Contiguous>>()
            == size_of::<body::View<'static, 'static>>()
    );
    assert!(
        size_of::<Option<Scoped<'static, 'static, Contiguous>>>()
            == size_of::<body::View<'static, 'static>>()
    );
    assert!(
        size_of::<Scoped<'static, 'static, Segmented>>()
            == size_of::<body::Ref<'static, 'static>>()
    );
    assert!(
        align_of::<Scoped<'static, 'static, Segmented>>()
            == align_of::<body::Ref<'static, 'static>>()
    );
};

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Len(usize);

impl<'req, 'd> From<Len> for body::Ref<'req, 'd> {
    fn from(length: Len) -> Self {
        Self::discarded(length.0)
    }
}

/// Completed logical payload coupled to its transport storage.
#[derive(Debug)]
pub struct Body<S> {
    source: S,
    len: usize,
}

impl<S> Body<S> {
    pub fn try_new<'d>(
        proof: admit::Track<admit::Finished>,
        source: S,
    ) -> Result<Self, Rejected<(admit::Track<admit::Finished>, S), Error>>
    where
        S: body::Source<'d>,
    {
        let len = proof.len();
        let seen = source.body().into().len();
        let valid = match proof.retention() {
            admit::Retention::Discarded => seen == 0,
            admit::Retention::Retained => seen == len,
        };
        if !valid {
            return Err(Rejected::new((proof, source), Error::SegmentMismatch));
        }
        Ok(Self { source, len })
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn into_source(self) -> S {
        self.source
    }

    pub fn input(&mut self) -> Input<Borrowed<'_, S>> {
        Input {
            source: Borrowed::new(&mut self.source),
            len: self.len,
        }
    }
}

/// One consumed projection choice retaining the completed logical body length.
pub struct Input<I> {
    pub(crate) source: I,
    pub(crate) len: usize,
}

impl<'d> Body<owned::Segments<'d>> {
    pub fn into_body(self) -> owned::Segments<'d> {
        self.source
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (owned::Segments<'d>, usize) {
        (self.source, self.len)
    }
}

const _: () = assert!(core::mem::size_of::<Len>() == core::mem::size_of::<usize>());

/// Linear successor of one cooperative construction turn.
pub enum Step<B, O, E> {
    NeedInput {
        build: B,
        released: credit::Amount,
    },
    Pending {
        build: B,
        released: credit::Amount,
    },
    Ready {
        output: O,
        released: credit::Amount,
    },
    Failed {
        build: B,
        error: E,
        released: credit::Amount,
    },
}

impl<B, O, E> Step<B, O, E> {
    pub fn map<C, T, F>(
        self,
        build: impl FnOnce(B) -> C,
        output: impl FnOnce(O) -> T,
        error: impl FnOnce(E) -> F,
    ) -> Step<C, T, F> {
        match self {
            Self::NeedInput {
                build: next,
                released,
            } => Step::NeedInput {
                build: build(next),
                released,
            },
            Self::Pending {
                build: next,
                released,
            } => Step::Pending {
                build: build(next),
                released,
            },
            Self::Ready {
                output: value,
                released,
            } => Step::Ready {
                output: output(value),
                released,
            },
            Self::Failed {
                build: next,
                error: cause,
                released,
            } => Step::Failed {
                build: build(next),
                error: error(cause),
                released,
            },
        }
    }
}

/// Result of one scheduled cooperative build turn.
pub enum BuildOutcome<K, B, E> {
    Idle,
    Pending,
    Ready { key: K, build: B },
    Failed { key: K, error: E },
}

/// Builder ownership after one retained segment transfer attempt.
pub enum Transfer<B, E> {
    Accepted(B),
    Rejected { build: B, error: E },
}

impl<B, E> Transfer<B, E> {
    /// Moves the successor builder while preserving the exact failure.
    pub fn map<C>(self, map: impl FnOnce(B) -> C) -> Transfer<C, E> {
        match self {
            Self::Accepted(build) => Transfer::Accepted(map(build)),
            Self::Rejected { build, error } => Transfer::Rejected {
                build: map(build),
                error,
            },
        }
    }

    pub fn map_err<F>(self, map: impl FnOnce(E) -> F) -> Transfer<B, F> {
        match self {
            Self::Accepted(build) => Transfer::Accepted(build),
            Self::Rejected { build, error } => Transfer::Rejected {
                build,
                error: map(error),
            },
        }
    }
}

/// Route-erased cooperative request builder.
pub trait Build<'d>: Sized {
    type Output;
    type StepError: fmt::Debug;

    fn finish_input(self, len: usize) -> Self;

    fn step(self, budget: NonZero<usize>) -> Step<Self, Self::Output, Self::StepError>;
}

/// Capability to accept retained logical payload segments.
pub trait Push<'d>: Build<'d> {
    type Error;

    /// Borrows the exact local failure without consuming retained input.
    fn observe<C: Copy, S: Copy, O: crate::life::Observer<C, S>>(
        error: &Self::Error,
        observer: &mut O,
    );

    fn push(self, part: owned::Segment<'d>) -> Transfer<Self, Self::Error>;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Error {
    TooLarge { actual: usize, limit: usize },
    LengthMismatch { actual: usize, declared: usize },
    Overflow { current: usize, added: usize },
    InputIncomplete,
    InputFinished,
    SegmentMismatch,
    Credit(credit::Error),
}

const _: () = {
    assert!(core::mem::size_of::<Error>() == 3 * core::mem::size_of::<usize>());
    assert!(core::mem::size_of::<Result<(), Error>>() == 3 * core::mem::size_of::<usize>());
};
