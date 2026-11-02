use std::{borrow::Borrow, marker::PhantomData};

use o3::buffer::bytes::scope;

use crate::{
    owner::handoff::{Origin, Project, Slot, origin, stage},
    payload::owned,
    request::body::{self, Source as _},
};

/// A projection from a borrowed source; it cannot defer ownership transfer.
pub struct Borrowed<'a, S>(&'a mut S);

impl<'a, S> Borrowed<'a, S> {
    pub const fn new(source: &'a mut S) -> Self {
        Self(source)
    }
}

impl<'a, 'd, S: body::Source<'d>> Project<'d> for Borrowed<'a, S> {
    type Proof = ();
    type Owner = ();

    fn contiguous<V, T>(
        self,
        value: V,
        project: impl for<'req> FnOnce(
            V,
            body::View<'req, 'd>,
            Slot<'req, 'd, (), Bound<'req, 'd, ()>>,
        ) -> T,
    ) -> Result<(T, ()), (Self, V, body::Error)> {
        match self.0.try_project(value, |value, view| {
            project(value, view, Bound::slot(view.into(), ()))
        }) {
            Ok(output) => Ok((output, ())),
            Err((value, error)) => Err((self, value, error)),
        }
    }

    fn segmented<V, T>(
        self,
        value: V,
        project: impl for<'req> FnOnce(
            V,
            body::Ref<'req, 'd>,
            Slot<'req, 'd, (), Bound<'req, 'd, ()>>,
        ) -> T,
    ) -> (T, ()) {
        let view = self.0.body().into();
        (project(value, view, Bound::slot(view, ())), ())
    }

    fn discarded<V, T>(
        self,
        value: V,
        project: impl for<'req> FnOnce(V, Slot<'req, 'd, (), Bound<'req, 'd, ()>>) -> T,
    ) -> (T, ()) {
        (
            project(value, Bound::slot(body::Ref::from_slice(&[]), ())),
            (),
        )
    }
}

/// The original source, before its one selected projection.
pub struct Input<'owner, 'd>(scope::Owner<'owner, ()>, owned::Segments<'d>);

/// The exact final source after normalization and response selection.
pub struct Frozen<'owner, 'd>(Input<'owner, 'd>);

/// A selected source whose short borrow can end without losing owner identity.
#[derive(Clone, Copy)]
pub struct Bound<'req, 'd, P> {
    body: body::Retained<'req, 'd>,
    proof: P,
}

/// An ownership selection with no borrow of the source storage.
#[derive(Clone, Copy)]
pub struct Selected<'owner>(PhantomData<*mut &'owner ()>);

impl<'d> Input<'_, 'd> {
    pub fn with<T>(
        source: owned::Segments<'d>,
        run: impl for<'owner> FnOnce(Input<'owner, 'd>) -> T,
    ) -> T {
        scope::Owner::with((), |brand| run(Input(brand, source)))
    }
}

impl<'req, 'd, P: stage::Proof> Bound<'req, 'd, P> {
    fn slot(view: body::Ref<'req, 'd>, proof: P) -> Slot<'req, 'd, (), Self> {
        let source = match view.try_retained() {
            Ok(body) => origin::Source::Retained(Self { body, proof }),
            Err(body) if body.is_empty() => origin::Source::Empty,
            Err(_) => origin::Source::Borrowed(PhantomData),
        };
        Slot {
            origin: Origin { source },
            head: (),
        }
    }
}

impl<'owner, 'd> Project<'d> for Input<'owner, 'd> {
    type Proof = Selected<'owner>;
    type Owner = Frozen<'owner, 'd>;

    fn contiguous<V, T>(
        mut self,
        value: V,
        project: impl for<'req> FnOnce(
            V,
            body::View<'req, 'd>,
            Slot<'req, 'd, (), Bound<'req, 'd, Selected<'owner>>>,
        ) -> T,
    ) -> Result<(T, Self::Owner), (Self, V, body::Error)> {
        let output = self.1.try_project(value, |value, view| {
            project(value, view, Bound::slot(view.into(), Selected(PhantomData)))
        });
        match output {
            Ok(output) => Ok((output, Frozen(self))),
            Err((value, error)) => Err((self, value, error)),
        }
    }

    fn segmented<V, T>(
        self,
        value: V,
        project: impl for<'req> FnOnce(
            V,
            body::Ref<'req, 'd>,
            Slot<'req, 'd, (), Bound<'req, 'd, Selected<'owner>>>,
        ) -> T,
    ) -> (T, Self::Owner) {
        let view = body::Ref::from(&self.1);
        let output = project(value, view, Bound::slot(view, Selected(PhantomData)));
        (output, Frozen(self))
    }

    fn discarded<V, T>(
        self,
        value: V,
        project: impl for<'req> FnOnce(V, Slot<'req, 'd, (), Bound<'req, 'd, Selected<'owner>>>) -> T,
    ) -> (T, Self::Owner) {
        let output = project(
            value,
            Bound::slot(body::Ref::from_slice(&[]), Selected(PhantomData)),
        );
        (output, Frozen(self))
    }
}

impl<'req, 'd, P> Borrow<body::Retained<'req, 'd>> for Bound<'req, 'd, P> {
    fn borrow(&self) -> &body::Retained<'req, 'd> {
        &self.body
    }
}

impl<'req, 'd: 'req, P: stage::Proof> stage::Body<'req, 'd> for Bound<'req, 'd, P> {
    type Deferred = P::Deferred;

    fn defer(self) -> Self::Deferred {
        self.proof.defer(self.body)
    }
}

impl stage::Proof for Selected<'_> {
    type Deferred = Self;

    fn defer(self, _: body::Retained<'_, '_>) -> Self {
        self
    }
}

impl<'owner, 'd> stage::Deferred<'d> for Selected<'owner> {
    type Owner = Frozen<'owner, 'd>;

    fn resolve(self, owner: Self::Owner) -> owned::Segments<'d> {
        let Input(brand, body) = owner.0;
        brand.into_inner();
        body
    }
}

const _: () = {
    assert!(size_of::<Input<'static, 'static>>() == size_of::<owned::Segments<'static>>());
    assert!(size_of::<Frozen<'static, 'static>>() == size_of::<owned::Segments<'static>>());
    assert!(
        size_of::<Bound<'static, 'static, Selected<'static>>>()
            == size_of::<body::Retained<'static, 'static>>()
    );
    assert!(size_of::<Selected<'static>>() == 0);
};
