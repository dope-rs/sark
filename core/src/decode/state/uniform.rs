use core::ops::ControlFlow;

use crate::{
    body::admit::{Finished, Open, Track},
    decode::{
        exec, request,
        state::{
            Admission, Admitted, Each, Finish, Flow, Fold, Map, Set, Slot, Uniforms, Whole, admit,
            body, open,
        },
    },
};

/// One-route representation with no route discriminant.
pub struct Uniform<P, F: Set> {
    slot: Whole<P>,
    body: request::Body<F::SyncOpen, F::SyncFinished>,
}

impl<P, F: Set> Uniform<P, F> {
    pub const fn new(prep: P, body: request::Body<F::SyncOpen, F::SyncFinished>) -> Self {
        Self {
            slot: Whole::new(prep),
            body,
        }
    }

    pub(super) fn into_parts(self) -> (P, request::Body<F::SyncOpen, F::SyncFinished>) {
        (self.slot.0, self.body)
    }
}

impl<P> Uniform<P, Admitted> {
    pub fn into_request(self) -> AdmittedBody<P> {
        match self.body {
            request::Body::Open(body) => request::Body::Open(self.slot.admit(body)),
            request::Body::Finished(body) => request::Body::Finished(self.slot.admit(body)),
        }
    }
}

impl<P, F: Set> Flow<P, F> for Uniform<P, F> {
    type Family = Uniforms<P>;

    fn each<'a, M: Each<P, F>>(&'a mut self, each: M) -> M::Output<'a> {
        match &mut self.body {
            request::Body::Open(body) => each.sync_open(&self.slot, body),
            request::Body::Finished(body) => each.sync_done(&self.slot, body),
        }
    }

    fn map_retain<G: Set, M: Map<P, F, G>>(
        self,
        map: M,
    ) -> ControlFlow<M::Break, (M::Retained, Uniform<P, G>)> {
        let (retained, slot, body) = match self.body {
            request::Body::Open(body) => {
                let (retained, slot, body) = map.sync_open(self.slot, body)?;
                (retained, slot, request::Body::Open(body))
            }
            request::Body::Finished(body) => {
                let (retained, slot, body) = map.sync_done(self.slot, body)?;
                (retained, slot, request::Body::Finished(body))
            }
        };
        ControlFlow::Continue((retained, Uniform { slot, body }))
    }

    fn fold<T: Fold<P, F>>(self, fold: T) -> T::Output {
        match self.body {
            request::Body::Open(body) => fold.sync_open(self.slot, body),
            request::Body::Finished(body) => fold.sync_done(self.slot, body),
        }
    }

    fn finish(self) -> Result<Self, Self>
    where
        F: Finish,
    {
        let request::Body::Open(body) = self.body else {
            return Err(self);
        };
        Ok(Self {
            slot: self.slot,
            body: request::Body::Finished(F::sync(body)),
        })
    }

    fn each_open<'a, V: open::Each<F>>(&'a mut self, visit: V) -> V::Output<'a>
    where
        F: open::Set,
    {
        match &mut self.body {
            request::Body::Open(body) => visit.apply(body),
            request::Body::Finished(never) => match *never {},
        }
    }

    fn fold_open<V: open::Fold<P, F>>(self, visit: V) -> V::Output
    where
        F: open::Set,
    {
        match self.body {
            request::Body::Open(body) => visit.sync(self.slot, body),
            request::Body::Finished(never) => match never {},
        }
    }

    fn consume<O, V: FnOnce(F::Body) -> O>(self, visit: V) -> O
    where
        F: open::Set,
    {
        match self.body {
            request::Body::Open(body) => visit(body),
            request::Body::Finished(never) => match never {},
        }
    }
}

impl<P, F: Set> body::Flow<F> for Uniform<P, F> {
    fn fold<T: body::Fold<F>>(self, fold: T) -> T::Output {
        match self.body {
            request::Body::Open(body) => fold.apply(body::Value::SyncOpen(body)),
            request::Body::Finished(body) => fold.apply(body::Value::SyncFinished(body)),
        }
    }
}

impl<P, F, G> Admission<P, F, G> for Uniform<P, F>
where
    F: Set,
    G: Set,
{
    type Output = Uniform<P, G>;

    fn admit<V: admit::Visit<P, F, G>>(self, visit: &mut V) -> ControlFlow<V::Break, Self::Output> {
        let (slot, body) = match self.body {
            request::Body::Open(body) => {
                let (slot, body) = visit.sync_open(self.slot, body)?;
                (slot, request::Body::Open(body))
            }
            request::Body::Finished(body) => {
                let (slot, body) = visit.sync_done(self.slot, body)?;
                (slot, request::Body::Finished(body))
            }
        };
        ControlFlow::Continue(Uniform { slot, body })
    }
}

type AdmittedBody<P> = request::Body<
    request::Admitted<exec::Sync, P, Track<Open>>,
    request::Admitted<exec::Sync, P, Track<Finished>>,
>;
