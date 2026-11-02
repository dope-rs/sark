use crate::body::admit;

#[derive(Debug)]
pub(super) enum Kind {
    Exact,
    Limit,
}

#[derive(Debug)]
pub(super) struct Bound<P> {
    pub(super) kind: Kind,
    pub(super) value: usize,
    pub(super) retention: admit::Retention,
    pub(super) stream: admit::Stream,
    pub(super) phase: P,
}

impl<P> Bound<P> {
    pub(super) fn map<Q>(self, map: impl FnOnce(P) -> Q) -> Bound<Q> {
        Bound {
            kind: self.kind,
            value: self.value,
            retention: self.retention,
            stream: self.stream,
            phase: map(self.phase),
        }
    }
}
