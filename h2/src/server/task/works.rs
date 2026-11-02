use sark_core::decode::request;

#[derive(dope::WorkGraph)]
pub enum Plain {
    Build,
    Task,
    Ingress,
    Egress,
}

#[derive(dope::WorkGraph)]
pub enum Guarded {
    Admission,
    Build,
    Task,
    Ingress,
    Egress,
}

pub trait Family {
    type Graph: Default;
}

#[repr(transparent)]
pub(in crate::server::task) struct State<S: Family> {
    pub(in crate::server::task) graph: S::Graph,
}

impl<S: Family> State<S> {
    pub(in crate::server::task) fn new() -> Self {
        Self {
            graph: S::Graph::default(),
        }
    }
}

impl<U> Family for request::Plain<U> {
    type Graph = PlainGraph;
}

impl<G> Family for request::Guarded<G> {
    type Graph = GuardedGraph;
}

impl<U, G> Family for request::Start<U, G> {
    type Graph = GuardedGraph;
}

const _: () = assert!(std::mem::size_of::<State<request::Plain<()>>>() == 1);
const _: () = assert!(std::mem::size_of::<State<request::Guarded<()>>>() == 1);
