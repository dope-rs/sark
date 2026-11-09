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

impl<U> Family for request::Plain<U> {
    type Graph = PlainGraph;
}

impl<G> Family for request::Guarded<G> {
    type Graph = GuardedGraph;
}

impl<U, G> Family for request::Start<U, G> {
    type Graph = GuardedGraph;
}

const _: () = assert!(std::mem::size_of::<PlainGraph>() == 1);
const _: () = assert!(std::mem::size_of::<GuardedGraph>() == 1);
