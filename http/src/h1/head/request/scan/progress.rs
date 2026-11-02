use crate::h1::head::request::{Parts, scan::State};

pub enum Progress<S> {
    More(State<S>),
    Complete(Parts<S>),
}
