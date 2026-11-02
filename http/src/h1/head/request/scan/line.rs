use crate::h1::head::request::scan::State;

/// Progress to a request-line terminator; header semantics remain unparsed.
pub enum Progress<S> {
    More(State<S>),
    Complete(S),
}
