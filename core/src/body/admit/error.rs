use o3::mem::Rejected;

use crate::{body, body::admit};

#[derive(Debug)]
pub enum Error<S> {
    Admission(Rejected<S, body::Error>),
    Storage(Rejected<(admit::Track<admit::Finished>, S), body::Error>),
}
