use crate::body::admit;

/// Initial request payload state after all head-known bounds were checked.
#[must_use = "an admitted request payload must be installed or rejected"]
pub enum Start {
    Open(admit::Track<admit::Open>),
    Finished(admit::Track<admit::Finished>),
}
