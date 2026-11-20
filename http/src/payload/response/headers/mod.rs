//! Admitted application response headers and their emission contract.

pub mod head;
mod list;
pub mod set;
pub mod value;

pub use list::List;

use crate::field;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Managed,
    Syntax(field::Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Application {}

pub type Name<T> = field::storage::Lower<T, Application>;
pub type Entry<'a> = field::Entry<'a, Application>;
pub type Group<'a> = field::Group<'a, Application>;
