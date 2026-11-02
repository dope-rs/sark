use o3::buffer::hold;

use crate::conn::output;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Charge(output::Overflow),
    Retention(hold::Overflow),
}
