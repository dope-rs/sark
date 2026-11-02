use crate::client::config::report::{Never, Retain};

pub trait Guard {}

impl Guard for Retain {}

impl Guard for Never {}
