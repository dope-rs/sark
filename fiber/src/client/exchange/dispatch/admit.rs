use crate::client::exchange::{dispatch::Prepared, request::Encoded};

pub(in crate::client) enum Admit<S, D, R> {
    Stream(S),
    Dial(D, Encoded),
    Wait(Prepared<R>),
}
