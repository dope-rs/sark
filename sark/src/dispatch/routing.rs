use std::marker::PhantomData;
use std::pin::Pin;

use super::conn_state::{ConnState, ConsumeOutcome, DispatchPermit};
use crate::date::Stamp;
use crate::timer::{Timer, TimerHost};

pub trait Routing<'d> {
    fn try_consume(
        self: Pin<&mut Self>,
        permit: DispatchPermit,
        bytes: &[u8],
        write: &mut [u8],
        conn: &mut ConnState,
    ) -> ConsumeOutcome;
}

pub trait RouteCore<'d> {
    fn timer(&self) -> &Timer<'d>;

    fn try_consume(
        self: Pin<&mut Self>,
        date: &Stamp,
        permit: DispatchPermit,
        bytes: &[u8],
        write: &mut [u8],
        conn: &mut ConnState,
    ) -> ConsumeOutcome;
}

pub struct H1Host<'a, 'd, C> {
    core: Pin<&'a mut C>,
    date: &'a Stamp,
    driver: PhantomData<fn(&'d ()) -> &'d ()>,
}

impl<'a, 'd, C> H1Host<'a, 'd, C> {
    pub fn new(core: Pin<&'a mut C>, date: &'a Stamp) -> Self {
        Self {
            core,
            date,
            driver: PhantomData,
        }
    }
}

impl<'d, C: RouteCore<'d>> TimerHost<'d> for H1Host<'_, 'd, C> {
    fn timer(&self) -> &Timer<'d> {
        self.core.as_ref().get_ref().timer()
    }
}

impl<'d, C: RouteCore<'d>> Routing<'d> for H1Host<'_, 'd, C> {
    fn try_consume(
        self: Pin<&mut Self>,
        permit: DispatchPermit,
        bytes: &[u8],
        write: &mut [u8],
        conn: &mut ConnState,
    ) -> ConsumeOutcome {
        let this = self.get_mut();
        this.core
            .as_mut()
            .try_consume(this.date, permit, bytes, write, conn)
    }
}
