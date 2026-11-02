use std::rc::Rc;

use dope_test::harness::{observe::Trace, signals};
use sark_ws::client::{self, Message, send::Sender};
pub const SATURATING_PAYLOAD: usize = 4 * 1024 * 1024;
pub const FRAME_PAYLOAD: usize = SATURATING_PAYLOAD / 2;
pub const FRAGMENTS: [u8; 377] = [0x5a; 377];

pub struct Saturating<'a> {
    pub ping: signals::Gate,
    pub close: signals::Gate,
    pub reason: Rc<Trace<client::CloseReason, 1>>,
    pub open: &'a dyn Fn(&mut Sender<'_, '_, '_, '_>),
    pub message: &'a dyn Fn(Message<'_>),
}

impl<'d> client::Egress<'d> for Saturating<'_> {
    fn open(&mut self, send: &mut Sender<'_, '_, '_, 'd>) {
        (self.open)(send);
    }

    fn message(&mut self, msg: Message<'_>) {
        (self.message)(msg);
        if let Message::Ping(_) = msg {
            self.ping.hit();
        }
    }

    fn close(&mut self, reason: client::CloseReason) {
        self.reason.record(reason);
        self.close.hit();
    }
}
