use dope_test::harness::signals::Gate;
use sark_ws::client::{self, send::Sender};

pub struct Closing<'a> {
    pub open: &'a dyn Fn(&mut Sender<'_, '_, '_, '_>),
    pub close: &'a dyn Fn(client::CloseReason),
    pub opened: &'a Gate,
    pub closed: &'a Gate,
    pub messages: &'a Gate,
}

impl<'d> client::Egress<'d> for Closing<'_> {
    fn open(&mut self, send: &mut Sender<'_, '_, '_, 'd>) {
        (self.open)(send);
        self.opened.hit();
    }

    fn message(&mut self, _: client::Message<'_>) {
        self.messages.hit();
    }

    fn close(&mut self, reason: client::CloseReason) {
        (self.close)(reason);
        self.closed.hit();
    }
}
