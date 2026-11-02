use dope_test::harness::{observe::Trace, signals::Gate};
use sark_ws::client::{self, Message, send::Sender};

pub enum Event {
    Opened,
    Message,
    Closed(client::CloseReason),
    Dropped,
}

pub struct Borrowed<'a> {
    pub events: &'a Trace<Event, 4>,
    pub closed: &'a Gate,
    pub message: &'a dyn Fn(Message<'_>),
}

impl<'d> client::Egress<'d> for Borrowed<'_> {
    fn open(&mut self, _: &mut Sender<'_, '_, '_, 'd>) {
        self.events.record(Event::Opened);
    }

    fn message(&mut self, message: Message<'_>) {
        (self.message)(message);
        self.events.record(Event::Message);
    }

    fn close(&mut self, reason: client::CloseReason) {
        self.events.record(Event::Closed(reason));
        self.closed.hit();
    }
}

impl Drop for Borrowed<'_> {
    fn drop(&mut self) {
        self.events.record(Event::Dropped);
    }
}
