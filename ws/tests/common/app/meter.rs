use std::{cell, rc, slice};

use dope_test::alloc::Tracker;
use sark_ws::client::{self, headers};

pub struct Usage {
    pub count: usize,
    pub len: usize,
    pub opened: bool,
    pub messages: [usize; 4],
    pub close: Option<client::CloseReason>,
}

pub struct Meter<'a> {
    pub proof: rc::Rc<cell::Cell<Option<Usage>>>,
    pub auth: headers::Field<&'a str, String>,
}

impl client::Handshake for Meter<'_> {
    fn handshake(&mut self, req: headers::Request<'_>) -> Result<headers::Encoded, client::Error> {
        let added = slice::from_ref(&self.auth);
        let (head, count) = Tracker::count(|| req.finish_with(added));
        let head = head?;
        self.proof.set(Some(Usage {
            count,
            len: head.len(),
            opened: false,
            messages: [0; 4],
            close: None,
        }));
        Ok(head)
    }
}

impl client::Observer for Meter<'_> {
    fn open(&mut self) {
        if let Some(mut proof) = self.proof.take() {
            proof.opened = true;
            self.proof.set(Some(proof));
        }
    }

    fn message(&mut self, msg: client::Message<'_>) {
        let index = match msg {
            client::Message::Text(_) => 0,
            client::Message::Binary(_) => 1,
            client::Message::Ping(_) => 2,
            client::Message::Pong(_) => 3,
        };
        if let Some(mut proof) = self.proof.take() {
            proof.messages[index] += 1;
            self.proof.set(Some(proof));
        }
    }

    fn close(&mut self, reason: client::CloseReason) {
        if let Some(mut proof) = self.proof.take() {
            proof.close = Some(reason);
            self.proof.set(Some(proof));
        }
    }
}
