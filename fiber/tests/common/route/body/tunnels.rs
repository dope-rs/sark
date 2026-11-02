use dope::{
    core::driver::{route::Identity, schedule::clock::timer::Timer},
    manifold::network::listener::conn,
    net::wire::{open, terminal},
};
use dope_tls::tls::Tls;
use o3::buffer::resident;
use sark_core::duplex;
use sark_h1::server::listener::failure::Policy;
use sark_http::payload::{owned, response::status::Code};

use crate::common::{retained, server};

pub struct State<'a> {
    pub counters: &'a Duplex,
    pub finish_output_on_input: bool,
    pub hold_output_on_finish: bool,
}

pub struct Echo<'state> {
    counters: &'state Duplex,
    finish_output_on_input: bool,
    hold_output_on_finish: bool,
    output_finished: bool,
}

impl<'d> duplex::Session<'d> for Echo<'_> {
    fn receive(&mut self, input: owned::Segments<'d>) -> duplex::Action<'d> {
        if self.output_finished || self.counters.input_finished.get() != 0 {
            self.counters.receive(input.len());
            return duplex::Action::idle();
        }
        if self.finish_output_on_input {
            self.output_finished = true;
            return duplex::Action::send_end(input);
        }
        duplex::Action::send(input)
    }

    fn finish(&mut self) -> duplex::Action<'d> {
        self.counters.finish();
        if self.hold_output_on_finish {
            duplex::Action::send(resident::Shared::from(b"finished".as_slice()))
        } else if self.output_finished {
            duplex::Action::close()
        } else {
            duplex::Action::send_end(resident::Shared::from(b"finished".as_slice()))
        }
    }

    fn activate(&mut self) -> duplex::Action<'d> {
        self.counters.activate();
        duplex::Action::send(resident::Shared::from(b"ready".as_slice()))
    }
}

#[sark_fiber::request]
pub struct Connect {}

#[sark_fiber::duplex(pub)]
fn echo<'state>(_: Connect, state: &'state State<'_>) -> Echo<'state> {
    Echo {
        counters: state.counters,
        finish_output_on_input: state.finish_output_on_input,
        hold_output_on_finish: state.hold_output_on_finish,
        output_finished: false,
    }
}

#[sark_fiber::handler(pub)]
async fn wait<'state>(state: &State<'state>, timer: Timer<'_>) -> super::OwnedReply {
    retained::State(&state.counters.timer_failures)
        .delay(timer, Duration::from_millis(50))
        .await;
    super::OwnedReply {
        status: Code::OK,
        body: b"waited".to_vec(),
    }
}

sark_fiber::define_route! {
    pub ConnectDispatch<'state>: State<'state> => {
        CONNECT "*" => echo,
        GET "/wait" => async(capacity = 1, body = discarded) wait,
    }
}

use std::{
    cell::Cell,
    io::{self, Read},
    time::Duration,
};

use dope_test::harness::{peer::Stream, signals::LocalGate};

#[derive(Default)]
pub struct Duplex {
    capability: Cell<usize>,
    input_finished: Cell<usize>,
    after_finish: Cell<usize>,
    pub closed: LocalGate,
    pub failures: Cell<Vec<server::Failure>>,
    pub timer_failures: Cell<Vec<retained::Failure>>,
}

#[derive(Debug)]
pub struct Snapshot {
    pub capability: usize,
    pub input_finished: usize,
    pub after_finish: usize,
}

impl Duplex {
    pub fn activate(&self) {
        self.capability.set(self.capability.get() + 1);
    }

    pub fn finish(&self) {
        self.input_finished.set(self.input_finished.get() + 1);
    }

    pub fn receive(&self, bytes: usize) {
        self.after_finish.set(self.after_finish.get() + bytes);
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            capability: self.capability.get(),
            input_finished: self.input_finished.get(),
            after_finish: self.after_finish.get(),
        }
    }
}

impl Drop for Echo<'_> {
    fn drop(&mut self) {
        self.counters.closed.handle().hit();
    }
}

impl Policy<Tls> for State<'_> {
    fn open_failed<'w>(&self, cause: open::Error<'w, Tls>)
    where
        Tls: 'w,
    {
        Policy::open_failed(&server::State(&self.counters.failures), cause);
    }

    fn wire_failed<'w, 'd: 'w, ID: Identity>(
        &self,
        id: conn::Id<'d, ID>,
        cause: terminal::Error<'w, Tls>,
    ) {
        Policy::wire_failed(&server::State(&self.counters.failures), id, cause);
    }
}

pub trait Receive {
    fn read_until(&self, buffered: &mut Vec<u8>, expected: &[u8]) -> io::Result<()>;
}

impl Receive for Stream<'_> {
    fn read_until(&self, buffered: &mut Vec<u8>, expected: &[u8]) -> io::Result<()> {
        let mut socket = self;
        let mut incoming = [0_u8; 4096];
        while !buffered
            .windows(expected.len())
            .any(|window| window == expected)
        {
            let received = socket.read(&mut incoming)?;
            if received == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            buffered.extend_from_slice(&incoming[..received]);
        }
        Ok(())
    }
}
