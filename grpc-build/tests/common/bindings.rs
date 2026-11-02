use std::{cell::Cell, vec};

use dope_test::harness::observe::VecCellExt as _;
use sark_grpc::{
    encode::Prost,
    metadata,
    o3::buffer,
    server::service::{
        handler::{modes, output},
        timers,
    },
};

use output::Poll::Pending as service;

pub struct Service<'a> {
    pub calls: &'a Cell<Vec<u8>>,
}

impl ProofService for Service<'_> {
    fn alpha(&mut self, request: modes::Request<()>) -> modes::Response<()> {
        let () = request.body;
        self.calls.push(0);
        modes::Response::new(())
    }

    fn alpine(&mut self, request: modes::Request<()>) -> modes::Response<()> {
        let () = request.body;
        self.calls.push(1);
        modes::Response::new(())
    }

    fn beta(&mut self, request: modes::Request<()>) -> modes::Response<()> {
        let () = request.body;
        self.calls.push(2);
        modes::Response::new(())
    }

    fn r#type(&mut self, request: modes::Request<()>) -> modes::Response<()> {
        let () = request.body;
        self.calls.push(3);
        modes::Response::new(())
    }

    fn call(&mut self, request: modes::Request<()>) -> modes::Response<()> {
        let () = request.body;
        self.calls.push(4);
        modes::Response::new(())
    }

    fn unknown(&mut self, request: modes::Request<()>) -> modes::Response<()> {
        let () = request.body;
        self.calls.push(5);
        modes::Response::new(())
    }
}

pub struct Empty;

impl EmptyService for Empty {}

pub struct Shapes<'a> {
    pub calls: &'a Cell<usize>,
    pub paths: &'a Cell<Vec<Vec<u8>>>,
}

impl Shapes<'_> {
    fn hit(&self) {
        self.calls.set(self.calls.get() + 1);
    }
}

pub struct Chat {
    received: usize,
    closed: bool,
}

impl output::Bidi<Prost<(), ()>> for Chat {
    fn message<'a, S: buffer::raw::Segment + 'a>(&mut self, message: ()) {
        let () = message;
        self.received += 1;
    }

    fn trailers(&mut self, _: metadata::Decoded) {
        self.closed = true;
    }
}

impl output::Stream for Chat {
    type Response = ();
    type Timer = timers::Off;

    fn poll(&mut self, _: &mut timers::Output<'_, '_, '_, Self::Timer>) -> output::Poll<()> {
        match self.received {
            0 if self.closed => output::Poll::Complete,
            0 => service,
            _ => {
                self.received -= 1;
                output::Poll::Message(())
            }
        }
    }
}

impl _123Service for Shapes<'_> {
    type _3Stream = vec::IntoIter<()>;
    type _4Stream = Chat;

    fn _1(&mut self, request: modes::Request<()>) -> modes::Response<()> {
        let () = request.body;
        self.hit();
        modes::Response::new(())
    }

    fn _2<P: buffer::raw::Segment>(
        &mut self,
        request: modes::Request<modes::Messages<'_, '_, Prost<(), ()>, P>>,
    ) -> modes::Response<()> {
        for _ in request.body {
            self.calls.set(self.calls.get() + 1);
        }
        modes::Response::new(())
    }

    fn _3(&mut self, request: modes::Request<()>) -> output::Response<Self::_3Stream> {
        let () = request.body;
        self.hit();
        output::Response::new(vec![()].into_iter())
    }

    fn _4(&mut self, request: output::BidiRequest<'_>) -> output::Response<Self::_4Stream> {
        self.paths.push(request.head.path().to_vec());
        self.hit();
        output::Response::new(Chat {
            received: 0,
            closed: false,
        })
    }
}
