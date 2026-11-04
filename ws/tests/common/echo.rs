//! Echo over the real server event loop, exercising the zero-copy
//! recv fast path (`ChunkSource`): batched, large, fragmented and split writes.

use std::{cell::Cell, net::SocketAddr, ops};

pub const OPENING: &[u8] = b"GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";

use dope::{
    core::{
        driver::{
            life::recovery,
            route::{FrameworkTag, Tag, kind},
        },
        io::socket::Backlog,
    },
    manifold::app::env::quota,
    runtime::{executor::session::Completion, topology::ops::Run},
};
use dope_test::{
    alloc::{Activity, Tracker},
    harness::server::Pair,
    reclaim,
};
use sark_ws::{
    frame,
    server::{Config, Error, Message, Response, app::Sink},
};

use crate::common::server::{self, Build};

pub struct Echo<'a> {
    pub calls: &'a Cell<usize>,
    pub repeat: Option<&'a [u8]>,
    pub activity: &'a dyn Fn(Activity),
    pub failed: &'a dyn Fn(Error),
}

impl<'a> Echo<'a> {
    pub fn config(bind: SocketAddr) -> Config {
        Config {
            bind,
            max_connections: 16,
            per_ip: Some(quota::PerIp::fixed::<8>()),
            backlog: Backlog::fixed::<16>(),
            path: "/".into(),
            max_frame_payload: 16 * 1024 * 1024,
            max_message_size: 16 * 1024 * 1024,
        }
    }
}

impl Sink for Echo<'_> {
    fn capacity(&self, payload: usize) -> Result<usize, Error> {
        let length = self.repeat.map_or(payload, <[u8]>::len);
        Ok(frame::Plan::new(0x2, true, length, false)?.wire_len())
    }

    fn message(
        &self,
        msg: Message<'_>,
        resp: &mut Response<'_>,
    ) -> Result<ops::ControlFlow<()>, Error> {
        self.calls.set(self.calls.get() + 1);
        let (result, activity) = Tracker::activity(|| match (self.repeat, msg) {
            (Some(bytes), _) => resp.try_binary(bytes),
            (None, Message::Text(text)) => resp.try_text(text),
            (None, Message::Binary(bytes)) => resp.try_binary(bytes),
        });
        (self.activity)(activity);
        result?;
        Ok(ops::ControlFlow::Continue(()))
    }

    fn failed(&self, error: Error) {
        (self.failed)(error);
    }
}

pub struct Closing<'a> {
    pub echo: Echo<'a>,
}

impl Sink for Closing<'_> {
    fn capacity(&self, payload: usize) -> Result<usize, Error> {
        self.echo.capacity(payload)
    }

    fn message(
        &self,
        msg: Message<'_>,
        resp: &mut Response<'_>,
    ) -> Result<ops::ControlFlow<()>, Error> {
        self.echo
            .message(msg, resp)
            .map(|_| ops::ControlFlow::Break(()))
    }

    fn failed(&self, error: Error) {
        self.echo.failed(error);
    }
}

pub fn run_echo(
    expected: usize,
    repeat: Option<&[u8]>,
    message_limit: Option<usize>,
    client: impl FnOnce(SocketAddr),
) {
    Pair::bind()
        .expect("harness")
        .check(
            move |ctx, bound| {
                let calls = Cell::new(0);
                let activity = |usage| assert_eq!(usage, Activity::default());
                let sink = Echo {
                    calls: &calls,
                    repeat,
                    activity: &activity,
                    failed: &|error| {
                        assert!(
                            matches!(&error, Error::Open(_))
                                || matches!(&error,
                        Error::Frame(cause) if matches!(cause.close_code(), 1002 | 1007 | 1009)),
                            "WebSocket echo: {error:?}"
                        );
                    },
                };
                let mut config = Echo::config(bound.local_addr());
                if let Some(limit) = message_limit {
                    config.max_message_size = limit;
                }
                let server = server::try_new(config, sink, bound)?;
                let mut reports = reclaim::Reports::new(
                    server.layout(),
                    [<FrameworkTag<kind::Any> as Tag>::ROUTE],
                );
                server.enter(
                    ctx,
                    Run(&mut Completion::new()),
                    recovery::Policy::new(|event| reports.push(event), crate::common::recover),
                    |result| assert!(matches!(result, Ok(Ok(_))), "echo server shutdown"),
                );
                let events = reports.events().expect("valid reclamation");
                assert!(events.is_empty(), "{reports:?}");
                assert_eq!(calls.get(), expected);
                Ok::<_, Build>(())
            },
            client,
            |result| result.expect("WebSocket server layout"),
        )
        .expect("harness")
}
