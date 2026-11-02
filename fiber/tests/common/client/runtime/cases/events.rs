use std::{convert::Infallible, net::SocketAddr};

use dope::core::driver::life::recovery;
use dope_fiber::BlockOn as _;
use sark_client::protocol::response::Event;
use sark_fiber::client::config::Config;

use crate::common::client::runtime::{
    self,
    cases::{
        request::{Error, Step},
        stream::Exchange,
    },
    pools,
};

pub struct Capture {
    pub status: u16,
    pub body: Vec<u8>,
    pub trailers: usize,
    pub info: usize,
}

pub struct Reader<'a> {
    addr: SocketAddr,
    config: Config,
    path: &'a str,
}

impl<'a> Reader<'a> {
    pub fn new(addr: SocketAddr, config: Config, path: &'a str) -> Self {
        Self { addr, config, path }
    }
    pub fn try_run<R, C>(
        self,
        check: C,
        policy: impl recovery::Observe,
        done: impl for<'run> FnOnce(Result<Result<R, Step>, runtime::Error<'run>>) -> Result<R, Step>,
    ) -> Result<R, Error<runtime::Failure, Infallible>>
    where
        C: for<'d> FnOnce(Result<Capture, Exchange<'d>>) -> R,
    {
        let Self { addr, config, path } = self;
        let output = pools::Plan::new(addr, config, 1, 1)
            .map_err(Error::FixedPlan)?
            .enter(
                move |mut app| -> Result<_, Step> {
                    let client = app.handle;
                    app.wait().map_err(Step::Wait)?;
                    let outcome = (|| {
                        let mut stream = app
                            .app
                            .block_on(client.stream_get(path))
                            .map_err(Exchange::Drive)?
                            .map_err(|error| Exchange::Response(error.widen()))?;
                        let mut status = None;
                        let mut body = Vec::new();
                        let mut trailers = 0;
                        let mut info = 0;
                        while let Some(event) = app
                            .app
                            .block_on(stream.next_event())
                            .map_err(Exchange::Drive)?
                        {
                            match event.map_err(|error| Exchange::Response(error.widen()))? {
                                Event::Informational(_) => info += 1,
                                Event::Head(head) => status = Some(head.status().as_u16()),
                                Event::Data(data) => body.extend_from_slice(data.as_slice()),
                                Event::Trailers(fields) => {
                                    trailers += fields
                                        .as_bytes()
                                        .iter()
                                        .filter(|&&byte| byte == b'\n')
                                        .count()
                                }
                            }
                        }
                        Ok(Capture {
                            status: status.ok_or(Exchange::Head(None))?,
                            body,
                            trailers,
                            info,
                        })
                    })();
                    Ok::<R, Step>(check(outcome))
                },
                policy,
                done,
            )
            .map_err(Error::Fixed)?
            .map_err(Error::Step)?;
        Ok(output)
    }
}
