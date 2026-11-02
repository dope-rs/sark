use std::{net::SocketAddr, time::Duration};

use dope::core::driver::{life::recovery, route::Identity};
use dope_fiber::BlockOn as _;
use sark_client::protocol::response::Event;
use sark_fiber::client::Stream;
use sark_http::method::Method;

use crate::common::client::{
    config::Config as _,
    runtime::{
        self,
        cases::{
            Flavor,
            request::{Error, Step},
            stream::Exchange,
        },
        origins, pools,
    },
};

#[derive(Debug, Default)]
pub struct Capture {
    pub status: Option<u16>,
    pub body: Vec<u8>,
    pub data: usize,
    pub trailers: usize,
    pub informational: usize,
    pub early_hints: usize,
}

pub fn try_run<R>(
    flavor: Flavor,
    addr: SocketAddr,
    check: impl for<'d> FnOnce(Result<(Capture, Capture), Exchange<'d>>) -> R,
    policy: impl recovery::Observe,
    done: impl for<'run> FnOnce(Result<Result<R, Step>, runtime::Error<'run>>) -> Result<R, Step>,
) -> Result<R, Error> {
    let config = flavor
        .try_config(addr)
        .map_err(Error::Host)?
        .timeout(Duration::from_secs(2))
        .request_pool(2, 4096)
        .queue_bytes(4096)
        .inflight(2);
    match flavor {
        Flavor::Fixed => pools::Plan::new(addr, config, 1, 2)
            .map_err(Error::FixedPlan)?
            .enter(
                |mut app| {
                    let client = app.handle;
                    app.wait().map_err(Step::Wait)?;
                    let result = (|| {
                        let mut head = app
                            .app
                            .block_on(client.stream_with(Method::HEAD, "/head", &[], &[]))
                            .map_err(Exchange::Drive)?
                            .map_err(|error| Exchange::Response(error.widen()))?;
                        let mut get = app
                            .app
                            .block_on(client.stream_get("/get"))
                            .map_err(Exchange::Drive)?
                            .map_err(|error| Exchange::Response(error.widen()))?;
                        Ok((
                            try_capture(&mut app.app, &mut head)?,
                            try_capture(&mut app.app, &mut get)?,
                        ))
                    })();
                    Ok(check(result))
                },
                policy,
                done,
            )
            .map_err(Error::Fixed)?
            .map_err(Error::Step),
        Flavor::OnDemand => origins::Plan::new(config, 1, 2, 1)
            .map_err(Error::DynamicPlan)?
            .enter(
                |mut app| {
                    let client = app.handle;
                    let result = (|| {
                        let mut head = app
                            .app
                            .block_on(client.stream_with(Method::HEAD, "/head", &[], &[]))
                            .map_err(Exchange::Drive)?
                            .map_err(Exchange::Response)?;
                        let mut get = app
                            .app
                            .block_on(client.stream_get("/get"))
                            .map_err(Exchange::Drive)?
                            .map_err(Exchange::Response)?;
                        Ok((
                            try_capture(&mut app.app, &mut head)?,
                            try_capture(&mut app.app, &mut get)?,
                        ))
                    })();
                    Ok(check(result))
                },
                policy,
                done,
            )
            .map_err(Error::Dynamic)?
            .map_err(Error::Step),
    }
}

fn try_capture<'app, 'e, 'b, 'c, 'd: 'app, R, B: ?Sized, ID: Identity>(
    runtime: &mut R,
    stream: &mut Stream<'app, 'e, 'b, 'c, 'd, B, ID>,
) -> Result<Capture, Exchange<'d>>
where
    R: dope_fiber::BlockOn<'d>,
{
    let mut capture = Capture::default();
    while let Some(event) = runtime
        .block_on(stream.next_event())
        .map_err(Exchange::Drive)?
    {
        match event.map_err(|error| Exchange::Response(error.widen()))? {
            Event::Informational(head) => {
                capture.informational += 1;
                capture.early_hints += usize::from(head.status().as_u16() == 103);
            }
            Event::Head(head) => capture.status = Some(head.status().as_u16()),
            Event::Data(data) => {
                capture.data += 1;
                capture.body.extend_from_slice(data.as_slice());
            }
            Event::Trailers(_) => capture.trailers += 1,
        }
    }
    Ok(capture)
}
