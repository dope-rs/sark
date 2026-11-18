use std::{convert::Infallible, num::NonZero};

use dope::core::driver::{self, life::recovery, schedule::ready::task::owner};
use dope_test::scope;
use sark::{
    core::{
        body::{self, Build, Step},
        request::raw::Parsed,
        route,
        route::raw::PathCapture,
    },
    http::{
        h1::target::Target,
        o3::buffer::storage::Shared,
        payload::owned::Segments,
        request,
        request::{Form, Layout},
    },
    request::Head,
};

pub mod common;

use common::router::{ParamReq, ParamReqHeadersRaw, ParamReqParamsRaw};
use sark::core::body::admit::{Length, Plan, Retention, Stream};

#[test]
fn retained_metadata() {
    let wire = b"POST http://origin.example/echo/item-42?q=1 HTTP/1.1\r\nHost: ignored.example\r\nContent-Length: 13\r\n\r\nborrowed-body";
    let hlen = wire
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
        .expect("complete request head");
    let start = b"POST ".len();
    let end = wire[start..]
        .iter()
        .position(|byte| *byte == b' ')
        .map(|offset| start + offset)
        .expect("complete request target");
    let target = Target::parse(b"POST", &wire[start..end])
        .and_then(|plan| plan.shifted(start))
        .expect("valid absolute-form target");
    let auth = target.authority().expect("target authority");
    let path = target.path_span();
    let owner = Shared::try_copy(wire).expect("wire owner allocation");
    let ptr = owner.as_slice().as_ptr();
    let target =
        request::Target::from_span(Form::Path, path).expect("representable request target");
    let layout = Layout::new(target, Some(auth), Some(path));
    let mut reports = Vec::new();
    scope::Driver::new(Default::default()).with_driver(
        |driver| {
            let binding = driver::Binding::new(
                driver.driver_ref().timer(),
                owner::Capacity::new(NonZero::<u32>::MIN),
            );
            let ids = request::Seq::new(binding.sequence());
            let blen = wire.len() - hlen;
            let track = Plan::new(
                Retention::Retained,
                blen,
                Length::Known(blen),
                Stream::Buffered,
            )
            .try_admit(blen, blen)
            .expect("test body admission");
            let request = owner;
            let tail = request
                .try_clone()
                .expect("body owner reference")
                .project(hlen..wire.len())
                .expect("body range");
            let head = Head::bind(request.project(0..hlen).expect("head range"), layout)
                .expect("request layout");
            let body = body::Body::try_new(track, Segments::from(tail)).expect("body admission");
            // SAFETY: this head retains the validated wire; ParamReq has no headers,
            // and its only capture is `item-42` at 6..13 within `/echo/item-42`.
            let parsed = unsafe {
                Parsed::<_, ParamReq>::new(
                    head,
                    <ParamReqParamsRaw as route::Params>::from_match((PathCapture::new(6, 13),)),
                    ParamReqHeadersRaw::default(),
                )
            };
            let req = parsed
                .with_id(ids.try_next().expect("request id"))
                .with_body(body);
            let build =
                <ParamReq as route::Own>::start_task(req).expect("retained request builder");
            let step = Build::step(build, NonZero::<usize>::MAX);
            let ready = if let Step::Ready { output, released } = step {
                Some((output, released))
            } else {
                None
            }
            .expect("non-JSON request must be immediately ready");
            assert!(ready.1.is_empty());
            let req = ready.0.into_parts().1;

            assert_eq!(req.id.as_slice(), b"item-42");
            assert_eq!(req.payload.as_slice(), b"borrowed-body");
            assert_eq!(
                req.id.as_slice().as_ptr(),
                ptr.wrapping_add(path.start() + 6)
            );
            assert_eq!(req.payload.as_slice().as_ptr(), ptr.wrapping_add(hlen));
        },
        recovery::Policy::new(
            |event| reports.push(event),
            |failure: recovery::Recovery<'_, '_>| -> Infallible {
                panic!("native recovery: {:?}", failure.cause())
            },
        ),
        |result| {
            result
                .as_ref()
                .map_err(scope::Error::cause)
                .expect("identity driver scope");
        },
    );
    assert!(
        reports.is_empty(),
        "unexpected identity-scope I/O: {reports:?}"
    );
}
