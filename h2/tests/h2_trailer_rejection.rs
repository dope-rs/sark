use std::io::Write;

use dope::core::driver::life::recovery;

pub mod common;

use std::{cell::Cell, convert::Infallible, time::Duration};

use common::session::Connect;
use dope::{
    core::driver::route::{FrameworkTag, Tag, kind},
    runtime::{
        executor::session::{Completion, Error},
        topology,
    },
};
use dope_test::{harness::server::Pair, reclaim::Reports};
use sark_core::manifold::stream::Boundary;
use sark_h2::{
    conn::CLIENT_PREFACE,
    frame::{
        Block,
        controls::RstStream,
        layout::Data,
        numbers::{ErrorCode, Flags, Type},
    },
    hpack::encode::{Encoder, Mode},
    server::Server,
    stream::Id,
};
use sark_http::field::metadata::map::record::Entry;

use crate::common::{
    INITIAL_SETTINGS,
    protocol::session::Frame,
    trailers::{Blocks, Echo, ReadFrames},
};

#[test]
fn rejects_split_trailers() {
    let pair = Pair::bind().expect("H2 trailer harness");
    let bind = pair.addr();
    pair.check(
        move |context, listener| {
            let calls = Cell::new(0);
            let prepared =
                Server::from_handler(Echo(&calls), Echo::try_plan(bind).expect("H2 server plan"))
                    .prepare_on(listener)
                    .expect("H2 server resources");
            let mut reports =
                Reports::<1>::new(prepared.layout(), [<FrameworkTag<kind::Any> as Tag>::ROUTE]);
            let mut finish = Completion::new();
            prepared.enter_with(
                context,
                |worker| {
                    worker.lend(|plan, stage| {
                        plan.install(stage, |host, stage| {
                            stage.with_app(&mut finish, host, |app| {
                                app.run().inspect(|_| assert_eq!(calls.get(), 1))
                            })
                        })
                        .map_err(topology::Failure::Install)?
                        .map_err(topology::Failure::Run)?
                        .map_err(|cause| topology::Failure::Run(Error::Drive(cause)))
                    })
                },
                recovery::Policy::new(|event| reports.push(event), common::recover),
                |result| {
                    let result = match result {
                        Ok(result) => result.map_err(topology::RunError::Failure),
                        Err(error) => Err(topology::RunError::from(error)),
                    };
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(topology::RunError::cause)
                        .expect("H2 server shutdown")
                },
            );
            assert!(
                reports.events().expect("H2 retirement").is_empty(),
                "retirement: {reports:?}"
            );
            Ok::<(), Infallible>(())
        },
        move |address| {
            let mut socket = address
                .try_connect(Duration::from_secs(3))
                .expect("H2 peer connection");
            let mut encoder = Encoder::try_new(4096).expect("HPACK encoder");
            encoder.set_mode(Mode::Plain);

            let mut first = Vec::new();
            first.extend_from_slice(CLIENT_PREFACE);
            first.extend_from_slice(INITIAL_SETTINGS);
            first.extend_from_slice(
                &Frame::new(1, &encoder.try_request().expect("HPACK request"))
                    .try_headers(Boundary::More, Block::End)
                    .expect("request HEADERS"),
            );
            first.extend_from_slice(
                &Frame::new(1, b"partial")
                    .try_data(Boundary::More)
                    .expect("partial DATA"),
            );
            let (invalid, len) = encoder
                .try_invalid(Id::new(1).expect("valid client stream"))
                .expect("invalid trailer frames");
            assert!(len > 1, "both trailer fragments must be nonempty");
            first.extend_from_slice(&invalid);
            socket.write_all(&first).expect("write malformed request");

            let mut pending = Vec::new();
            let mut reset = None;
            socket
                .try_frames(&mut pending, |header, payload| {
                    if header.kind == Type::RstStream
                        && header.stream_id == Id::new(1).expect("valid client stream")
                    {
                        reset = Some(
                            RstStream::parse(header, payload)
                                .expect("server RST_STREAM frame is well-formed")
                                .error(),
                        );
                        return true;
                    }
                    false
                })
                .expect("reset response frames");
            assert_eq!(reset, Some(ErrorCode::ProtocolError));

            let mut block = encoder.try_request().expect("HPACK request");
            let before = block.len();
            encoder
                .try_encode([Entry::new(b"x-discard-sync", b"retained")], &mut block)
                .expect("HPACK table");
            assert_eq!(
                block.len(),
                before + 1,
                "follow-up request must reference the discarded block's dynamic entry",
            );
            encoder
                .try_encode([Entry::new(b"content-length", b"7")], &mut block)
                .expect("HPACK table");
            let mut next = Frame::new(3, &block)
                .try_headers(Boundary::More, Block::End)
                .expect("healthy HEADERS");
            next.extend_from_slice(
                &Frame::new(3, b"healthy")
                    .try_data(Boundary::End)
                    .expect("healthy DATA"),
            );
            socket.write_all(&next).expect("write healthy request");

            let mut response = Vec::new();
            let mut fault = None;
            socket
                .try_frames(&mut pending, |header, payload| {
                    if header.kind == Type::RstStream
                        && header.stream_id == Id::new(3).expect("valid client stream")
                    {
                        let reset = RstStream::parse(header, payload).expect("parse reset");
                        fault = Some(reset.error());
                        return true;
                    }
                    if header.kind != Type::Data
                        || header.stream_id != Id::new(3).expect("valid client stream")
                    {
                        return false;
                    }
                    let frame = Data::parse(header, payload).expect("response DATA");
                    response.extend_from_slice(frame.payload());
                    header.flags.contains(Flags::END_STREAM)
                })
                .expect("healthy response frames");
            assert_eq!(fault, None, "healthy stream reset");
            assert_eq!(response, b"healthy");
        },
        |result| result.expect("H2 server runtime"),
    )
    .expect("trailer rejection E2E");
}
