pub mod common;

use std::{cell::Cell, convert::Infallible, mem, rc::Rc};

use common::{
    codec::Bytes,
    grpc::{
        State,
        call::{contexts::*, egress::reply::Reply},
        settings::Settings,
    },
};
use dope_test::harness::{
    server::Pair,
    signals::{Gate, Progress as _},
};
use sark_grpc::{
    Code,
    server::{
        config::Limits,
        service::handler::modes::{Streaming, Unary},
    },
};

#[test]
fn owned_state() {
    let pair = Pair::bind().expect("context server");
    let settings = Settings::new(pair.addr(), Limits::default());
    pair.run(
        move |context, listener| {
            let observed = Rc::new(Cell::new(None));
            let route = Unary::<_, Owner>::new(Echo, Bytes);
            assert_eq!(mem::size_of_val(&route), 0);
            let state = State::new(
                route,
                Owner {
                    calls: 0,
                    report: Rc::clone(&observed),
                },
            );
            assert_eq!(mem::size_of_val(&state), mem::size_of::<Owner>());
            let retired = settings
                .try_serve_on(state, context, listener, common::recover, |result| {
                    result.expect("gRPC runtime")
                })
                .expect("server bank");
            assert!(
                retired.is_empty(),
                "unexpected server retirement: {retired:?}"
            );
            assert_eq!(observed.take(), Some(1));
            Ok::<_, Infallible>(())
        },
        |address| {
            let reply = Reply::try_exchange(address, b"/use.case/echo", &[b"proof"], true)
                .expect("context request");
            assert_eq!(reply.status().expect("response status").code(), Code::Ok);
            assert_eq!(reply.messages, [b"proof".to_vec()]);
        },
    )
    .expect("owned context");
}

#[test]
fn owned_stream() {
    let pair = Pair::bind().expect("stream context server");
    let settings = Settings::new(pair.addr(), Limits::default());
    pair.run(
        move |context, listener| {
            let observed = Rc::new(Cell::new(None));
            let route = Streaming::<_, Owner>::new(First, Bytes);
            assert_eq!(mem::size_of_val(&route), 0);
            let state = State::new(
                route,
                Owner {
                    calls: 0,
                    report: Rc::clone(&observed),
                },
            );
            let retired = settings
                .try_serve_on(state, context, listener, common::recover, |result| {
                    result.expect("gRPC runtime")
                })
                .expect("server bank");
            assert!(
                retired.is_empty(),
                "unexpected server retirement: {retired:?}"
            );
            assert_eq!(observed.take(), Some(1));
            Ok::<_, Infallible>(())
        },
        |address| {
            let reply = Reply::try_exchange(address, b"/use.case/stream", &[b"one", b"two"], true)
                .expect("stream context request");
            assert_eq!(reply.status().expect("response status").code(), Code::Ok);
            assert_eq!(reply.messages, [b"one".to_vec()]);
        },
    )
    .expect("owned stream context");
}

#[test]
fn lazy_decode() {
    let pair = Pair::bind().expect("lazy decode server");
    let settings = Settings::new(pair.addr(), Limits::default());
    pair.run(
        move |context, listener| {
            let events = Gate::new();
            let decoded = events.clone();
            let mut sizes = Cell::new(Vec::new());
            let retired = settings
                .try_serve_on(
                    Streaming::new(FirstOnly(&sizes), Count { decoded }),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("gRPC runtime"),
                )
                .expect("server bank");
            assert!(
                retired.is_empty(),
                "unexpected server retirement: {retired:?}"
            );
            assert_eq!(events.hits(), 1);
            assert_eq!(sizes.get_mut().as_slice(), &[(0, Some(2))]);
            Ok::<_, Infallible>(())
        },
        |address| {
            let reply = Reply::try_exchange(address, b"/use.case/lazy", &[b"one", b"two"], true)
                .expect("lazy request");
            assert_eq!(reply.status().expect("response status").code(), Code::Ok);
            assert_eq!(reply.messages, [b"one".to_vec()]);
        },
    )
    .expect("lazy decode");
}

#[test]
fn decode_error() {
    let pair = Pair::bind().expect("decode error server");
    let settings = Settings::new(pair.addr(), Limits::default());
    pair.run(
        move |context, listener| {
            let events = Gate::new();
            let decoded = events.clone();
            let retired = settings
                .try_serve_on(
                    Streaming::new(All, Count { decoded }),
                    context,
                    listener,
                    common::recover,
                    |result| result.expect("gRPC runtime"),
                )
                .expect("server bank");
            assert!(
                retired.is_empty(),
                "unexpected server retirement: {retired:?}"
            );
            assert_eq!(events.hits(), 2);
            Ok::<_, Infallible>(())
        },
        |address| {
            let reply =
                Reply::try_exchange(address, b"/use.case/lazy-error", &[b"good", b"bad"], true)
                    .expect("rejected decode");
            assert_eq!(
                reply.status().expect("response status").code(),
                Code::InvalidArgument
            );
            assert!(reply.messages.is_empty());
        },
    )
    .expect("decode error");
}
