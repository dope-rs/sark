use sark_grpc::frame::MessageFrame;
use sark_grpc::headers::RequestHead;
use sark_grpc::server::{Config, Handler, Request, Response, Routes, dispatch_buffered};
use sark_grpc::status::Code;

struct Echo;

impl Handler for Echo {
    fn on_request(&mut self, request: Request, response: &mut Response) {
        for message in &request.messages {
            response.push_message(message.payload.clone());
        }
    }
}

fn head(path: &[u8]) -> RequestHead {
    RequestHead {
        path: path.to_vec(),
        authority: None,
        metadata: Default::default(),
    }
}

fn framed(count: usize, size: usize, compressed: bool) -> Vec<u8> {
    let mut out = Vec::new();
    let payload = vec![0u8; size];
    for _ in 0..count {
        MessageFrame::encode(compressed, &payload, &mut out).unwrap();
    }
    out
}

#[test]
fn drives_handler_and_reframes_to_original_wire() {
    let body = framed(2, 4, false);
    let response = dispatch_buffered(&mut Echo, head(b"/svc"), &body, &Config::default());
    assert_eq!(response.status.code(), Code::Ok);
    assert_eq!(response.messages.len(), 2);
    let mut wire = Vec::new();
    response.encode_body(&mut wire).unwrap();
    assert_eq!(
        wire, body,
        "encode_body must reframe to the original wire bytes"
    );
}

#[test]
fn over_long_message_is_rejected() {
    let config = Config {
        max_message_len: 2,
        ..Config::default()
    };
    let response = dispatch_buffered(&mut Echo, head(b"/svc"), &framed(1, 8, false), &config);
    assert_ne!(response.status.code(), Code::Ok);
    assert!(response.messages.is_empty());
}

#[test]
fn compressed_message_is_unimplemented() {
    let response = dispatch_buffered(
        &mut Echo,
        head(b"/svc"),
        &framed(1, 4, true),
        &Config::default(),
    );
    assert_eq!(response.status.code(), Code::Unimplemented);
}

#[test]
fn unknown_method_is_unimplemented() {
    let mut routes: Routes<Echo> = Routes::new();
    let response = dispatch_buffered(&mut routes, head(b"/missing"), &[], &Config::default());
    assert_eq!(response.status.code(), Code::Unimplemented);
}
