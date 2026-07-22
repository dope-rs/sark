use sark_grpc::frame::MessageFrame;
use sark_grpc::headers::RequestHead;
use sark_grpc::server::{Handler, Limits, Request, Response, Routes};
use sark_grpc::status::Code;

struct Echo;

impl Handler for Echo {
    fn request(&mut self, request: Request, response: &mut Response) {
        for message in &request.messages {
            response.push_message(message.payload.as_slice().to_vec());
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
    let response = Limits::default().dispatch_buffered(&mut Echo, head(b"/svc"), &body);
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
    let config = Limits {
        max_message_len: 2,
        ..Limits::default()
    };
    let response = config.dispatch_buffered(&mut Echo, head(b"/svc"), &framed(1, 8, false));
    assert_ne!(response.status.code(), Code::Ok);
    assert!(response.messages.is_empty());
}

#[test]
fn compressed_message_is_unimplemented() {
    let response =
        Limits::default().dispatch_buffered(&mut Echo, head(b"/svc"), &framed(1, 4, true));
    assert_eq!(response.status.code(), Code::Unimplemented);
}

#[test]
fn unknown_method_is_unimplemented() {
    let mut routes: Routes<Echo> = Routes::new();
    let response = Limits::default().dispatch_buffered(&mut routes, head(b"/missing"), &[]);
    assert_eq!(response.status.code(), Code::Unimplemented);
}
