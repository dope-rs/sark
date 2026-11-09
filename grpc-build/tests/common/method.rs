use prost_build::{Comments, Method};

pub(super) fn new(name: &str, proto_name: &str, client_streaming: bool) -> Method {
    Method {
        name: name.into(),
        proto_name: proto_name.into(),
        comments: Comments::default(),
        input_type: "()".into(),
        output_type: "()".into(),
        input_proto_type: ".sark.test.Unit".into(),
        output_proto_type: ".sark.test.Unit".into(),
        options: Default::default(),
        client_streaming,
        server_streaming: false,
    }
}
