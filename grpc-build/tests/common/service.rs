use prost_build::{Comments, Method, Service};

use super::method;

pub fn generate(services: impl IntoIterator<Item = Service>) -> syn::File {
    use prost_build::ServiceGenerator as _;

    let mut code = String::new();
    for service in services {
        sark_grpc_build::Routes.generate(service, &mut code);
    }
    syn::parse_file(&code).expect("generated service")
}

pub fn duplicate_paths() -> Service {
    let mut service = empty("Proof");
    service.methods = vec![
        method::new("first", "Same", false, false),
        method::new("second", "Same", false, false),
    ];
    service
}

pub fn streaming() -> Service {
    let mut service = empty("Streaming");
    service.methods = vec![method::new("upload", "Same", true, false)];
    service
}

pub fn empty(proto_name: &str) -> Service {
    Service {
        proto_name: proto_name.into(),
        ..new("Proof", Vec::new())
    }
}

fn new(name: &str, methods: Vec<Method>) -> Service {
    Service {
        name: name.into(),
        proto_name: name.into(),
        package: "sark.test".into(),
        comments: Comments::default(),
        methods,
        options: Default::default(),
    }
}

pub fn routes() -> [Service; 3] {
    [
        new(
            "Proof",
            [
                ("alpha", "Alpha"),
                ("alpine", "Alpine"),
                ("beta", "Beta"),
                ("r#type", "Type"),
                ("call", "Call"),
                ("unknown", "Unknown"),
            ]
            .into_iter()
            .map(|(name, proto)| method::new(name, proto, false, false))
            .collect(),
        ),
        new(
            "_123",
            [
                ("_1", false, false),
                ("_2", true, false),
                ("_3", false, true),
                ("_4", true, true),
            ]
            .into_iter()
            .map(|(name, client, server)| method::new(name, name, client, server))
            .collect(),
        ),
        new("Empty", Vec::new()),
    ]
}

pub fn messages() -> [Service; 2] {
    [
        new(
            "Messages",
            [
                ("echo", "Echo", false, false, "S", "P"),
                ("upload", "Upload", true, false, "self::P", "W"),
                (
                    "watch",
                    "Watch",
                    false,
                    true,
                    "W",
                    "<::core::iter::Once<self::S> as ::core::iter::Iterator>::Item",
                ),
                (
                    "chat",
                    "Chat",
                    true,
                    true,
                    "super::schema::S",
                    "crate::common::schema::W",
                ),
            ]
            .into_iter()
            .map(|(name, proto, client, server, input, output)| Method {
                input_type: input.into(),
                output_type: output.into(),
                input_proto_type: format!(".sark.test.{input}"),
                output_proto_type: format!(".sark.test.{output}"),
                ..method::new(name, proto, client, server)
            })
            .collect(),
        ),
        new(
            "Raw",
            vec![Method {
                input_type: "r#S".into(),
                output_type: "r#P".into(),
                ..method::new("echo", "Echo", false, false)
            }],
        ),
    ]
}
