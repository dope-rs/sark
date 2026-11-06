use prost_build::{Comments, Service};

use super::method;

pub fn generate(service: Service) -> syn::Result<syn::File> {
    use prost_build::ServiceGenerator as _;

    let mut code = String::new();
    let mut generator = sark_grpc_build::Routes::default();
    generator.generate(service, &mut code);
    generator.finalize_package("sark.test", &mut code);
    syn::parse_file(&code)
}

pub fn duplicate_paths() -> Service {
    let mut service = empty("Proof");
    service.methods = vec![
        method::new("first", "Same", false),
        method::new("second", "Same", false),
    ];
    service
}

pub fn streaming() -> Service {
    let mut service = empty("Streaming");
    service.methods = vec![method::new("upload", "Same", true)];
    service
}

pub fn empty(proto_name: &str) -> Service {
    Service {
        name: "Proof".into(),
        proto_name: proto_name.into(),
        package: "sark.test".into(),
        comments: Comments::default(),
        methods: Vec::new(),
        options: Default::default(),
    }
}
