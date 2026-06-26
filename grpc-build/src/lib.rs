use std::fmt::Write;
use std::io;
use std::path::Path;

pub use prost_build;
use prost_build::{Method, Service, ServiceGenerator};

pub struct Config {
    prost: prost_build::Config,
}

impl Config {
    pub fn new() -> Self {
        let mut prost = prost_build::Config::new();
        prost.service_generator(Box::new(SarkServiceGenerator));
        Self { prost }
    }

    pub fn prost_config_mut(&mut self) -> &mut prost_build::Config {
        &mut self.prost
    }

    pub fn compile_protos(
        &mut self,
        protos: &[impl AsRef<Path>],
        includes: &[impl AsRef<Path>],
    ) -> io::Result<()> {
        self.prost.compile_protos(protos, includes)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

pub fn compile_protos(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> io::Result<()> {
    Config::new().compile_protos(protos, includes)
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SarkServiceGenerator;

impl ServiceGenerator for SarkServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        generate_service(&service, buf);
    }
}

fn generate_service(service: &Service, buf: &mut String) {
    let trait_name = format!("{}Service", service.name);
    let routes_fn = format!("{}_routes", to_snake(&service.name));
    let client_name = format!("{}Client", service.name);
    let shared_ty = "::std::rc::Rc<::std::cell::RefCell<S>>";

    let _ = writeln!(buf);
    let _ = writeln!(buf, "pub trait {trait_name}: 'static {{");
    for method in &service.methods {
        let req_ty = &method.input_type;
        let resp_ty = &method.output_type;
        let method_name = &method.name;
        if method.client_streaming || method.server_streaming {
            let _ = writeln!(
                buf,
                "    fn {method_name}(&mut self, request: ::sark_grpc::StreamingRequest<{req_ty}>) -> ::sark_grpc::StreamingResponse<{resp_ty}>;"
            );
        } else {
            let _ = writeln!(
                buf,
                "    fn {method_name}(&mut self, request: ::sark_grpc::UnaryRequest<{req_ty}>) -> ::sark_grpc::UnaryResponse<{resp_ty}>;"
            );
        }
    }
    let _ = writeln!(buf, "}}");
    let _ = writeln!(buf);

    for method in &service.methods {
        emit_server_wrapper(service, method, &trait_name, buf);
    }

    let routes_enum = routes_enum_name(service);
    emit_routes_enum(service, &trait_name, &routes_enum, buf);

    let _ = writeln!(
        buf,
        "pub fn {routes_fn}<S: {trait_name}>(service: S) -> ::sark_grpc::Routes<{routes_enum}<S>> {{"
    );
    let _ = writeln!(
        buf,
        "    let shared: {shared_ty} = ::std::rc::Rc::new(::std::cell::RefCell::new(service));"
    );
    let _ = writeln!(buf, "    let mut routes = ::sark_grpc::Routes::new();");
    for method in &service.methods {
        let wrapper = wrapper_name(service, method);
        let variant = variant_name(service, method);
        let path = grpc_path(service, method);
        if method.client_streaming || method.server_streaming {
            let _ = writeln!(
                buf,
                "    routes.push(b\"{path}\", {routes_enum}::{variant}(::sark_grpc::Streaming::new({wrapper} {{ inner: shared.clone() }}, ::sark_grpc::ProstCodec::<{}, {}>::new())));",
                method.output_type, method.input_type
            );
        } else {
            let _ = writeln!(
                buf,
                "    routes.push(b\"{path}\", {routes_enum}::{variant}(::sark_grpc::Unary::new({wrapper} {{ inner: shared.clone() }}, ::sark_grpc::ProstCodec::<{}, {}>::new())));",
                method.output_type, method.input_type
            );
        }
    }
    let _ = writeln!(buf, "    routes");
    let _ = writeln!(buf, "}}");
    let _ = writeln!(buf);

    emit_client(service, &client_name, buf);
}

fn emit_server_wrapper(service: &Service, method: &Method, trait_name: &str, buf: &mut String) {
    let wrapper = wrapper_name(service, method);
    let req_ty = &method.input_type;
    let resp_ty = &method.output_type;
    let method_name = &method.name;
    let _ = writeln!(buf, "pub struct {wrapper}<S: {trait_name}> {{");
    let _ = writeln!(
        buf,
        "    pub inner: ::std::rc::Rc<::std::cell::RefCell<S>>,"
    );
    let _ = writeln!(buf, "}}");
    if method.client_streaming || method.server_streaming {
        let _ = writeln!(
            buf,
            "impl<S: {trait_name}> ::sark_grpc::StreamingHandler for {wrapper}<S> {{"
        );
        let _ = writeln!(buf, "    type Request = {req_ty};");
        let _ = writeln!(buf, "    type Response = {resp_ty};");
        let _ = writeln!(
            buf,
            "    type Codec = ::sark_grpc::ProstCodec<{resp_ty}, {req_ty}>;"
        );
        let _ = writeln!(
            buf,
            "    fn on_stream(&mut self, request: ::sark_grpc::StreamingRequest<{req_ty}>) -> ::sark_grpc::StreamingResponse<{resp_ty}> {{"
        );
        let _ = writeln!(
            buf,
            "        self.inner.borrow_mut().{method_name}(request)"
        );
        let _ = writeln!(buf, "    }}");
        let _ = writeln!(buf, "}}");
    } else {
        let _ = writeln!(
            buf,
            "impl<S: {trait_name}> ::sark_grpc::UnaryHandler for {wrapper}<S> {{"
        );
        let _ = writeln!(buf, "    type Request = {req_ty};");
        let _ = writeln!(buf, "    type Response = {resp_ty};");
        let _ = writeln!(
            buf,
            "    type Codec = ::sark_grpc::ProstCodec<{resp_ty}, {req_ty}>;"
        );
        let _ = writeln!(
            buf,
            "    fn on_unary(&mut self, request: ::sark_grpc::UnaryRequest<{req_ty}>) -> ::sark_grpc::UnaryResponse<{resp_ty}> {{"
        );
        let _ = writeln!(
            buf,
            "        self.inner.borrow_mut().{method_name}(request)"
        );
        let _ = writeln!(buf, "    }}");
        let _ = writeln!(buf, "}}");
    }
    let _ = writeln!(buf);
}

fn emit_client(service: &Service, client_name: &str, buf: &mut String) {
    let _ = writeln!(buf, "pub struct {client_name};");
    let _ = writeln!(buf, "impl {client_name} {{");
    for method in &service.methods {
        let path = grpc_path(service, method);
        let req_ty = &method.input_type;
        let resp_ty = &method.output_type;
        let method_name = &method.name;
        let open_name = format!("open_{method_name}");
        let send_name = format!("send_{method_name}");
        let finish_name = format!("finish_{method_name}");
        let poll_name = format!("poll_{method_name}");
        let decode_name = format!("decode_{method_name}");
        if method.client_streaming {
            let _ = writeln!(
                buf,
                "    pub fn {method_name}(session: &mut ::sark_grpc::Session, authority: ::core::option::Option<&[u8]>, metadata: &::sark_grpc::Metadata, messages: &[{req_ty}]) -> ::core::result::Result<::sark_grpc::StreamId, ::sark_grpc::Status> {{"
            );
            let _ = writeln!(
                buf,
                "        let mut codec = ::sark_grpc::ProstCodec::<{req_ty}, {resp_ty}>::new();"
            );
            let _ = writeln!(
                buf,
                "        session.start_streaming(b\"{path}\", authority, metadata, &mut codec, messages)"
            );
            let _ = writeln!(buf, "    }}");
            let _ = writeln!(
                buf,
                "    pub fn {open_name}(session: &mut ::sark_grpc::Session, authority: ::core::option::Option<&[u8]>, metadata: &::sark_grpc::Metadata) -> ::core::result::Result<::sark_grpc::StreamId, ::sark_grpc::Status> {{"
            );
            let _ = writeln!(
                buf,
                "        session.start_stream_raw(b\"{path}\", authority, metadata)"
            );
            let _ = writeln!(buf, "    }}");
            let _ = writeln!(
                buf,
                "    pub fn {send_name}(session: &mut ::sark_grpc::Session, stream_id: ::sark_grpc::StreamId, request: &{req_ty}) -> ::core::result::Result<(), ::sark_grpc::Status> {{"
            );
            let _ = writeln!(
                buf,
                "        let mut codec = ::sark_grpc::ProstCodec::<{req_ty}, {resp_ty}>::new();"
            );
            let _ = writeln!(
                buf,
                "        session.send_message(stream_id, &mut codec, request)"
            );
            let _ = writeln!(buf, "    }}");
            let _ = writeln!(
                buf,
                "    pub fn {finish_name}(session: &mut ::sark_grpc::Session, stream_id: ::sark_grpc::StreamId) -> ::core::result::Result<(), ::sark_grpc::Status> {{"
            );
            let _ = writeln!(buf, "        session.finish_send(stream_id)");
            let _ = writeln!(buf, "    }}");
        } else {
            let _ = writeln!(
                buf,
                "    pub fn {method_name}(session: &mut ::sark_grpc::Session, authority: ::core::option::Option<&[u8]>, metadata: &::sark_grpc::Metadata, request: &{req_ty}) -> ::core::result::Result<::sark_grpc::StreamId, ::sark_grpc::Status> {{"
            );
            let _ = writeln!(
                buf,
                "        let mut codec = ::sark_grpc::ProstCodec::<{req_ty}, {resp_ty}>::new();"
            );
            let _ = writeln!(
                buf,
                "        session.start_unary(b\"{path}\", authority, metadata, &mut codec, request)"
            );
            let _ = writeln!(buf, "    }}");
        }
        if method.server_streaming {
            let _ = writeln!(
                buf,
                "    pub fn {decode_name}(result: ::sark_grpc::UnaryResult) -> ::core::result::Result<::std::vec::Vec<{resp_ty}>, ::sark_grpc::Status> {{"
            );
            let _ = writeln!(
                buf,
                "        let mut codec = ::sark_grpc::ProstCodec::<{req_ty}, {resp_ty}>::new();"
            );
            let _ = writeln!(buf, "        result.decode_messages(&mut codec)");
            let _ = writeln!(buf, "    }}");
        } else {
            let _ = writeln!(
                buf,
                "    pub fn {decode_name}(result: ::sark_grpc::UnaryResult) -> ::core::result::Result<{resp_ty}, ::sark_grpc::Status> {{"
            );
            let _ = writeln!(
                buf,
                "        let mut codec = ::sark_grpc::ProstCodec::<{req_ty}, {resp_ty}>::new();"
            );
            let _ = writeln!(buf, "        result.decode_single(&mut codec)");
            let _ = writeln!(buf, "    }}");
        }
        let _ = writeln!(
            buf,
            "    pub fn {poll_name}(session: &mut ::sark_grpc::Session) -> ::core::option::Option<::core::result::Result<::sark_grpc::TypedStreamEvent<{resp_ty}>, ::sark_grpc::Status>> {{"
        );
        let _ = writeln!(buf, "        let event = session.poll_event()?;");
        let _ = writeln!(
            buf,
            "        let mut codec = ::sark_grpc::ProstCodec::<{req_ty}, {resp_ty}>::new();"
        );
        let _ = writeln!(
            buf,
            "        ::core::option::Option::Some(event.decode(&mut codec))"
        );
        let _ = writeln!(buf, "    }}");
    }
    let _ = writeln!(buf, "}}");
}

fn emit_routes_enum(service: &Service, trait_name: &str, routes_enum: &str, buf: &mut String) {
    let _ = writeln!(buf, "pub enum {routes_enum}<S: {trait_name}> {{");
    for method in &service.methods {
        let variant = variant_name(service, method);
        let handler_ty = variant_handler_type(service, method);
        let _ = writeln!(buf, "    {variant}({handler_ty}),");
    }
    let _ = writeln!(buf, "}}");

    let _ = writeln!(
        buf,
        "impl<S: {trait_name}> ::sark_grpc::server::Handler for {routes_enum}<S> {{"
    );

    let _ = writeln!(
        buf,
        "    fn on_start(&mut self, routes: &mut ::sark_grpc::server::StreamRoutes, stream_id: ::sark_grpc::StreamId, head: &::sark_grpc::RequestHead, reply: &mut ::sark_grpc::StreamReply) -> ::sark_grpc::StreamMode {{"
    );
    let _ = writeln!(buf, "        match self {{");
    for method in &service.methods {
        let variant = variant_name(service, method);
        let _ = writeln!(
            buf,
            "            {routes_enum}::{variant}(h) => h.on_start(routes, stream_id, head, reply),"
        );
    }
    let _ = writeln!(buf, "        }}");
    let _ = writeln!(buf, "    }}");

    let _ = writeln!(
        buf,
        "    fn on_message(&mut self, routes: &mut ::sark_grpc::server::StreamRoutes, stream_id: ::sark_grpc::StreamId, message: ::sark_grpc::MessageFrame, reply: &mut ::sark_grpc::StreamReply) {{"
    );
    let _ = writeln!(buf, "        match self {{");
    for method in &service.methods {
        let variant = variant_name(service, method);
        let _ = writeln!(
            buf,
            "            {routes_enum}::{variant}(h) => h.on_message(routes, stream_id, message, reply),"
        );
    }
    let _ = writeln!(buf, "        }}");
    let _ = writeln!(buf, "    }}");

    let _ = writeln!(
        buf,
        "    fn on_trailers(&mut self, routes: &mut ::sark_grpc::server::StreamRoutes, stream_id: ::sark_grpc::StreamId, trailers: ::sark_grpc::Metadata, reply: &mut ::sark_grpc::StreamReply) {{"
    );
    let _ = writeln!(buf, "        match self {{");
    for method in &service.methods {
        let variant = variant_name(service, method);
        let _ = writeln!(
            buf,
            "            {routes_enum}::{variant}(h) => h.on_trailers(routes, stream_id, trailers, reply),"
        );
    }
    let _ = writeln!(buf, "        }}");
    let _ = writeln!(buf, "    }}");

    let _ = writeln!(
        buf,
        "    fn on_request(&mut self, request: ::sark_grpc::Request, response: &mut ::sark_grpc::Response) {{"
    );
    let _ = writeln!(buf, "        match self {{");
    for method in &service.methods {
        let variant = variant_name(service, method);
        let _ = writeln!(
            buf,
            "            {routes_enum}::{variant}(h) => h.on_request(request, response),"
        );
    }
    let _ = writeln!(buf, "        }}");
    let _ = writeln!(buf, "    }}");

    let _ = writeln!(buf, "}}");
    let _ = writeln!(buf);
}

fn variant_handler_type(service: &Service, method: &Method) -> String {
    let wrapper = wrapper_name(service, method);
    if method.client_streaming || method.server_streaming {
        format!("::sark_grpc::Streaming<{wrapper}<S>>")
    } else {
        format!("::sark_grpc::Unary<{wrapper}<S>>")
    }
}

fn routes_enum_name(service: &Service) -> String {
    format!("__SarkGrpc{}Routes", service.name)
}

fn variant_name(service: &Service, method: &Method) -> String {
    let _ = service;
    to_upper_camel(&method.name)
}

fn wrapper_name(service: &Service, method: &Method) -> String {
    format!("__SarkGrpc{}{}", service.name, to_upper_camel(&method.name))
}

fn grpc_path(service: &Service, method: &Method) -> String {
    if service.package.is_empty() {
        format!("/{}/{}", service.proto_name, method.proto_name)
    } else {
        format!(
            "/{}.{}/{}",
            service.package, service.proto_name, method.proto_name
        )
    }
}

fn to_upper_camel(input: &str) -> String {
    let mut out = String::new();
    let mut upper = true;
    for ch in input.chars() {
        if ch == '_' {
            upper = true;
        } else if upper {
            out.extend(ch.to_uppercase());
            upper = false;
        } else {
            out.push(ch);
        }
    }
    out
}

fn to_snake(input: &str) -> String {
    let mut out = String::new();
    for (i, ch) in input.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use prost_build::{Comments, Method, Service};

    use super::*;

    fn method(name: &str, input: &str, output: &str, cs: bool, ss: bool) -> Method {
        Method {
            name: to_snake(name),
            proto_name: name.to_string(),
            comments: Comments::default(),
            input_type: input.to_string(),
            output_type: output.to_string(),
            input_proto_type: format!(".test.{input}"),
            output_proto_type: format!(".test.{output}"),
            options: Default::default(),
            client_streaming: cs,
            server_streaming: ss,
        }
    }

    #[test]
    fn emits_routes_and_client_for_unary_and_streaming() {
        let service = Service {
            name: "Echo".to_string(),
            proto_name: "Echo".to_string(),
            package: "test.echo".to_string(),
            comments: Comments::default(),
            methods: vec![
                method("Say", "SayRequest", "SayResponse", false, false),
                method("Chat", "ChatRequest", "ChatResponse", true, true),
            ],
            options: Default::default(),
        };
        let mut out = String::new();

        generate_service(&service, &mut out);

        assert!(out.contains("pub trait EchoService"));
        assert!(out.contains("pub fn echo_routes"));
        assert!(out.contains("-> ::sark_grpc::Routes<__SarkGrpcEchoRoutes<S>>"));
        assert!(out.contains("enum __SarkGrpcEchoRoutes<S: EchoService>"));
        assert!(out.contains(
            "impl<S: EchoService> ::sark_grpc::server::Handler for __SarkGrpcEchoRoutes<S>"
        ));
        assert!(out.contains("b\"/test.echo.Echo/Say\""));
        assert!(out.contains("::sark_grpc::Unary::new"));
        assert!(out.contains("::sark_grpc::Streaming::new"));
        assert!(out.contains("__SarkGrpcEchoRoutes::Say(::sark_grpc::Unary::new"));
        assert!(out.contains("pub struct EchoClient"));
        assert!(out.contains("session.start_streaming"));
        assert!(out.contains("pub fn open_chat"));
        assert!(out.contains("pub fn send_chat"));
        assert!(out.contains("pub fn finish_chat"));
        assert!(out.contains("pub fn poll_chat"));
        assert!(out.contains("pub fn decode_chat"));
    }
}
