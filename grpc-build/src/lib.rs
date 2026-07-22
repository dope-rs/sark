use std::io;
use std::path::Path;

use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro2::{Ident, Literal, Span, TokenStream};
pub use prost_build;
use prost_build::{Method, Service, ServiceGenerator};
use quote::{format_ident, quote};
use syn::Type;

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
        buf.push('\n');
        buf.push_str(&generate_service(&service).to_string());
    }
}

fn generate_service(service: &Service) -> TokenStream {
    let service_name = rust_ident(&service.name, "service");
    let trait_name = format_ident!("{}Service", service_name);
    let routes_fn = format_ident!("{}_routes", service.proto_name.to_snake_case());
    let routes_enum = format_ident!("__SarkGrpc{}Routes", service_name);
    let client_name = format_ident!("{}Client", service_name);
    let methods = service
        .methods
        .iter()
        .map(|method| GeneratedMethod::new(service, method, &service_name))
        .collect::<Vec<_>>();

    let trait_methods = methods.iter().map(emit_trait_method);
    let server_wrappers = methods
        .iter()
        .map(|method| emit_server_wrapper(method, &trait_name));
    let routes = emit_routes_enum(&methods, &trait_name, &routes_enum);
    let registrations = methods.iter().map(|method| {
        let GeneratedMethod {
            descriptor,
            variant,
            wrapper,
            request,
            response,
            path,
            ..
        } = method;
        if descriptor.client_streaming || descriptor.server_streaming {
            quote! {
                routes.push(
                    #path,
                    #routes_enum::#variant(::sark_grpc::ServiceStreaming::new(
                        #wrapper,
                        ::sark_grpc::ProstCodec::<#response, #request>::new(),
                    )),
                );
            }
        } else {
            quote! {
                routes.push(
                    #path,
                    #routes_enum::#variant(::sark_grpc::ServiceUnary::new(
                        #wrapper,
                        ::sark_grpc::ProstCodec::<#response, #request>::new(),
                    )),
                );
            }
        }
    });
    let client = emit_client(&methods, &client_name);

    quote! {
        pub trait #trait_name: 'static {
            #(#trait_methods)*
        }

        #(#server_wrappers)*

        #routes

        pub fn #routes_fn<S: #trait_name>(
            service: S,
        ) -> ::sark_grpc::ServiceRoutes<S, #routes_enum<S>> {
            let mut routes = ::sark_grpc::ServiceRoutes::new(service);
            #(#registrations)*
            routes
        }

        #client
    }
}

struct GeneratedMethod<'a> {
    descriptor: &'a Method,
    name: Ident,
    variant: Ident,
    wrapper: Ident,
    request: Type,
    response: Type,
    path: Literal,
    open_name: Ident,
    send_name: Ident,
    finish_name: Ident,
    poll_name: Ident,
    decode_name: Ident,
}

impl<'a> GeneratedMethod<'a> {
    fn new(service: &Service, method: &'a Method, service_name: &Ident) -> Self {
        let name = rust_ident(&method.name, "method");
        let variant = generated_ident(&method.proto_name.to_upper_camel_case());
        let wrapper = format_ident!("__SarkGrpc{}{}", service_name, variant);
        let helper_stem = method.proto_name.to_snake_case();
        Self {
            descriptor: method,
            name,
            variant,
            wrapper,
            request: rust_type(&method.input_type, method, "input"),
            response: rust_type(&method.output_type, method, "output"),
            path: Literal::byte_string(grpc_path(service, method).as_bytes()),
            open_name: format_ident!("open_{helper_stem}"),
            send_name: format_ident!("send_{helper_stem}"),
            finish_name: format_ident!("finish_{helper_stem}"),
            poll_name: format_ident!("poll_{helper_stem}"),
            decode_name: format_ident!("decode_{helper_stem}"),
        }
    }
}

fn emit_trait_method(method: &GeneratedMethod<'_>) -> TokenStream {
    let GeneratedMethod {
        descriptor,
        name,
        request,
        response,
        ..
    } = method;
    if descriptor.client_streaming || descriptor.server_streaming {
        quote! {
            fn #name(
                &mut self,
                request: ::sark_grpc::StreamingRequest<#request>,
            ) -> ::sark_grpc::StreamingResponse<#response>;
        }
    } else {
        quote! {
            fn #name(
                &mut self,
                request: ::sark_grpc::UnaryRequest<#request>,
            ) -> ::sark_grpc::UnaryResponse<#response>;
        }
    }
}

fn emit_server_wrapper(method: &GeneratedMethod<'_>, trait_name: &Ident) -> TokenStream {
    let GeneratedMethod {
        descriptor,
        name,
        wrapper,
        request,
        response,
        ..
    } = method;
    if descriptor.client_streaming || descriptor.server_streaming {
        quote! {
            pub struct #wrapper;

            impl<S: #trait_name> ::sark_grpc::StreamingService<S> for #wrapper {
                type Request = #request;
                type Response = #response;
                type Codec = ::sark_grpc::ProstCodec<#response, #request>;

                fn stream(
                    &mut self,
                    service: &mut S,
                    request: ::sark_grpc::StreamingRequest<#request>,
                ) -> ::sark_grpc::StreamingResponse<#response> {
                    service.#name(request)
                }
            }
        }
    } else {
        quote! {
            pub struct #wrapper;

            impl<S: #trait_name> ::sark_grpc::UnaryService<S> for #wrapper {
                type Request = #request;
                type Response = #response;
                type Codec = ::sark_grpc::ProstCodec<#response, #request>;

                fn unary(
                    &mut self,
                    service: &mut S,
                    request: ::sark_grpc::UnaryRequest<#request>,
                ) -> ::sark_grpc::UnaryResponse<#response> {
                    service.#name(request)
                }
            }
        }
    }
}

fn emit_client(methods: &[GeneratedMethod<'_>], client_name: &Ident) -> TokenStream {
    let methods = methods.iter().map(emit_client_method);
    quote! {
        pub struct #client_name;

        impl #client_name {
            #(#methods)*
        }
    }
}

fn emit_client_method(method: &GeneratedMethod<'_>) -> TokenStream {
    let GeneratedMethod {
        descriptor,
        name,
        request,
        response,
        path,
        open_name,
        send_name,
        finish_name,
        poll_name,
        decode_name,
        ..
    } = method;

    let start = if descriptor.client_streaming {
        quote! {
            pub fn #name(
                session: &mut ::sark_grpc::Session,
                authority: ::core::option::Option<&[u8]>,
                metadata: &::sark_grpc::Metadata,
                messages: &[#request],
            ) -> ::core::result::Result<::sark_grpc::StreamId, ::sark_grpc::Status> {
                let mut codec = ::sark_grpc::ProstCodec::<#request, #response>::new();
                session.start_streaming(#path, authority, metadata, &mut codec, messages)
            }

            pub fn #open_name(
                session: &mut ::sark_grpc::Session,
                authority: ::core::option::Option<&[u8]>,
                metadata: &::sark_grpc::Metadata,
            ) -> ::core::result::Result<::sark_grpc::StreamId, ::sark_grpc::Status> {
                session.start_stream_raw(#path, authority, metadata)
            }

            pub fn #send_name(
                session: &mut ::sark_grpc::Session,
                stream_id: ::sark_grpc::StreamId,
                request: &#request,
            ) -> ::core::result::Result<(), ::sark_grpc::Status> {
                let mut codec = ::sark_grpc::ProstCodec::<#request, #response>::new();
                session.send_message(stream_id, &mut codec, request)
            }

            pub fn #finish_name(
                session: &mut ::sark_grpc::Session,
                stream_id: ::sark_grpc::StreamId,
            ) -> ::core::result::Result<(), ::sark_grpc::Status> {
                session.finish_send(stream_id)
            }
        }
    } else if descriptor.server_streaming {
        quote! {
            pub fn #name(
                session: &mut ::sark_grpc::Session,
                authority: ::core::option::Option<&[u8]>,
                metadata: &::sark_grpc::Metadata,
                request: &#request,
            ) -> ::core::result::Result<::sark_grpc::StreamId, ::sark_grpc::Status> {
                let stream_id = session.start_stream_raw(#path, authority, metadata)?;
                let mut codec = ::sark_grpc::ProstCodec::<#request, #response>::new();
                session.send_message(stream_id, &mut codec, request)?;
                session.finish_send(stream_id)?;
                ::core::result::Result::Ok(stream_id)
            }
        }
    } else {
        quote! {
            pub fn #name(
                session: &mut ::sark_grpc::Session,
                authority: ::core::option::Option<&[u8]>,
                metadata: &::sark_grpc::Metadata,
                request: &#request,
            ) -> ::core::result::Result<::sark_grpc::StreamId, ::sark_grpc::Status> {
                let mut codec = ::sark_grpc::ProstCodec::<#request, #response>::new();
                session.start_unary(#path, authority, metadata, &mut codec, request)
            }
        }
    };

    let decode = (!descriptor.server_streaming).then(|| {
        quote! {
            pub fn #decode_name(
                result: ::sark_grpc::UnaryResult,
            ) -> ::core::result::Result<#response, ::sark_grpc::Status> {
                let mut codec = ::sark_grpc::ProstCodec::<#request, #response>::new();
                result.decode_single(&mut codec)
            }
        }
    });

    quote! {
        #start
        #decode

        pub fn #poll_name(
            session: &mut ::sark_grpc::Session,
        ) -> ::core::option::Option<
            ::core::result::Result<
                ::sark_grpc::TypedStreamEvent<#response>,
                ::sark_grpc::Status,
            >,
        > {
            let event = session.poll_event()?;
            let mut codec = ::sark_grpc::ProstCodec::<#request, #response>::new();
            ::core::option::Option::Some(event.decode(&mut codec))
        }
    }
}

fn emit_routes_enum(
    methods: &[GeneratedMethod<'_>],
    trait_name: &Ident,
    routes_enum: &Ident,
) -> TokenStream {
    let variants = methods.iter().map(|method| {
        let GeneratedMethod {
            descriptor,
            variant,
            wrapper,
            ..
        } = method;
        if descriptor.client_streaming || descriptor.server_streaming {
            quote!(#variant(::sark_grpc::ServiceStreaming<S, #wrapper>),)
        } else {
            quote!(#variant(::sark_grpc::ServiceUnary<S, #wrapper>),)
        }
    });
    let start_arms = methods.iter().map(|method| {
        let variant = &method.variant;
        quote! {
            #routes_enum::#variant(handler) =>
                ::sark_grpc::server::ServiceHandler::start(
                    handler, service, routes, stream_id, head, reply,
                ),
        }
    });
    let message_arms = methods.iter().map(|method| {
        let variant = &method.variant;
        quote! {
            #routes_enum::#variant(handler) =>
                ::sark_grpc::server::ServiceHandler::message(
                    handler, service, routes, stream_id, message, reply,
                ),
        }
    });
    let trailers_arms = methods.iter().map(|method| {
        let variant = &method.variant;
        quote! {
            #routes_enum::#variant(handler) =>
                ::sark_grpc::server::ServiceHandler::trailers(
                    handler, service, routes, stream_id, trailers, reply,
                ),
        }
    });
    let request_arms = methods.iter().map(|method| {
        let variant = &method.variant;
        quote! {
            #routes_enum::#variant(handler) =>
                ::sark_grpc::server::ServiceHandler::request(
                    handler, service, request, response,
                ),
        }
    });

    quote! {
        pub enum #routes_enum<S: #trait_name> {
            #(#variants)*
        }

        impl<S: #trait_name> ::sark_grpc::server::ServiceHandler<S> for #routes_enum<S> {
            fn start(
                &mut self,
                service: &mut S,
                routes: &mut ::sark_grpc::server::StreamRoutes,
                stream_id: ::sark_grpc::StreamId,
                head: &::sark_grpc::RequestHead,
                reply: &mut ::sark_grpc::StreamReply,
            ) -> ::sark_grpc::StreamMode {
                match self {
                    #(#start_arms)*
                }
            }

            fn message(
                &mut self,
                service: &mut S,
                routes: &mut ::sark_grpc::server::StreamRoutes,
                stream_id: ::sark_grpc::StreamId,
                message: ::sark_grpc::MessageFrame,
                reply: &mut ::sark_grpc::StreamReply,
            ) {
                match self {
                    #(#message_arms)*
                }
            }

            fn trailers(
                &mut self,
                service: &mut S,
                routes: &mut ::sark_grpc::server::StreamRoutes,
                stream_id: ::sark_grpc::StreamId,
                trailers: ::sark_grpc::Metadata,
                reply: &mut ::sark_grpc::StreamReply,
            ) {
                match self {
                    #(#trailers_arms)*
                }
            }

            fn request(
                &mut self,
                service: &mut S,
                request: ::sark_grpc::Request<'_>,
                response: &mut ::sark_grpc::Response,
            ) {
                match self {
                    #(#request_arms)*
                }
            }
        }
    }
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

fn rust_ident(input: &str, kind: &str) -> Ident {
    syn::parse_str(input).unwrap_or_else(|error| {
        panic!("prost-build returned an invalid Rust {kind} name `{input}`: {error}")
    })
}

fn generated_ident(input: &str) -> Ident {
    syn::parse_str(input).unwrap_or_else(|_| Ident::new(&format!("{input}_"), Span::call_site()))
}

fn rust_type(input: &str, method: &Method, direction: &str) -> Type {
    syn::parse_str(input).unwrap_or_else(|error| {
        panic!(
            "prost-build returned an invalid Rust {direction} type `{input}` for RPC `{}`: {error}",
            method.proto_name,
        )
    })
}

#[cfg(test)]
mod tests {
    use prost_build::{Comments, Method, Service, ServiceGenerator};

    use super::SarkServiceGenerator;

    fn method(
        name: &str,
        proto_name: &str,
        client_streaming: bool,
        server_streaming: bool,
    ) -> Method {
        Method {
            name: name.into(),
            proto_name: proto_name.into(),
            comments: Comments::default(),
            input_type: "Input".into(),
            output_type: "Output".into(),
            input_proto_type: ".use.case.Input".into(),
            output_proto_type: ".use.case.Output".into(),
            options: Default::default(),
            client_streaming,
            server_streaming,
        }
    }

    fn generate(methods: Vec<Method>) -> String {
        let service = Service {
            name: "Proof".into(),
            proto_name: "Proof".into(),
            package: "use.case".into(),
            comments: Comments::default(),
            methods,
            options: Default::default(),
        };
        let mut generated = String::new();
        SarkServiceGenerator.generate(service, &mut generated);
        syn::parse_file(&generated).expect("generated service must be valid Rust syntax");
        generated
    }

    fn compact(source: &str) -> String {
        source.chars().filter(|ch| !ch.is_whitespace()).collect()
    }

    #[test]
    fn generated_routes_own_the_service_without_rc_or_dynamic_borrows() {
        let generated = generate(vec![method("echo", "Echo", false, false)]);
        let compact = compact(&generated);

        assert!(compact.contains("ServiceRoutes<S,__SarkGrpcProofRoutes<S>>"));
        assert!(compact.contains("ServiceUnary::new(__SarkGrpcProofEcho"));
        assert!(!generated.contains("Rc"));
        assert!(!generated.contains("RefCell"));
        assert!(!generated.contains("borrow_mut"));
        assert!(!generated.contains("shared.clone"));
    }

    #[test]
    fn every_streaming_shape_generates_valid_rust() {
        generate(vec![
            method("unary", "Unary", false, false),
            method("upload", "Upload", true, false),
            method("watch", "Watch", false, true),
            method("chat", "Chat", true, true),
        ]);
    }

    #[test]
    fn raw_method_identifiers_do_not_leak_into_composed_names() {
        let generated = compact(&generate(vec![method("r#type", "Type", false, false)]));

        assert!(generated.contains("fnr#type("));
        assert!(generated.contains("__SarkGrpcProofRoutes::Type("));
        assert!(generated.contains("fndecode_type("));
        assert!(generated.contains("fnpoll_type("));
        assert!(!generated.contains("R#type"));
    }
}
