use std::mem;

pub(super) struct Method {
    pub(super) shape: Shape,
    pub(super) name: proc_macro2::Ident,
    pub(super) variant: proc_macro2::Ident,
    pub(super) wrapper: proc_macro2::Ident,
    stream: proc_macro2::Ident,
    pub(super) request: syn::Type,
    pub(super) response: syn::Type,
    pub(super) path: Box<[u8]>,
}

#[derive(Clone, Copy)]
pub(super) enum Shape {
    Unary,
    Client,
    Server,
    Bidi,
}

impl Method {
    pub(super) fn new(
        schema: &prost_build::Service,
        method: &prost_build::Method,
        prefix: &proc_macro2::Ident,
        index: usize,
    ) -> syn::Result<Self> {
        let name = syn::parse_str(&method.name)?;
        let stream = crate::names::suffixed(
            &method.proto_name,
            heck::ToUpperCamelCase::to_upper_camel_case(method.proto_name.as_str()),
            "Stream",
        )?;
        let variant = quote::format_ident!("Method{}", index);
        let wrapper = quote::format_ident!("{}Method{}", prefix, index);
        let path = if schema.package.is_empty() {
            format!("/{}/{}", schema.proto_name, method.proto_name)
        } else {
            format!(
                "/{}.{}/{}",
                schema.package, schema.proto_name, method.proto_name
            )
        };
        Ok(Self {
            shape: Shape::new(method.client_streaming, method.server_streaming),
            name,
            variant,
            wrapper,
            stream,
            request: syn::parse_str(&method.input_type)?,
            response: syn::parse_str(&method.output_type)?,
            path: path.into_bytes().into_boxed_slice(),
        })
    }

    pub(super) fn scope(&mut self, names: &crate::names::Names) -> proc_macro2::TokenStream {
        let request = names.ident(&format!("{}Input", self.wrapper));
        let response = names.ident(&format!("{}Output", self.wrapper));
        let input = mem::replace(&mut self.request, syn::parse_quote!(super::#request));
        let output = mem::replace(&mut self.response, syn::parse_quote!(super::#response));
        quote::quote! {
            type #request = #input;
            type #response = #output;
        }
    }

    pub(super) fn adapter(&self) -> proc_macro2::TokenStream {
        match self.shape {
            Shape::Bidi => quote::quote!(::sark_grpc::server::service::handler::output::Duplex),
            Shape::Server => {
                quote::quote!(::sark_grpc::server::service::handler::output::Streaming)
            }
            Shape::Client => quote::quote!(::sark_grpc::server::service::handler::modes::Streaming),
            Shape::Unary => quote::quote!(::sark_grpc::server::service::handler::modes::Unary),
        }
    }

    pub(super) fn emit_trait(&self) -> proc_macro2::TokenStream {
        let name = &self.name;
        let request = &self.request;
        let response = &self.response;
        let stream = &self.stream;
        match self.shape {
            Shape::Bidi => quote::quote! {
                type #stream: ::sark_grpc::server::service::handler::output::Bidi<
                    ::sark_grpc::encode::Prost<#response, #request>,
                    Response = #response,
                    Timer = ::sark_grpc::server::service::timers::Off,
                >;

                fn #name(
                    &mut self,
                    request: ::sark_grpc::server::service::handler::output::BidiRequest<'_>,
                ) -> ::sark_grpc::server::service::handler::output::Response<Self::#stream>;
            },
            Shape::Server => quote::quote! {
                type #stream: ::sark_grpc::server::service::handler::output::Stream<
                    Response = #response,
                    Timer = ::sark_grpc::server::service::timers::Off,
                >;

                fn #name(
                    &mut self,
                    request: ::sark_grpc::server::service::handler::modes::Request<#request>,
                ) -> ::sark_grpc::server::service::handler::output::Response<Self::#stream>;
            },
            Shape::Client => quote::quote! {
                fn #name<P: ::sark_grpc::o3::buffer::raw::Segment>(
                    &mut self,
                    request: ::sark_grpc::server::service::handler::modes::Request<
                        ::sark_grpc::server::service::handler::modes::Messages<
                            '_,
                            '_,
                            ::sark_grpc::encode::Prost<#response, #request>,
                            P,
                        >,
                    >,
                ) -> ::sark_grpc::server::service::handler::modes::Response<#response>;
            },
            Shape::Unary => quote::quote! {
                fn #name(
                    &mut self,
                    request: ::sark_grpc::server::service::handler::modes::Request<#request>,
                ) -> ::sark_grpc::server::service::handler::modes::Response<#response>;
            },
        }
    }

    pub(super) fn emit_wrapper(&self, service: &proc_macro2::Ident) -> proc_macro2::TokenStream {
        let name = &self.name;
        let wrapper = &self.wrapper;
        let request = &self.request;
        let response = &self.response;
        let stream = &self.stream;
        let body = match self.shape {
            Shape::Bidi => quote::quote! {
                impl<S: #service> ::sark_grpc::server::service::handler::output::BidiFn<S>
                    for #wrapper
                {
                    type Response = #response;
                    type Stream = S::#stream;
                    type Codec = ::sark_grpc::encode::Prost<#response, #request>;

                    fn bidi(
                        &mut self,
                        context: &mut S,
                        request: ::sark_grpc::server::service::handler::output::BidiRequest<'_>,
                    ) -> ::sark_grpc::server::service::handler::output::Response<Self::Stream> {
                        context.#name(request)
                    }
                }
            },
            Shape::Server => quote::quote! {
                impl<S: #service> ::sark_grpc::server::service::handler::output::StreamFn<S>
                    for #wrapper
                {
                    type Response = #response;
                    type Stream = S::#stream;
                    type Codec = ::sark_grpc::encode::Prost<#response, #request>;

                    fn stream<'a, P: ::sark_grpc::o3::buffer::raw::Segment + 'a>(
                        &mut self,
                        context: &mut S,
                        request: ::sark_grpc::server::service::handler::modes::Request<
                            <Self::Codec as ::sark_grpc::Codec>::Decode<'a, P>,
                        >,
                    ) -> ::sark_grpc::server::service::handler::output::Response<Self::Stream> {
                        context.#name(request)
                    }
                }
            },
            Shape::Client => quote::quote! {
                impl<S: #service> ::sark_grpc::server::service::handler::modes::StreamingFn<S>
                    for #wrapper
                {
                    type Response = #response;
                    type Codec = ::sark_grpc::encode::Prost<#response, #request>;

                    fn stream<P: ::sark_grpc::o3::buffer::raw::Segment>(
                        &mut self,
                        context: &mut S,
                        request: ::sark_grpc::server::service::handler::modes::Request<
                            ::sark_grpc::server::service::handler::modes::Messages<
                                '_,
                                '_,
                                Self::Codec,
                                P,
                            >,
                        >,
                    ) -> ::sark_grpc::server::service::handler::modes::Response<#response> {
                        context.#name(request)
                    }
                }
            },
            Shape::Unary => quote::quote! {
                impl<S: #service> ::sark_grpc::server::service::handler::modes::UnaryFn<S>
                    for #wrapper
                {
                    type Response = #response;
                    type Codec = ::sark_grpc::encode::Prost<#response, #request>;

                    fn unary<'a, P: ::sark_grpc::o3::buffer::raw::Segment + 'a>(
                        &mut self,
                        context: &mut S,
                        request: ::sark_grpc::server::service::handler::modes::Request<
                            <Self::Codec as ::sark_grpc::Codec>::Decode<'a, P>,
                        >,
                    ) -> ::sark_grpc::server::service::handler::modes::Response<#response> {
                        context.#name(request)
                    }
                }
            },
        };
        quote::quote! {
            pub struct #wrapper;

            #body
        }
    }
}

impl Shape {
    const fn new(client: bool, server: bool) -> Self {
        match (client, server) {
            (false, false) => Self::Unary,
            (true, false) => Self::Client,
            (false, true) => Self::Server,
            (true, true) => Self::Bidi,
        }
    }
}
