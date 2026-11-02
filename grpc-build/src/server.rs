use crate::{Table, method};

/// Prefixes unused generated arguments with an underscore.
fn bound(name: &str, read: bool) -> proc_macro2::Ident {
    if read {
        quote::format_ident!("{}", name)
    } else {
        quote::format_ident!("_{}", name)
    }
}

pub(super) fn emit(
    methods: &[method::Method],
    service: &proc_macro2::Ident,
    routes: &proc_macro2::Ident,
    call: &proc_macro2::Ident,
) -> syn::Result<proc_macro2::TokenStream> {
    let table = Table::new(methods)?;
    let routed = !methods.is_empty();
    let stream = bound("stream", routed);
    let message = bound("message", routed);
    let trailers = bound("trailers", routed);
    let request = bound("request", routed);
    let output = bound("output", routed);
    let reply = bound("reply", routed);
    let variants = methods.iter().map(|method| {
        let variant = &method.variant;
        let wrapper = &method.wrapper;
        let adapter = method.adapter();
        quote::quote! {
            #variant {
                handler: #adapter<#wrapper, S>,
                call: <#adapter<#wrapper, S> as
                    ::sark_grpc::server::service::handler::Handler<S>>::Call,
            },
        }
    });
    let start = table.emit(
        quote::quote! {{
            reply.finish_with(::sark_grpc::Status::from_code(
                ::sark_grpc::Code::Unimplemented,
            ));
            ::sark_grpc::server::service::handler::Start::buffered(
                #call::default(),
            )
        }},
        |method| {
            let variant = &method.variant;
            let wrapper = &method.wrapper;
            let decode = &method.request;
            let encode = &method.response;
            let adapter = method.adapter();
            quote::quote! {{
                let mut handler = #adapter::new(
                    #wrapper,
                    ::sark_grpc::encode::Prost::<#encode, #decode>::new(),
                );
                let started = ::sark_grpc::server::service::handler::Handler::start(
                    &mut handler,
                    &mut self.service,
                    #stream,
                    head,
                    reply,
                );
                started.map(|call| #call::#variant { handler, call })
            }}
        },
    );
    let forward = |name: proc_macro2::TokenStream, args: proc_macro2::TokenStream| {
        methods
            .iter()
            .map(|method| {
                let variant = &method.variant;
                quote::quote! {
                    #call::#variant { handler, call } =>
                        ::sark_grpc::server::service::handler::Handler::#name(
                            handler, &mut self.service, call, #args
                        ),
                }
            })
            .collect::<proc_macro2::TokenStream>()
    };
    let messages = forward(
        quote::quote!(message),
        quote::quote!(#stream, #message, #reply,),
    );
    let trailing = forward(
        quote::quote!(trailers),
        quote::quote!(#stream, #trailers, #reply,),
    );
    let requests = forward(quote::quote!(request), quote::quote!(#request, response,));
    let outputs = forward(quote::quote!(output), quote::quote!(#output, reply,));

    Ok(quote::quote! {
        #[repr(transparent)]
        pub struct #routes<S: #service> {
            service: S,
        }

        #[doc(hidden)]
        pub enum #call<S: #service> {
            Unknown(::core::marker::PhantomData<fn(&mut S)>),
            #(#variants)*
        }

        impl<S: #service> ::core::default::Default for #call<S> {
            fn default() -> Self {
                Self::Unknown(::core::marker::PhantomData)
            }
        }

        impl<S: #service> ::sark_grpc::server::service::handler::Handler for #routes<S>
        {
            type Call = #call<S>;
            type Timer = ::sark_grpc::server::service::timers::Off;

            fn start<W: ::sark_grpc::server::service::reply::Write>(
                &mut self,
                &mut (): &mut (),
                #stream: &mut ::sark_grpc::server::service::Stream,
                head: &::sark_grpc::protocol::request::Head,
                reply: &mut ::sark_grpc::server::service::reply::Reply<'_, W>,
            ) -> ::sark_grpc::server::service::handler::Start<Self::Call> {
                let __grpc_path = head.path();
                #start
            }

            fn message<
                P: ::sark_grpc::o3::buffer::raw::Segment,
                W: ::sark_grpc::server::service::reply::Write,
            >(
                &mut self,
                &mut (): &mut (),
                call: &mut Self::Call,
                #stream: &mut ::sark_grpc::server::service::Stream,
                #message: ::sark_grpc::message::Frame<P>,
                #reply: &mut ::sark_grpc::server::service::reply::Reply<'_, W>,
            ) {
                match call {
                    #messages
                    #call::Unknown(_) => {}
                }
            }

            fn trailers<W: ::sark_grpc::server::service::reply::Write>(
                &mut self,
                &mut (): &mut (),
                call: &mut Self::Call,
                #stream: &mut ::sark_grpc::server::service::Stream,
                #trailers: ::sark_grpc::metadata::Decoded,
                #reply: &mut ::sark_grpc::server::service::reply::Reply<'_, W>,
            ) {
                match call {
                    #trailing
                    #call::Unknown(_) => {}
                }
            }

            fn request<
                P: ::sark_grpc::o3::buffer::raw::Segment,
                W: ::sark_grpc::server::service::reply::Write,
            >(
                &mut self,
                &mut (): &mut (),
                call: &mut Self::Call,
                #request: ::sark_grpc::server::service::Request<'_, P>,
                response: &mut ::sark_grpc::server::service::Response<'_, W>,
            ) -> ::sark_grpc::server::service::handler::RequestState {
                match call {
                    #requests
                    #call::Unknown(_) => {
                        response.finish_with(::sark_grpc::Status::from_code(
                            ::sark_grpc::Code::Unimplemented,
                        ));
                        ::sark_grpc::server::service::handler::RequestState::Complete
                    }
                }
            }

            fn output<W: ::sark_grpc::server::service::reply::Write>(
                &mut self,
                &mut (): &mut (),
                call: &mut Self::Call,
                #output: &mut ::sark_grpc::server::service::timers::Output<
                    '_,
                    '_,
                    '_,
                    Self::Timer,
                >,
                reply: &mut ::sark_grpc::server::service::reply::Reply<'_, W>,
            ) -> ::sark_grpc::server::service::handler::OutputState {
                match call {
                    #outputs
                    #call::Unknown(_) => {
                        reply.finish_with(::sark_grpc::Status::from_code(
                            ::sark_grpc::Code::Unimplemented,
                        ));
                        ::sark_grpc::server::service::handler::OutputState::Complete
                    }
                }
            }
        }
    })
}
