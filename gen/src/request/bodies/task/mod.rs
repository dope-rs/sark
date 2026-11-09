use crate::{macros::tokens::local, runtime::paths::Paths};

use super::Source;

mod json;
mod retain;

pub(in crate::request) struct Names<'tokens> {
    pub(in crate::request) request: &'tokens syn::Ident,
    pub(in crate::request) builder: &'tokens syn::Ident,
    pub(in crate::request) params: &'tokens proc_macro2::TokenStream,
    pub(in crate::request) headers: &'tokens proc_macro2::TokenStream,
}

pub(in crate::request) struct Output {
    pub(in crate::request) declaration: proc_macro2::TokenStream,
    pub(in crate::request) definition: proc_macro2::TokenStream,
    pub(in crate::request) reject: proc_macro2::TokenStream,
    pub(in crate::request) ty: proc_macro2::TokenStream,
    pub(in crate::request) start: proc_macro2::TokenStream,
}

fn ready(paths: &Paths, task: &Task<'_>) -> proc_macro2::TokenStream {
    let ns_core = &paths.core;
    let request = task.names.request;
    let fields = task.fields;
    let [v_auth, v_params, v_heads, v_len, v_id, v_credit] = [
        "__sark_authority",
        "params",
        "headers",
        "__sark_body_len",
        "__sark_id",
        "__sark_released",
    ]
    .map(local);
    let take = task.authority.map(|peer| {
        quote::quote! {
            let #v_auth: #peer = self.authority;
        }
    });
    quote::quote! {
        let #v_params = self.params;
        let #v_heads = self.headers;
        #take
        let #v_len = self.body_len;
        let #v_id = self.id;
        #ns_core::body::Step::Ready {
            output: #ns_core::route::Built::new(
                #v_id,
                #request { #(#fields,)* },
            ),
            released: #v_credit,
        }
    }
}

use retain::Retain;

use crate::request::params;

pub(in crate::request) struct Task<'a> {
    pub(in crate::request) names: Names<'a>,
    pub(in crate::request) authority: Option<&'a syn::Type>,
    pub(in crate::request) params: &'a params::Expansion,
    pub(in crate::request) head: &'a proc_macro2::TokenStream,
    pub(in crate::request) fields: &'a [proc_macro2::TokenStream],
    pub(in crate::request) vis: &'a syn::Visibility,
}

impl Task<'_> {
    pub(super) fn own(&self) -> proc_macro2::TokenStream {
        let head = self.head;
        let [id, request, body, storage, len] = [
            "id",
            "request",
            "body",
            "__sark_body_storage",
            "__sark_body_len",
        ]
        .map(local);
        quote::quote! {
            let #id = #request.owner().id();
            let (#request, #body) = #request.into_parts();
            #head
            let (#storage, #len) = #body.into_parts();
        }
    }

    pub(super) fn empty(&self) -> proc_macro2::TokenStream {
        let head = self.head;
        let [id, request, body, len] = ["id", "request", "body", "__sark_body_len"].map(local);
        quote::quote! {
            let #id = #request.owner().id();
            let (#request, #body) = #request.into_parts();
            #head
            let #len = #body.len();
        }
    }

    pub(super) fn start(
        &self,
        paths: &Paths,
        own: &proc_macro2::TokenStream,
        body_err: proc_macro2::TokenStream,
        body_rej: proc_macro2::TokenStream,
        own_add: proc_macro2::TokenStream,
        feed_add: proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let builder = self.names.builder;
        let [
            v_auth,
            v_owned,
            v_id,
            v_rqst,
            v_params,
            v_heads,
            v_len,
            v_err,
        ] = [
            "authority",
            "__sark_authority",
            "id",
            "request",
            "params",
            "headers",
            "__sark_body_len",
            "error",
        ]
        .map(local);
        let (frame, init) = match self.authority {
            Some(peer) => (
                quote::quote!(#ns_core::request::frame::proof::Peer),
                quote::quote! {
                    authority: {
                        let #v_auth: #peer = #v_owned;
                        #v_auth
                    },
                },
            ),
            None => (
                quote::quote!(#ns_core::request::frame::proof::Maybe),
                proc_macro2::TokenStream::new(),
            ),
        };
        let error = &self.params.error;
        let head = self.head;
        let reject = &self.params.reject;
        let feed = quote::quote! {
            let #v_id = #v_rqst.owner().id();
            #head
        };
        quote::quote! {
            type Error<'d> = #ns_core::route::Input<
                #error, #body_err, #ns_http::o3::mem::rc::Error,
            >;

            fn reject<'d>(
                #v_err: <Self as #ns_core::route::Own>::Error<'d>,
            ) -> #ns_core::route::Reject {
                match #v_err {
                    #ns_core::route::Input::Params(#v_err) => #reject,
                    #ns_core::route::Input::Body(#v_err) => #body_rej,
                    #ns_core::route::Input::Retain(
                        #ns_http::o3::mem::rc::Error::Exhausted,
                    ) => #ns_core::route::Reject::Unavailable,
                }
            }

            fn start_task<'d>(
                #v_rqst: #ns_core::request::raw::Parsed<
                    #ns_core::request::Retained<'d, #frame>, Self,
                >,
            ) -> ::core::result::Result<
                Self::TaskBuilder<'d>,
                <Self as #ns_core::route::Own>::Error<'d>,
            > {
                #own
                ::core::result::Result::Ok(#builder {
                    id: #v_id,
                    params: #v_params,
                    headers: #v_heads,
                    #init
                    body_len: #v_len,
                    #own_add
                })
            }

            fn start_stream<'d>(
                #v_rqst: #ns_core::request::raw::Parsed<
                    #ns_core::request::Parts<'d, #frame>, Self,
                >,
            ) -> ::core::result::Result<
                Self::TaskBuilder<'d>,
                <Self as #ns_core::route::Own>::Error<'d>,
            > {
                #feed
                ::core::result::Result::Ok(#builder {
                    id: #v_id,
                    params: #v_params,
                    headers: #v_heads,
                    #init
                    body_len: 0,
                    #feed_add
                })
            }
        }
    }

    pub(in crate::request) fn build(self, paths: &Paths, source: &Source) -> Output {
        let ns_http = &paths.http;
        let builder = self.names.builder;
        let params = self.names.params;
        let headers = self.names.headers;
        let field = self.authority.map(|peer| quote::quote!(authority: #peer,));
        let vis = self.vis;
        let mut output = match source {
            Source::Json(json) => json::build(self, paths, &json.ty),
            Source::Raw(raw) => Retain {
                task: self,
                segmented: raw.segmented,
            }
            .build(paths),
            Source::Discarded => self.discard(paths),
        };
        let fields = output.declaration;
        output.declaration = quote::quote! {
            #[doc(hidden)]
            #vis struct #builder<'d> {
                id: #ns_http::request::Id<'d>,
                params: #params,
                headers: #headers,
                #field
                body_len: ::core::primitive::usize,
                #fields
            }
        };
        output
    }

    fn discard(self, paths: &Paths) -> Output {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let [error, len, released] = ["error", "body_len", "__sark_released"].map(local);
        let ready = ready(paths, &self);
        let own = self.empty();
        let start = self.start(
            paths,
            &own,
            quote::quote!(::core::convert::Infallible),
            quote::quote!(match #error {}),
            quote::quote!(input_finished: true,),
            quote::quote!(input_finished: false,),
        );
        let request = self.names.request;
        let builder = self.names.builder;
        Output {
            declaration: quote::quote!(input_finished: ::core::primitive::bool,),
            definition: quote::quote! {
                impl<'d> #ns_core::route::TaskBuild<'d> for #builder<'d> {
                    type Task = #request<'d>;

                    fn reject(#error: Self::StepError) -> #ns_core::route::Reject {
                        match #error {}
                    }
                }

                impl<'d> #ns_core::body::Build<'d> for #builder<'d> {
                    type Output = #ns_core::route::Built<'d, #request<'d>>;
                    type StepError = ::core::convert::Infallible;

                    fn finish_input(mut self, #len: ::core::primitive::usize) -> Self {
                        self.body_len = #len;
                        self.input_finished = true;
                        self
                    }

                    fn step(
                        self,
                        _: ::core::num::NonZero<::core::primitive::usize>,
                    ) -> #ns_core::body::Step<
                        Self,
                        Self::Output,
                        Self::StepError,
                    > {
                        let #released =
                            #ns_core::body::credit::Amount::new(
                                0,
                                #ns_http::o3::buffer::segments::Count::EMPTY,
                            );
                        if !self.input_finished {
                            return #ns_core::body::Step::NeedInput {
                                build: self,
                                released: #released,
                            };
                        }
                        #ready
                    }
                }
            },
            reject: quote::quote!(match #error {}),
            ty: quote::quote!(#builder<'d>),
            start,
        }
    }
}
