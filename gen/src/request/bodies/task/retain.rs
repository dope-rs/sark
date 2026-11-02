use crate::{
    macros::tokens::local,
    request::bodies::task::{Output, Task, ready},
    runtime::paths::Paths,
};

pub(super) struct Retain<'a> {
    pub(super) task: Task<'a>,
    pub(super) segmented: bool,
}

impl Retain<'_> {
    pub(super) fn build(self, paths: &Paths) -> Output {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let [v_err, v_seg, v_store, v_len, v_obs, v_body, v_reject] = [
            "error",
            "segment",
            "__sark_body_storage",
            "body_len",
            "observer",
            "body",
            "rejected",
        ]
        .map(local);
        let step = self.step(paths);
        let (body, push_error) = if self.segmented {
            (
                quote::quote!(#ns_http::payload::owned::Segments),
                quote::quote!(#ns_http::payload::owned::flatten::Error::Assembly(#v_err)),
            )
        } else {
            (
                quote::quote!(#ns_http::payload::owned::Flatten),
                quote::quote!(#v_err),
            )
        };
        let wrap = if self.segmented {
            quote::quote!(#v_store)
        } else {
            quote::quote!(#body::new(#v_store))
        };
        let empty = if self.segmented {
            quote::quote!(#body::new())
        } else {
            quote::quote!(#body::new(#ns_http::payload::owned::Segments::new()))
        };
        let (error, reject) = match self.segmented {
            true => (
                quote::quote!(::core::convert::Infallible),
                quote::quote!(match #v_err {}),
            ),
            false => (
                quote::quote!(#ns_http::o3::buffer::storage::AllocError),
                quote::quote! {
                    {
                        use #ns_core::route::Reject;
                        use #ns_http::o3::buffer::storage;

                        match #v_err {
                            storage::AllocError::Exhausted(_) => Reject::Unavailable,
                            storage::AllocError::Capacity(_) => Reject::Internal,
                        }
                    }
                },
            ),
        };
        let own = self.task.own();
        let start = self.task.start(
            paths,
            &own,
            quote::quote!(::core::convert::Infallible),
            quote::quote!(match #v_err {}),
            quote::quote! {
                body: #wrap,
                input_finished: true,
            },
            quote::quote! {
                body: #empty,
                input_finished: false,
            },
        );
        let request = self.task.names.request;
        let builder = self.task.names.builder;
        Output {
            declaration: quote::quote! {
                body: #body<'d>,
                input_finished: ::core::primitive::bool,
            },
            definition: quote::quote! {
                impl<'d> #ns_core::route::TaskBuild<'d> for #builder<'d> {
                    type Task = #request<'d>;
                    type StepError = #error;

                    fn reject(#v_err: Self::StepError) -> #ns_core::route::Reject {
                        #reject
                    }

                    fn finish_body(mut self, #v_len: ::core::primitive::usize) -> Self {
                        self.body_len = #v_len;
                        self.input_finished = true;
                        self
                    }

                    #step
                }

                impl<'d> #ns_core::route::Push<'d> for #builder<'d> {
                    type Error = (
                        #ns_http::payload::owned::Segment<'d>,
                        #ns_http::payload::owned::flatten::Error,
                    );

                    fn observe<C: ::core::marker::Copy, S: ::core::marker::Copy, O: #ns_core::life::Observer<C, S>>(
                        #v_err: &Self::Error,
                        #v_obs: &mut O,
                    ) {
                        #v_obs.diagnostic(&#v_err.1);
                    }

                    fn reject_push(#v_err: Self::Error) -> #ns_core::route::Reject {
                        use #ns_core::route::Reject;
                        use #ns_http::o3::{alloc, buffer::segments::assembly};

                        let #v_err = match #v_err.1 {
                            #ns_http::payload::owned::flatten::Error::Finished => {
                                return Reject::BadRequest;
                            }
                            #ns_http::payload::owned::flatten::Error::Assembly(#v_err) => #v_err,
                        };
                        match #v_err {
                            assembly::Error::Metadata(alloc::Error::Exhausted)
                            | assembly::Error::Nodes {
                                allocation: alloc::Error::Exhausted, ..
                            }
                            | assembly::Error::Descriptors {
                                allocation: alloc::Error::Exhausted, ..
                            } => Reject::Unavailable,
                            assembly::Error::Metadata(alloc::Error::Overflow)
                            | assembly::Error::Nodes {
                                allocation: alloc::Error::Overflow, ..
                            }
                            | assembly::Error::Descriptors {
                                allocation: alloc::Error::Overflow, ..
                            }
                            | assembly::Error::Total(_) => Reject::Internal,
                        }
                    }

                    fn push_body(
                        mut self,
                        #v_seg: #ns_http::payload::owned::Segment<'d>,
                    ) -> #ns_core::body::Transfer<Self, Self::Error> {
                        if self.input_finished {
                            return #ns_core::body::Transfer::Rejected {
                                build: self,
                                error: (#v_seg, #ns_http::payload::owned::flatten::Error::Finished),
                            };
                        }
                        match self.body.try_push(#v_seg) {
                            ::core::result::Result::Ok(#v_body) => {
                                self.body = #v_body;
                                #ns_core::body::Transfer::Accepted(self)
                            }
                            ::core::result::Result::Err(#v_reject) => {
                                let ((#v_body, #v_seg), #v_err) = #v_reject.into_parts();
                                self.body = #v_body;
                                #ns_core::body::Transfer::Rejected {
                                    build: self,
                                    error: (#v_seg, #push_error),
                                }
                            }
                        }
                    }
                }
            },
            reject: quote::quote!(match #v_err {}),
            ty: quote::quote!(#builder<'d>),
            start,
        }
    }

    fn step(&self, paths: &Paths) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let [v_task, v_budget, v_body, v_credit, v_reject, v_err] = [
            "__sark_task_body",
            "__sark_budget",
            "body",
            "__sark_released",
            "rejected",
            "error",
        ]
        .map(local);
        let ready = ready(paths, &self.task);
        let parsed = match self.segmented {
            true => quote::quote! {
                let #v_task = #ns_http::request::Segments::new(self.id, self.body);
                #ready
            },
            false => quote::quote! {
                let #v_task = match self.body.try_step(#v_budget) {
                    ::core::result::Result::Ok(::core::ops::ControlFlow::Break(#v_body)) => {
                        #ns_http::request::Bytes::new(self.id, #v_body)
                    }
                    ::core::result::Result::Ok(::core::ops::ControlFlow::Continue(#v_body)) => {
                        self.body = #v_body;
                        return #ns_core::body::Step::Pending {
                            build: self,
                            released: #v_credit,
                        };
                    }
                    ::core::result::Result::Err(#v_reject) => {
                        let (#v_body, #v_err) = #v_reject.into_parts();
                        self.body = #v_body;
                        return #ns_core::body::Step::Failed {
                            build: self,
                            error: #v_err,
                            released: #v_credit,
                        };
                    }
                };
                #ready
            },
        };
        quote::quote! {
            fn step(
                mut self,
                #v_budget: ::core::num::NonZero<::core::primitive::usize>,
            ) -> #ns_core::body::Step<
                Self,
                #ns_core::route::Built<'d, Self::Task>,
                Self::StepError,
            > {
                let #v_credit = #ns_core::body::credit::Amount::new(
                    0,
                    #ns_http::o3::buffer::segments::Count::EMPTY,
                );
                if !self.input_finished {
                    return #ns_core::body::Step::NeedInput {
                        build: self,
                        released: #v_credit,
                    };
                }
                #parsed
            }
        }
    }
}
