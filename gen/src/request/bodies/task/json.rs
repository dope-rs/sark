use crate::{
    macros::tokens::local,
    request::bodies::task::{Output, Task, ready},
    runtime::paths::Paths,
};

pub(super) fn build(task: Task<'_>, paths: &Paths, json: &syn::Type) -> Output {
    let ns_core = &paths.core;
    let ns_http = &paths.http;
    let ns_json = &paths.json;
    let [
        v_body,
        v_store,
        v_id,
        v_err,
        v_len,
        v_budget,
        v_prog,
        v_credit,
    ] = [
        "body",
        "__sark_body_storage",
        "id",
        "error",
        "body_len",
        "budget",
        "progress",
        "__sark_released",
    ]
    .map(local);
    let [v_task, v_obs, v_size, v_add, v_count, v_seg, v_state] = [
        "__sark_task_body",
        "observer",
        "len",
        "add",
        "current",
        "segment",
        "state",
    ]
    .map(local);
    let ready = ready(paths, &task);
    let mut own = task.own();
    own.extend(quote::quote! {
        let #v_body = #ns_json::segment::Decoder::try_scoped(
            #v_store, #v_id,
        )
        .map_err(#ns_core::route::Input::Body)?;
    });
    let start = task.start(
        paths,
        &own,
        quote::quote!(#ns_json::segment::Create),
        quote::quote! {
            match #v_err {
                #ns_json::segment::Create::Exhausted => {
                    #ns_core::route::Reject::Unavailable
                }
                #ns_json::segment::Create::Overflow => {
                    #ns_core::route::Reject::Internal
                }
            }
        },
        quote::quote!(body: #v_body,),
        quote::quote! {
            body: #ns_json::segment::Decoder::try_feed(#v_id)
                .map_err(#ns_core::route::Input::Body)?,
        },
    );
    let request = task.names.request;
    let builder = task.names.builder;
    Output {
        declaration: quote::quote! {
            body: #ns_json::segment::Decoder<'d,
                <#json as #ns_json::segment::IntoRequest>::Request<'d>,
                #ns_http::request::Id<'d>>,
        },
        definition: quote::quote! {
            impl<'d> #ns_core::route::TaskBuild<'d> for #builder<'d> {
                type Task = #request<'d>;

                fn reject(#v_err: Self::StepError) -> #ns_core::route::Reject {
                    match #v_err {
                        #ns_json::parsing::Error::Missing(_) => {
                            #ns_core::route::Reject::BadRequest
                        }
                        #ns_json::parsing::Error::Parse(
                            #ns_json::Error::Invalid
                            | #ns_json::Error::Depth
                            | #ns_json::Error::Token(_),
                        ) => #ns_core::route::Reject::BadRequest,
                        #ns_json::parsing::Error::Parse(
                            #ns_json::Error::Storage(
                                #ns_json::Storage::Exhausted(_),
                            )
                            | #ns_json::Error::Allocation(
                                #ns_json::o3::alloc::Error::Exhausted,
                            )
                            | #ns_json::Error::Exhausted(_)
                            | #ns_json::Error::Retain(_),
                        ) => #ns_core::route::Reject::Unavailable,
                        #ns_json::parsing::Error::Parse(
                            #ns_json::Error::Storage(
                                #ns_json::Storage::Capacity(_),
                            )
                            | #ns_json::Error::Allocation(
                                #ns_json::o3::alloc::Error::Overflow,
                            )
                            | #ns_json::Error::InputLength { .. }
                            | #ns_json::Error::InputSegments { .. }
                            | #ns_json::Error::OutputLength(_),
                        ) => #ns_core::route::Reject::Internal,
                    }
                }
            }

            impl<'d> #ns_core::body::Build<'d> for #builder<'d> {
                type Output = #ns_core::route::Built<'d, #request<'d>>;
                type StepError = #ns_json::parsing::Error<'d>;

                fn finish_input(mut self, #v_len: ::core::primitive::usize) -> Self {
                    self.body_len = #v_len;
                    self.body = self.body.close();
                    self
                }

                fn step(
                    mut self,
                    #v_budget: ::core::num::NonZero<::core::primitive::usize>,
                ) -> #ns_core::body::Step<
                    Self,
                    Self::Output,
                    Self::StepError,
                > {
                    let #v_prog = self.body.poll(#v_budget);
                    let #v_credit = #ns_core::body::credit::Amount::new(
                        self.body.take_bytes(),
                        self.body.take_chunks(),
                    );
                    match #v_prog {
                        #ns_json::segment::Progress::Failed(#v_err) => {
                            #ns_core::body::Step::Failed {
                                build: self,
                                error: #v_err,
                                released: #v_credit,
                            }
                        }
                        #ns_json::segment::Progress::NeedInput => {
                            #ns_core::body::Step::NeedInput {
                            build: self,
                            released: #v_credit,
                            }
                        }
                        #ns_json::segment::Progress::Pending => #ns_core::body::Step::Pending {
                            build: self,
                            released: #v_credit,
                        },
                        #ns_json::segment::Progress::Ready(#v_task) => {
                            #ready
                        }
                    }
                }
            }

            impl<'d> #ns_core::route::Push<'d> for #builder<'d> {
                fn reject_push(#v_err: Self::Error) -> #ns_core::route::Reject {
                    match #v_err {
                        #ns_json::segment::Push::Complete(_)
                        | #ns_json::segment::Push::Length { .. }
                        | #ns_json::segment::Push::Segments { .. } => {
                            #ns_core::route::Reject::BadRequest
                        }
                        #ns_json::segment::Push::Allocation {
                            error: #ns_json::o3::alloc::Error::Exhausted, ..
                        } => #ns_core::route::Reject::Unavailable,
                        #ns_json::segment::Push::Allocation {
                            error: #ns_json::o3::alloc::Error::Overflow, ..
                        } => #ns_core::route::Reject::Internal,
                    }
                }
            }

            impl<'d> #ns_core::body::Push<'d> for #builder<'d> {
                type Error = #ns_json::segment::Push<'d>;

                fn observe<C: ::core::marker::Copy, S: ::core::marker::Copy, O: #ns_core::life::Observer<C, S>>(
                    #v_err: &Self::Error,
                    #v_obs: &mut O,
                ) {
                    match #v_err {
                        #ns_json::segment::Push::Complete(_) => {
                            #v_obs.diagnostic(&"complete");
                        }
                        #ns_json::segment::Push::Length { len: #v_size, add: #v_add, .. } => {
                            #v_obs.diagnostic(&("length", #v_size, #v_add));
                        }
                        #ns_json::segment::Push::Segments { current: #v_count, add: #v_add, .. } => {
                            #v_obs.diagnostic(&("segments", #v_count, #v_add));
                        }
                        #ns_json::segment::Push::Allocation { error: #v_err, .. } => {
                            #v_obs.diagnostic(#v_err);
                        }
                    }
                }

                fn push(
                    mut self,
                    #v_seg: #ns_http::payload::owned::Segment<'d>,
                ) -> #ns_core::body::Transfer<Self, Self::Error> {
                    match self.body.push(#v_seg.into()) {
                        #ns_json::segment::Transfer::Accepted(#v_body) => {
                            self.body = #v_body;
                            #ns_core::body::Transfer::Accepted(self)
                        }
                        #ns_json::segment::Transfer::Rejected { state: #v_state, error: #v_err } => {
                            self.body = #v_state;
                            #ns_core::body::Transfer::Rejected { build: self, error: #v_err }
                        }
                    }
                }
            }
        },
        reject: quote::quote! {
            <#builder<'req> as #ns_core::route::TaskBuild<'req>>::reject(#v_err)
        },
        ty: quote::quote!(#builder<'d>),
        start,
    }
}
