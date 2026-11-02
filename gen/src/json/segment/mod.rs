use crate::{
    json::{field, plan, scalar},
    macros::tokens,
    runtime::paths::Paths,
    types,
};

pub(super) mod task;

pub(super) fn decode(
    paths: &Paths,
    name: &syn::Ident,
    vis: &syn::Visibility,
    fields: &[plan::JsonField],
    order: proc_macro2::TokenStream,
    schema: Vec<proc_macro2::TokenStream>,
    marker: &syn::Ident,
) -> syn::Result<proc_macro2::TokenStream> {
    let owned = emit(paths, name, vis, fields, &order, &schema, None)?;
    let request = emit(paths, name, vis, fields, &order, &schema, Some(marker))?;
    Ok(quote::quote!(#owned #request))
}

fn emit(
    paths: &Paths,
    name: &syn::Ident,
    vis: &syn::Visibility,
    fields: &[plan::JsonField],
    order: &proc_macro2::TokenStream,
    schema: &[proc_macro2::TokenStream],
    marker: Option<&syn::Ident>,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_json = &paths.json;
    let ns_http = &paths.http;
    let [v_doc, v_ctx, v_budget] = ["__document", "__context", "__budget"].map(tokens::local);
    let [v_left, v_prefix, v_state] = ["__remaining", "__prefix", "__state"].map(tokens::local);
    let [v_proj, v_worker, v_input] = ["__project", "__worker", "__input"].map(tokens::local);
    let [v_next, v_rel, v_error] = ["__next", "__release", "__error"].map(tokens::local);
    let request = marker.is_some();
    let value = if request {
        quote::format_ident!("{}Scoped", name)
    } else {
        name.clone()
    };
    let (imp, target, lives, context) = if request {
        (
            quote::quote!(<'req>),
            quote::quote!(#value<'req>),
            quote::quote!(<'d, 'req>),
            quote::quote!(#ns_http::request::Id<'req>),
        )
    } else {
        (
            quote::quote!(),
            quote::quote!(#value),
            quote::quote!(<'d>),
            quote::quote!(()),
        )
    };
    let marker = marker.map(|marker| quote::quote!(#marker: ::core::marker::PhantomData,));
    if fields.is_empty() {
        return Ok(quote::quote! {
            impl #imp #ns_json::segment::Decode<#context> for #target {
                type State<'d> = #ns_json::segment::Ready<'d, Self>;

                const ORDER: #ns_json::segment::Order = #order;

                fn fields<'d>() -> &'d [#ns_json::segment::Field<'d>] {
                    const FIELDS: &[#ns_json::segment::Field<'_>] = &[#(#schema,)*];
                    FIELDS
                }

                fn start<'d>(
                    #v_doc: #ns_json::segment::Document<'d>,
                    #v_ctx: #context,
                ) -> Self::State<'d> {
                    #ns_json::segment::Ready::new(
                        Self { #marker },
                        #v_doc,
                    )
                }

                fn release(
                    &mut self,
                    #v_budget: &mut ::core::primitive::usize,
                ) -> ::core::task::Poll<()> {
                    ::core::task::Poll::Ready(())
                }
            }
        });
    }

    let state = quote::format_ident!("__{}SegmentedState", value);
    let phase = quote::format_ident!("__{}SegmentedPhase", value);
    let input = quote::format_ident!("__{}SegmentedInput", value);
    let compound = fields
        .iter()
        .any(|field| field.mode.nested || field.mode.seq);
    let admitted = fields
        .iter()
        .any(|field| field.mode.nested && !field.mode.seq);
    let admitted_variant = admitted.then(|| {
        quote::quote! {
            Admitted(#ns_json::segment::Document<'d>, #ns_json::segment::Admitted<'d>),
        }
    });
    let released_pattern = if admitted {
        quote::quote!(#v_state @ (#input::Released | #input::Admitted(..)))
    } else {
        quote::quote!(#v_state @ #input::Released)
    };
    let admitted_release = admitted.then(|| {
        quote::quote! {
            #input::Admitted(#v_doc, #v_input) => {
                if #v_input.release(#v_budget).is_pending()
                    || #v_doc.retire(#v_budget).is_pending()
                {
                    return ::core::task::Poll::Pending;
                }
            }
        }
    });
    let worker = compound.then(|| {
        quote::quote! {
            enum #input<'d, W> {
                Document(#ns_json::segment::Document<'d>),
                Project(#ns_json::segment::Project<'d>),
                Worker(#ns_json::segment::Document<'d>, W),
                #admitted_variant
                Released,
            }
        }
    });
    let phases = (0..fields.len())
        .map(|index| quote::format_ident!("Field{index}"))
        .collect::<Vec<_>>();
    let first = phases.first().ok_or_else(|| {
        syn::Error::new_spanned(name, "segmented state requires one materialized field")
    })?;
    let start = if fields[0].mode.nested || fields[0].mode.seq {
        quote::quote!(#phase::#first { input: #input::Document(#v_doc), prefix: () })
    } else {
        quote::quote!(#phase::#first { input: #v_doc, prefix: () })
    };
    let stored = fields
        .iter()
        .map(|field| {
            let ty = if request {
                field
                    .mode
                    .task_type(paths, &field.ty, &syn::parse_quote!('req))?
            } else {
                field.ty.clone()
            };
            field::proof_type(paths, &ty, field.mode)
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let scope = if compound
        || stored.iter().any(|ty| {
            types::Lifetimes::new(ty)
                .named()
                .iter()
                .any(|lifetime| lifetime.ident == "req")
        }) {
        lives.clone()
    } else {
        quote::quote!(<'d>)
    };
    let slots = fields.iter().map(|field| &field.bind).collect::<Vec<_>>();
    let finals = fields.iter().map(|field| {
        let ident = &field.ident;
        let slot = &field.bind;
        quote::quote!(#ident: #slot)
    });

    let mut variants = Vec::with_capacity(fields.len());
    let mut steps = Vec::with_capacity(fields.len());
    let mut releases = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        let current = &phases[index];
        let stage = quote::quote!(#phase::#current);
        let prefix = &stored[..index];
        let kept = &slots[..index];
        let values = &slots[..=index];
        let next = match phases.get(index + 1) {
            Some(next) if fields[index + 1].mode.nested || fields[index + 1].mode.seq => {
                quote::quote!(#phase::#next { input: #input::Document(#v_doc), prefix: (#(#values,)*) })
            }
            Some(next) => {
                quote::quote!(#phase::#next { input: #v_doc, prefix: (#(#values,)*) })
            }
            None => {
                quote::quote!(#phase::Done { input: #v_doc.release(), prefix: (#(#values,)*) })
            }
        };
        let next = quote::quote!({
            let (#(#kept,)*) = #v_prefix;
            #next
        });
        let slot = &field.bind;
        let release = fields[..index]
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let index = syn::Index::from(index);
                release_value(
                    paths,
                    field,
                    quote::quote!(#v_prefix.#index),
                    request,
                    &context,
                )
            })
            .collect::<syn::Result<Vec<_>>>()?;
        let job = if field.mode.seq {
            let elem = types::View::new(&field.ty).vector().ok_or_else(|| {
                syn::Error::new_spanned(&field.ty, "#[field(seq)] requires a Vec<T> field")
            })?;
            Some(if field.mode.nested {
                let ty = if request {
                    quote::quote!(<#elem as #ns_json::segment::IntoRequest>::Request<'req>)
                } else {
                    quote::quote!(#elem)
                };
                (
                    quote::quote!(#ns_json::segment::Nested<'d, #ty, #context>),
                    quote::quote!(#ns_json::segment::Nested::admitted(#v_input, self.__context)),
                )
            } else {
                match scalar::Classified::of(elem)?.scalar {
                    scalar::Scalar::Shared => {
                        let proof = field::proof_name(field.mode);
                        (
                            quote::quote!(#ns_json::segment::frames::Frames<'d, #context, #ns_json::#proof<#ns_json::o3::buffer::storage::Shared>>),
                            quote::quote!(#ns_json::segment::frames::Frames::admitted(#v_input, self.__context)),
                        )
                    }
                    scalar::Scalar::U64
                    | scalar::Scalar::I64
                    | scalar::Scalar::F64
                    | scalar::Scalar::Bool
                    | scalar::Scalar::String
                    | scalar::Scalar::Borrowed
                    | scalar::Scalar::Bytes
                    | scalar::Scalar::Token => {
                        return Err(syn::Error::new_spanned(
                            elem,
                            "unsupported JSON sequence field type",
                        ));
                    }
                }
            })
        } else if field.mode.nested {
            let ty = &field.ty;
            let ty = if request {
                quote::quote!(<#ty as #ns_json::segment::IntoRequest>::Request<'req>)
            } else {
                quote::quote!(#ty)
            };
            Some((
                quote::quote!(::std::boxed::Box<#ns_json::segment::Decoder<'d, #ty, #context>>),
                quote::quote!(#ns_json::segment::Decoder::try_admitted(&mut #v_input, self.__context)),
            ))
        } else {
            None
        };
        if let Some((job_ty, job_new)) = job {
            variants.push(
                quote::quote!(#current { input: #input<'d, #job_ty>, prefix: (#(#prefix,)*) }),
            );
            let name = &field.name;
            let mutable = (!field.mode.seq).then(|| quote::quote!(mut));
            let reject_document = quote::quote! {
                self.__stage = #stage { input: #input::Document(#v_doc), prefix: #v_prefix };
                return self.fail(#v_error.into());
            };
            let poll = if field.mode.seq {
                quote::quote! {
                    match #v_worker.step(#v_left) {
                        #ns_json::segment::Outcome::Ready(#slot) => {
                            self.__stage = #next;
                        }
                        #ns_json::segment::Outcome::Pending(#v_worker) => {
                            self.__stage = #stage { input: #input::Worker(#v_doc, #v_worker), prefix: #v_prefix };
                        }
                        #ns_json::segment::Outcome::Failed(#v_error) => { #reject_document }
                    }
                }
            } else {
                quote::quote! {
                    match #v_worker.poll(#v_left) {
                        #ns_json::segment::Progress::Ready(#slot) => {
                            self.__stage = #next;
                        }
                        #ns_json::segment::Progress::Pending => {
                            self.__stage = #stage { input: #input::Worker(#v_doc, #v_worker), prefix: #v_prefix };
                        }
                        #ns_json::segment::Progress::NeedInput => {
                            self.__stage = #stage { input: #input::Worker(#v_doc, #v_worker), prefix: #v_prefix };
                            return self.fail(#ns_json::Error::Invalid.into());
                        }
                        #ns_json::segment::Progress::Failed(#v_error) => {
                            self.__stage = #stage { input: #input::Worker(#v_doc, #v_worker), prefix: #v_prefix };
                            return self.fail(#v_error);
                        }
                    }
                }
            };
            let job_new = if field.mode.seq {
                job_new
            } else {
                quote::quote! {
                    match #job_new {
                        ::core::result::Result::Ok(#v_worker) => #v_worker,
                        ::core::result::Result::Err(#v_error) => {
                            self.__stage = #stage { input: #input::Admitted(#v_doc, #v_input), prefix: #v_prefix };
                            return self.fail(#v_error.into());
                        }
                    }
                }
            };
            steps.push(quote::quote! {
                #stage { prefix: #v_prefix, input: #v_state } => {
                    let #v_proj = match #v_state {
                        #input::Worker(#v_doc, #mutable #v_worker) => {
                            #poll
                            return #ns_json::segment::Outcome::Pending(self);
                        }
                        #input::Project(#v_proj) => #v_proj,
                        #input::Document(mut #v_doc) => {
                            match #v_doc.project_at(#index) {
                                ::core::result::Result::Ok(::core::option::Option::Some(#v_proj)) => #v_proj,
                                ::core::result::Result::Ok(::core::option::Option::None) => {
                                    let #v_error = #ns_json::parsing::Error::Missing(#name);
                                    #reject_document
                                }
                                ::core::result::Result::Err(#v_error) => { #reject_document }
                            }
                        }
                        #released_pattern => {
                            self.__stage = #stage { input: #v_state, prefix: #v_prefix };
                            return #ns_json::segment::Outcome::Pending(self);
                        }
                    };
                    match #v_proj.step(#v_left) {
                        #ns_json::segment::Outcome::Ready((#v_doc, #mutable #v_input)) => {
                            let #v_worker = #job_new;
                            self.__stage = #stage { input: #input::Worker(#v_doc, #v_worker), prefix: #v_prefix };
                        }
                        #ns_json::segment::Outcome::Pending(#v_proj) => {
                            self.__stage = #stage { input: #input::Project(#v_proj), prefix: #v_prefix };
                        }
                        #ns_json::segment::Outcome::Failed(#v_error) => {
                            self.__stage = #stage { input: #input::Released, prefix: #v_prefix };
                            return self.fail(#v_error);
                        }
                    }
                    return #ns_json::segment::Outcome::Pending(self);
                }
            });
            releases.push(quote::quote! {
                #stage { input: #v_state, prefix: #v_prefix } => {
                    match #v_state {
                        #input::Document(#v_doc) => {
                            if #v_doc.retire(#v_budget).is_pending() {
                                return ::core::task::Poll::Pending;
                            }
                        }
                        #input::Project(#v_proj) => {
                            if #v_proj.release(#v_budget).is_pending() {
                                return ::core::task::Poll::Pending;
                            }
                        }
                        #input::Worker(#v_doc, #v_worker) => {
                            if #v_worker.release(#v_budget).is_pending()
                                || #v_doc.retire(#v_budget).is_pending()
                            {
                                return ::core::task::Poll::Pending;
                            }
                        }
                        #admitted_release
                        #input::Released => {}
                    }
                    #(#release)*
                }
            });
        } else {
            variants.push(quote::quote!(#current { input: #ns_json::segment::Document<'d>, prefix: (#(#prefix,)*) }));
            let value = scalar_value(paths, field, index, request, quote::quote!(self.__context))?;
            steps.push(quote::quote! {
                #stage { prefix: #v_prefix, input: #v_doc } => {
                    let #slot = match (|| -> ::core::result::Result<_, #ns_json::parsing::Error<'d>> {
                        ::core::result::Result::Ok(#value)
                    })() {
                        ::core::result::Result::Ok(#slot) => #slot,
                        ::core::result::Result::Err(#v_error) => {
                            self.__stage = #stage { input: #v_doc, prefix: #v_prefix };
                            return self.fail(#v_error);
                        }
                    };
                    self.__stage = #next;
                    let ::core::option::Option::Some(#v_next) = ::core::num::NonZero::new(#v_left.get() - 1) else {
                        return #ns_json::segment::Outcome::Pending(self);
                    };
                    #v_left = #v_next;
                }
            });
            releases.push(quote::quote! {
                #stage { input: #v_doc, prefix: #v_prefix } => {
                    if #v_doc.retire(#v_budget).is_pending() {
                        return ::core::task::Poll::Pending;
                    }
                    #(#release)*
                }
            });
        }
    }
    let release_done = fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let index = syn::Index::from(index);
            release_value(
                paths,
                field,
                quote::quote!(#v_prefix.#index),
                request,
                &context,
            )
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let release_value = fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            release_value(paths, field, quote::quote!(self.#ident), request, &context)
        })
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(quote::quote! {
        enum #phase #scope {
            #(#variants,)*
            Done { input: #ns_json::segment::Release<'d>, prefix: (#(#stored,)*) },
            Released,
        }

        #worker

        #[doc(hidden)]
        #vis struct #state #lives {
            __context: #context,
            __stage: #phase #scope,
            __error: ::core::option::Option<::core::result::Result<(), #ns_json::parsing::Error<'d>>>,
        }

        impl #lives #state #lives {
            fn fail(
                mut self,
                #v_error: #ns_json::parsing::Error<'d>,
            ) -> #ns_json::segment::Outcome<'d, Self, #target> {
                self.__error = ::core::option::Option::Some(::core::result::Result::Err(#v_error));
                #ns_json::segment::Outcome::Pending(self)
            }
        }

        impl #lives #ns_json::segment::Materialize<'d, #target> for #state #lives {
            fn step(
                mut self,
                #v_budget: ::core::num::NonZero<::core::primitive::usize>,
            ) -> #ns_json::segment::Outcome<'d, Self, #target> {
                if self.__error.is_some() {
                    let mut #v_left = #v_budget.get();
                    if self.release(&mut #v_left).is_pending() {
                        return #ns_json::segment::Outcome::Pending(self);
                    }
                    return match self.__error {
                        ::core::option::Option::Some(::core::result::Result::Err(#v_error)) => #ns_json::segment::Outcome::Failed(#v_error),
                        ::core::option::Option::Some(::core::result::Result::Ok(()))
                        | ::core::option::Option::None => #ns_json::segment::Outcome::Pending(self),
                    };
                }
                let mut #v_left = #v_budget;
                loop {
                    match self.__stage {
                        #(#steps,)*
                        #phase::Done { prefix: #v_prefix, input: #v_rel } => {
                            return match #v_rel.step(#v_left) {
                                ::core::ops::ControlFlow::Continue(#v_rel) => {
                                    self.__stage = #phase::Done { input: #v_rel, prefix: #v_prefix };
                                    #ns_json::segment::Outcome::Pending(self)
                                }
                                ::core::ops::ControlFlow::Break(()) => {
                                    let (#(#slots,)*) = #v_prefix;
                                    #ns_json::segment::Outcome::Ready(
                                        #value { #(#finals,)* #marker }
                                    )
                                }
                            };
                        }
                        #phase::Released => return #ns_json::segment::Outcome::Pending(self),
                    }
                }
            }

            fn release(
                &mut self,
                #v_budget: &mut ::core::primitive::usize,
            ) -> ::core::task::Poll<()> {
                self.__error.get_or_insert(::core::result::Result::Ok(()));
                match &mut self.__stage {
                    #(#releases,)*
                    #phase::Done { input: #v_rel, prefix: #v_prefix } => {
                        if #v_rel.retire(#v_budget).is_pending() {
                            return ::core::task::Poll::Pending;
                        }
                        #(#release_done)*
                    }
                    #phase::Released => return ::core::task::Poll::Ready(()),
                }
                if *#v_budget == 0 {
                    return ::core::task::Poll::Pending;
                }
                *#v_budget -= 1;
                self.__stage = #phase::Released;
                ::core::task::Poll::Ready(())
            }
        }

        impl #imp #ns_json::segment::Decode<#context> for #target {
            type State<'d> = #state #lives;

            const ORDER: #ns_json::segment::Order = #order;

            fn fields<'d>() -> &'d [#ns_json::segment::Field<'d>] {
                const FIELDS: &[#ns_json::segment::Field<'_>] = &[#(#schema,)*];
                FIELDS
            }

            fn start<'d>(
                #v_doc: #ns_json::segment::Document<'d>,
                #v_ctx: #context,
            ) -> Self::State<'d> {
                #state {
                    __context: #v_ctx,
                    __stage: #start,
                    __error: ::core::option::Option::None,
                }
            }

            fn release(
                &mut self,
                #v_budget: &mut ::core::primitive::usize,
            ) -> ::core::task::Poll<()> {
                #(#release_value)*
                ::core::task::Poll::Ready(())
            }
        }
    })
}

fn release_value(
    paths: &Paths,
    field: &plan::JsonField,
    value: proc_macro2::TokenStream,
    request: bool,
    context: &proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_json = &paths.json;
    let [v_budget, v_values, v_value] = ["__budget", "__values", "__value"].map(tokens::local);
    let nested = if field.mode.nested {
        let ty = if field.mode.seq {
            types::View::new(&field.ty).vector().ok_or_else(|| {
                syn::Error::new_spanned(&field.ty, "#[field(seq)] requires a Vec<T> field")
            })?
        } else {
            &field.ty
        };
        let ty = if request {
            quote::quote!(<#ty as #ns_json::segment::IntoRequest>::Request<'req>)
        } else {
            quote::quote!(#ty)
        };
        quote::quote! {
            if <#ty as #ns_json::segment::Decode<#context>>::release(#v_value, #v_budget).is_pending() {
                return ::core::task::Poll::Pending;
            }
        }
    } else {
        proc_macro2::TokenStream::new()
    };
    Ok(if field.mode.seq {
        let remaining = field.mode.nested.then(|| {
            quote::quote! {
                if *#v_budget == 0 {
                    return ::core::task::Poll::Pending;
                }
            }
        });
        quote::quote! {
            {
                let #v_values = &mut #value;
                while let ::core::option::Option::Some(#v_value) = #v_values.last_mut() {
                    if *#v_budget == 0 {
                        return ::core::task::Poll::Pending;
                    }
                    #nested
                    #remaining
                    *#v_budget -= 1;
                    ::core::mem::drop(#v_values.pop());
                }
            }
        }
    } else if field.mode.nested {
        quote::quote! {
            {
                let #v_value = &mut #value;
                #nested
            }
        }
    } else {
        proc_macro2::TokenStream::new()
    })
}

fn scalar_value(
    paths: &Paths,
    field: &plan::JsonField,
    index: usize,
    request: bool,
    context: proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let ns_json = &paths.json;
    let [v_doc, v_value] = ["__document", "__value"].map(tokens::local);
    let name = &field.name;
    let class = scalar::Classified::of(&field.ty)?;
    let decode = match class.scalar {
        scalar::Scalar::U64 => quote::quote!(#v_value.u64()?),
        scalar::Scalar::Bool => quote::quote!(#v_value.boolean()?),
        scalar::Scalar::Shared => {
            if field.mode.raw {
                quote::quote!(#v_value.raw()?)
            } else if field.mode.plain {
                quote::quote!(#v_value.plain()?)
            } else {
                quote::quote!(#v_value.frame()?)
            }
        }
        scalar::Scalar::Token => {
            if field.mode.plain && !field.mode.raw {
                quote::quote!(#v_value.inline_plain()?)
            } else {
                quote::quote!(#v_value.inline_raw()?)
            }
        }
        scalar::Scalar::I64
        | scalar::Scalar::F64
        | scalar::Scalar::String
        | scalar::Scalar::Borrowed
        | scalar::Scalar::Bytes => {
            return Err(syn::Error::new_spanned(
                &field.ty,
                "unsupported segmented JSON field type",
            ));
        }
    };
    let decode =
        if matches!(class.scalar, scalar::Scalar::Token) && !field.mode.raw && !field.mode.plain {
            quote::quote!((#decode).into())
        } else {
            decode
        };
    let decode = if request && matches!(class.scalar, scalar::Scalar::Shared) {
        quote::quote!((#decode).into_request(#context))
    } else {
        decode
    };
    let value = if class.optional {
        quote::quote! {
            match #v_doc.field(#index)? {
                ::core::option::Option::Some(#v_value) if !#v_value.is_null() => {
                    ::core::option::Option::Some(#decode)
                }
                ::core::option::Option::Some(_)
                | ::core::option::Option::None => ::core::option::Option::None,
            }
        }
    } else {
        quote::quote! {
            let #v_value = #v_doc.field(#index)?.ok_or(
                #ns_json::parsing::Error::Missing(#name),
            )?;
            #decode
        }
    };
    Ok(quote::quote!({ #value }))
}
