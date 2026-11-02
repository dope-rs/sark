use crate::{
    app::{inputs, spec},
    runtime::paths::Paths,
};

mod duplex;
pub(in crate::app::serve) mod layout;
mod task;

pub(in crate::app::serve) use task::{Task, TaskPlan};

pub(in crate::app::serve) struct TaskSpec<'a> {
    capacity: &'a syn::Expr,
    slot: usize,
    path: layout::Path,
}

pub(in crate::app::serve) struct RouteSpec<'a, M> {
    pub(in crate::app::serve) entry: &'a spec::Entry,
    pub(in crate::app::serve) kind: spec::Kind,
    pub(in crate::app::serve) mode: M,
    head: syn::Ident,
    pending_variant: syn::Ident,
    pending_flow: Flow,
    prepared: syn::Ident,
    request: syn::Ident,
}

impl<'a, M> RouteSpec<'a, M> {
    fn new(
        owner: &syn::Ident,
        index: usize,
        entry: &'a spec::Entry,
        kind: spec::Kind,
        mode: M,
        flow: Flow,
    ) -> Self {
        Self {
            entry,
            kind,
            mode,
            head: quote::format_ident!("__{}Route{index:04}", owner),
            pending_variant: quote::format_ident!("R{index}"),
            pending_flow: flow,
            prepared: quote::format_ident!("R{index:04}"),
            request: quote::format_ident!("__{}Request{index:04}", owner),
        }
    }
}

enum Flow {
    Push,
    Discard,
    Retain,
}

pub(in crate::app::serve) enum Mode<'a> {
    Sync {
        cache: usize,
    },
    Fiber {
        task: TaskSpec<'a>,
        fiber: Fiber<'a>,
    },
    Stream {
        task: TaskSpec<'a>,
    },
    Duplex {
        family: duplex::Id,
    },
}

pub(in crate::app::serve) struct Fiber<'a> {
    body: &'a inputs::Body,
    slot: usize,
    future: syn::Ident,
    maker: syn::Ident,
    path: layout::Path,
}

impl TaskSpec<'_> {
    fn tag(&self, owner: &syn::Ident) -> syn::Ident {
        quote::format_ident!("__{}TaskTag{:04}", owner, self.slot)
    }

    fn variant(&self) -> syn::Ident {
        quote::format_ident!("Task{:04}", self.slot)
    }

    const fn path(&self) -> &layout::Path {
        &self.path
    }
}

impl Fiber<'_> {
    pub(in crate::app::serve) const fn path(&self) -> &layout::Path {
        &self.path
    }
}

impl Fiber<'_> {
    fn body_bound(&self, paths: &Paths, route: &syn::TypePath) -> proc_macro2::TokenStream {
        let ns_core = &paths.core;
        let ns_http = &paths.http;
        let ns_json = &paths.json;
        let (push, step) = match self.body {
            inputs::Body::Discarded => {
                return quote::quote! {
                    <<#route as #ns_core::route::Spec>::Request as
                        #ns_core::route::Own>::TaskBuilder<'d>:
                        #ns_core::route::TaskBuild<
                            'd,
                            StepError = ::core::convert::Infallible,
                        >,
                };
            }
            inputs::Body::Json => (
                quote::quote!(#ns_json::segment::Push<'d>),
                quote::quote!(#ns_json::parsing::Error<'d>),
            ),
            inputs::Body::Segmented => (
                quote::quote!((
                    #ns_http::payload::owned::Segment<'d>,
                    #ns_http::payload::owned::flatten::Error,
                )),
                quote::quote!(::core::convert::Infallible),
            ),
            inputs::Body::Contiguous => (
                quote::quote!((
                    #ns_http::payload::owned::Segment<'d>,
                    #ns_http::payload::owned::flatten::Error,
                )),
                quote::quote!(#ns_http::o3::buffer::storage::AllocError),
            ),
        };
        quote::quote! {
            <<#route as #ns_core::route::Spec>::Request as
                #ns_core::route::Own>::TaskBuilder<'d>:
                #ns_core::route::TaskBuild<
                    'd,
                    StepError = #step,
                > + #ns_core::route::Push<'d, Error = #push>,
        }
    }
}

pub(in crate::app::serve) struct Execution<'a, 'spec> {
    tasks: Vec<Task<'a, 'spec>>,
    fibers: Vec<(Task<'a, 'spec>, &'a Fiber<'spec>)>,
    streams: Vec<Task<'a, 'spec>>,
    duplexes: Vec<(&'a RouteSpec<'spec, Mode<'spec>>, duplex::Id)>,
    sync: usize,
    pub(in crate::app::serve) task: TaskPlan,
}

impl<'a, 'spec> Execution<'a, 'spec> {
    fn new(paths: &Paths, plan: &'a ProtocolPlan<'spec>) -> Self {
        let [g_id, g_store] = plan.spec.state_generics.idents(["__ID", "__P"]);
        let ns_fiber = &paths.fiber;
        let mut tasks = Vec::new();
        let mut fibers = Vec::new();
        let mut streams = Vec::new();
        let mut duplexes = Vec::new();
        let mut direct = Vec::new();
        let mut queued = Vec::new();
        let mut slabs = Vec::new();
        let mut sync = 0;
        let spec = plan.spec;
        let owner = &spec.name;
        for route in &plan.entries {
            match &route.mode {
                Mode::Sync { cache } => sync = *cache + 1,
                Mode::Fiber { task: spec, fiber } => {
                    let task = Task::new(route, spec);
                    let capacity = task.capacity();
                    let tag = task.tag(owner);
                    let (ty, bound) = task.store_fiber(paths, fiber, plan.spec.state.ty());
                    direct.push(quote::quote! {
                        #ns_fiber::dope_fiber::task::storage::fixed::staged::Slab<
                            'd,
                            #ty,
                            { #capacity },
                            #tag<#g_id>,
                            #ns_fiber::o3::depot::arena::slab::storage::Heap,
                        >
                    });
                    queued.push(quote::quote! {
                        #ns_fiber::dope_fiber::task::storage::fixed::schedule::Pool<
                            'd,
                            #ty,
                            { #capacity },
                            #tag<#g_id>,
                            <#g_store as #ns_fiber::manifold::runtime::Storage>::Target,
                        >
                    });
                    slabs.push(bound);
                    tasks.push(task);
                    fibers.push((task, fiber));
                }
                Mode::Stream { task: spec } => {
                    let task = Task::new(route, spec);
                    let capacity = task.capacity();
                    let tag = task.tag(owner);
                    let (ty, bound) = task.store_stream(paths);
                    direct.push(quote::quote! {
                        #ns_fiber::dope_fiber::task::storage::fixed::Slab<
                            'd,
                            #ty,
                            { #capacity },
                            #tag<#g_id>,
                            #ns_fiber::o3::depot::arena::slab::storage::Heap,
                        >
                    });
                    slabs.push(bound);
                    tasks.push(task);
                    streams.push(task);
                }
                Mode::Duplex { family } => duplexes.push((route, *family)),
            }
        }
        let task = TaskPlan::new(paths, spec, &fibers, direct, queued, slabs);
        Self {
            tasks,
            fibers,
            streams,
            duplexes,
            sync,
            task,
        }
    }

    pub(in crate::app::serve) fn tasks(&self) -> &[Task<'a, 'spec>] {
        &self.tasks
    }

    pub(in crate::app::serve) fn fibers(&self) -> &[(Task<'a, 'spec>, &'a Fiber<'spec>)] {
        &self.fibers
    }

    pub(in crate::app::serve) fn streams(&self) -> &[Task<'a, 'spec>] {
        &self.streams
    }

    pub(in crate::app::serve) fn duplexes(
        &self,
    ) -> &[(&'a RouteSpec<'spec, Mode<'spec>>, duplex::Id)] {
        &self.duplexes
    }

    pub(in crate::app::serve) fn producers(
        &self,
    ) -> (
        impl Iterator<Item = &'a syn::Ident> + Clone,
        impl Iterator<Item = &'a syn::Ident> + Clone,
    ) {
        let fibers = self.fibers.iter();
        (
            fibers.clone().map(|pair| &pair.1.future),
            fibers.map(|pair| &pair.1.maker),
        )
    }

    pub(in crate::app::serve) const fn sync_count(&self) -> usize {
        self.sync
    }
}

mod app;
pub(in crate::app) mod handle;

pub(in crate::app::serve) struct ProtocolPlan<'spec> {
    pub(in crate::app::serve) spec: &'spec spec::Spec,
    pub(in crate::app::serve) entries: Vec<RouteSpec<'spec, Mode<'spec>>>,
    pub(in crate::app::serve) duplex: duplex::Plan<'spec>,
}

impl<'spec> ProtocolPlan<'spec> {
    fn entries(
        paths: &Paths,
        spec: &'spec spec::Spec,
        duplex: &mut duplex::Plan<'spec>,
    ) -> Vec<RouteSpec<'spec, Mode<'spec>>> {
        let mut tasks = 0;
        let mut fibers = 0;
        for entry in spec.route_specs.iter() {
            match &entry.mode {
                spec::Mode::Fiber { .. } => {
                    tasks += 1;
                    fibers += 1;
                }
                spec::Mode::Stream { .. } => tasks += 1,
                spec::Mode::Sync | spec::Mode::Duplex => {}
            }
        }
        let mut cache = 0;
        let mut task = 0;
        let mut producer = 0;
        let owner = &spec.name;
        spec.route_specs
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let (kind, mode, flow) = match &entry.mode {
                    spec::Mode::Sync => {
                        let slot = cache;
                        cache += 1;
                        (spec::Kind::Sync, Mode::Sync { cache: slot }, Flow::Push)
                    }
                    spec::Mode::Fiber { capacity, body } => {
                        let tslot = task;
                        let pslot = producer;
                        task += 1;
                        producer += 1;
                        let flow = match body {
                            inputs::Body::Discarded => Flow::Discard,
                            inputs::Body::Json
                            | inputs::Body::Segmented
                            | inputs::Body::Contiguous => Flow::Retain,
                        };
                        (
                            spec::Kind::Fiber,
                            Mode::Fiber {
                                task: TaskSpec {
                                    capacity,
                                    slot: tslot,
                                    path: layout::Path::new(paths, tasks, tslot),
                                },
                                fiber: Fiber {
                                    body,
                                    path: layout::Path::new(paths, fibers, pslot),
                                    slot: pslot,
                                    future: spec.state_generics.ident(&format!("__F{pslot:04}")),
                                    maker: spec.state_generics.ident(&format!("__MK{pslot:04}")),
                                },
                            },
                            flow,
                        )
                    }
                    spec::Mode::Stream { capacity } => {
                        let slot = task;
                        task += 1;
                        (
                            spec::Kind::Stream,
                            Mode::Stream {
                                task: TaskSpec {
                                    capacity,
                                    slot,
                                    path: layout::Path::new(paths, tasks, slot),
                                },
                            },
                            Flow::Push,
                        )
                    }
                    spec::Mode::Duplex => {
                        let family = duplex.register(entry);
                        (spec::Kind::Duplex, Mode::Duplex { family }, Flow::Push)
                    }
                };
                RouteSpec::new(owner, index, entry, kind, mode, flow)
            })
            .collect()
    }

    pub(in crate::app::serve) fn route(
        &self,
        index: usize,
    ) -> Option<&RouteSpec<'spec, Mode<'spec>>> {
        self.entries.get(index)
    }

    fn new(paths: &Paths, spec: &'spec spec::Spec) -> Self {
        let mut duplex = duplex::Plan::new(spec);
        let entries = Self::entries(paths, spec, &mut duplex);
        Self {
            spec,
            entries,
            duplex,
        }
    }
}

pub(super) fn build(
    paths: &Paths,
    spec: &spec::Spec,
    h1: crate::runtime::H1,
) -> proc_macro2::TokenStream {
    use quote::quote;

    let plan = ProtocolPlan::new(paths, spec);
    let exec = Execution::new(paths, &plan);
    let h1 = match h1 {
        crate::runtime::H1::Available => Some(handle::H1Plan::new(&plan, &exec)),
        crate::runtime::H1::Missing => None,
    };
    let (handle, contract, shape) = handle::build(paths, &plan, &exec, h1.as_ref());
    let app = app::build(paths, &plan, &exec, h1.as_ref(), &contract, &shape);
    quote! {
        #app
        #handle
    }
}
