use std::collections::BTreeMap;

pub use prost_build;

mod method;
mod names;
mod table;
pub(crate) use table::Table;
mod server;

/// Generates direct gRPC server routes for prost services.
///
/// Names already bound in the package receive trailing underscores in the
/// generated API. Construct with [`Routes::default`].
#[derive(Default)]
pub struct Routes {
    services: BTreeMap<prost_build::Module, Vec<prost_build::Service>>,
}

impl Routes {
    fn package(
        services: Vec<prost_build::Service>,
        source: &str,
    ) -> syn::Result<proc_macro2::TokenStream> {
        let services = services
            .into_iter()
            .map(Service::new)
            .collect::<syn::Result<Vec<_>>>()?;
        let mut names = names::Names::new(source)?;
        for service in &services {
            names.types(&service.methods);
        }
        let mut tokens = proc_macro2::TokenStream::new();
        for service in services {
            tokens.extend(service.emit(&mut names)?);
        }
        Ok(tokens)
    }
}

struct Service {
    name: proc_macro2::Ident,
    prefix: proc_macro2::Ident,
    factory: proc_macro2::Ident,
    methods: Vec<method::Method>,
}

impl Service {
    fn new(schema: prost_build::Service) -> syn::Result<Self> {
        let name = syn::parse_str(&schema.name)?;
        let prefix = quote::format_ident!("__SarkGrpc{}", names::stem(&name));
        let factory = names::suffixed(
            &schema.proto_name,
            heck::ToSnakeCase::to_snake_case(schema.proto_name.as_str()),
            "_routes",
        )?;
        let methods = schema
            .methods
            .iter()
            .enumerate()
            .map(|(index, desc)| method::Method::new(&schema, desc, &prefix, index))
            .collect::<syn::Result<Vec<_>>>()?;
        Ok(Self {
            name,
            prefix,
            factory,
            methods,
        })
    }

    fn emit(self, names: &mut names::Names) -> syn::Result<proc_macro2::TokenStream> {
        let Self {
            name,
            prefix,
            factory,
            mut methods,
        } = self;
        let service = names.ident(&quote::format_ident!("{}Service", name));
        let routes = names.ident(&quote::format_ident!("{}Routes", name));
        let call = names.ident(&quote::format_ident!("{}Call", prefix));
        let factory = names.ident(&factory);
        let module = names.ident(&quote::format_ident!(
            "__sark_grpc_{}",
            names::stem(&factory)
        ));
        let aliases = methods
            .iter_mut()
            .map(|method| method.scope(names))
            .collect::<proc_macro2::TokenStream>();
        let handler = server::emit(&methods, &service, &routes, &call)?;
        let traits = methods.iter().map(method::Method::emit_trait);
        let wrappers = methods.iter().map(|method| method.emit_wrapper(&service));
        let exports = methods.iter().map(|method| &method.wrapper);
        Ok(quote::quote! {
            #aliases

            pub use self::#module::{#service, #routes, #call, #factory, #(#exports,)*};

            mod #module {
                pub trait #service {
                    #(#traits)*
                }

                #(#wrappers)*

                #handler

                pub fn #factory<S: #service>(service: S) -> #routes<S> {
                    #routes { service }
                }
            }
        })
    }
}

impl prost_build::ServiceGenerator for Routes {
    fn generate(&mut self, service: prost_build::Service, buffer: &mut String) {
        self.services
            .entry(prost_build::Module::from_protobuf_package_name(
                &service.package,
            ))
            .or_default()
            .push(service);
        // Prost retains a service-only package only when its buffer is nonempty.
        buffer.push('\n');
    }

    fn finalize_package(&mut self, package: &str, buffer: &mut String) {
        let module = prost_build::Module::from_protobuf_package_name(package);
        let Some(services) = self.services.remove(&module) else {
            return;
        };
        let tokens = match Self::package(services, buffer) {
            Ok(tokens) => tokens,
            Err(error) => error.into_compile_error(),
        };
        buffer.push('\n');
        buffer.push_str(&tokens.to_string());
    }
}
