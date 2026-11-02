pub use prost_build;

mod method;
mod names;
mod table;
pub(crate) use table::Table;
mod server;

/// Generates direct gRPC server routes for prost services.
pub struct Routes;

impl Routes {
    fn emit(schema: prost_build::Service) -> syn::Result<proc_macro2::TokenStream> {
        let name = syn::parse_str(&schema.name)?;
        let service = quote::format_ident!("{}Service", name);
        let routes = quote::format_ident!("{}Routes", name);
        let prefix = quote::format_ident!("__SarkGrpc{}", names::stem(&name));
        let call = quote::format_ident!("{}Call", prefix);
        let factory = names::suffixed(
            &schema.proto_name,
            heck::ToSnakeCase::to_snake_case(schema.proto_name.as_str()),
            "_routes",
        )?;
        let mut methods = schema
            .methods
            .iter()
            .enumerate()
            .map(|(index, desc)| method::Method::new(&schema, desc, &prefix, index))
            .collect::<syn::Result<Vec<_>>>()?;
        let names = names::Names::new(&methods, &service);
        let module = names.ident(&format!("__sark_grpc_{}", names::stem(&factory)));
        let aliases = methods
            .iter_mut()
            .map(|method| method.scope(&names))
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
        let tokens = match Self::emit(service) {
            Ok(tokens) => tokens,
            Err(error) => error.into_compile_error(),
        };
        buffer.push('\n');
        buffer.push_str(&tokens.to_string());
    }
}
