use crate::runtime::paths::Paths;

/// Borrows the leaves whose balanced binary shape is emitted as a type or a
/// value. Both forms therefore use the same split rule.
pub(in crate::app::serve) struct Tree<'a>(&'a [proc_macro2::TokenStream]);

impl<'a> Tree<'a> {
    pub(in crate::app::serve) const fn new(leaves: &'a [proc_macro2::TokenStream]) -> Self {
        Self(leaves)
    }

    pub(in crate::app::serve) fn ty(&self, paths: &Paths) -> proc_macro2::TokenStream {
        use quote::quote;
        let ns_fiber = &paths.fiber;

        match self.0 {
            [] => proc_macro2::TokenStream::new(),
            [leaf] => quote!(#ns_fiber::dope_fiber::task::storage::fixed::tree::Leaf<#leaf>),
            leaves => {
                let split = leaves.len() / 2;
                let left = Self(&leaves[..split]).ty(paths);
                let right = Self(&leaves[split..]).ty(paths);
                quote!(#ns_fiber::dope_fiber::task::storage::fixed::tree::Branch<#left, #right>)
            }
        }
    }

    pub(in crate::app::serve) fn value(&self, paths: &Paths) -> proc_macro2::TokenStream {
        use quote::quote;
        let ns_fiber = &paths.fiber;

        match self.0 {
            [] => proc_macro2::TokenStream::new(),
            [leaf] => quote!(#ns_fiber::dope_fiber::task::storage::fixed::tree::Leaf::new(#leaf)),
            leaves => {
                let split = leaves.len() / 2;
                let left = Self(&leaves[..split]).value(paths);
                let right = Self(&leaves[split..]).value(paths);
                quote!(#ns_fiber::dope_fiber::task::storage::fixed::tree::Branch::new(#left, #right))
            }
        }
    }
}
