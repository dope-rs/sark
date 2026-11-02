use std::collections;

pub(crate) struct LengthArms {
    groups: collections::BTreeMap<usize, Vec<proc_macro2::TokenStream>>,
}

impl LengthArms {
    pub(crate) fn collect(
        items: impl IntoIterator<Item = (usize, proc_macro2::TokenStream)>,
    ) -> Self {
        use std::collections::BTreeMap;
        let mut groups: collections::BTreeMap<usize, Vec<proc_macro2::TokenStream>> =
            BTreeMap::new();
        for (len, check) in items {
            groups.entry(len).or_default().push(check);
        }
        Self { groups }
    }

    pub(crate) fn emit(self) -> Vec<proc_macro2::TokenStream> {
        use quote::quote;
        self.groups
            .into_iter()
            .map(|(len, checks)| quote! { #len => { #( #checks )* } })
            .collect()
    }
}
