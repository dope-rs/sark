use crate::macros::tokens::local;

#[derive(Clone, Copy)]
pub(in crate::request) enum Case {
    Exact,
    Folded,
}

impl Case {
    pub(in crate::request) fn emit(
        self,
        name: &proc_macro2::Ident,
        bytes: &[u8],
    ) -> proc_macro2::TokenStream {
        let folded = matches!(self, Self::Folded);
        let chunk = local("__c");
        let mut checks = Vec::new();
        if folded && bytes.len() > 8 {
            let mut offset = 0usize;
            while offset + 8 < bytes.len() {
                checks.push(self.chunk_check::<8>(&chunk, bytes, offset));
                offset += 8;
            }
            let tail = bytes.len() - 8;
            checks.push(self.chunk_check::<8>(&chunk, bytes, tail));
            return Self::wrap(&chunk, name, bytes.len(), checks);
        }
        let mut offset = 0usize;
        while offset + 8 <= bytes.len() {
            checks.push(self.chunk_check::<8>(&chunk, bytes, offset));
            offset += 8;
        }
        if offset + 4 <= bytes.len() {
            checks.push(self.chunk_check::<4>(&chunk, bytes, offset));
            offset += 4;
        }
        if offset + 2 <= bytes.len() {
            checks.push(self.chunk_check::<2>(&chunk, bytes, offset));
            offset += 2;
        }
        if offset < bytes.len() {
            let byte = bytes[offset];
            if folded && byte.is_ascii_lowercase() {
                checks.push(quote::quote! { ((#chunk[#offset] | 0x20) == #byte) });
            } else {
                checks.push(quote::quote! { #chunk[#offset] == #byte });
            }
        }
        Self::wrap(&chunk, name, bytes.len(), checks)
    }

    fn chunk_check<const N: usize>(
        self,
        chunk: &proc_macro2::Ident,
        bytes: &[u8],
        offset: usize,
    ) -> proc_macro2::TokenStream {
        const { assert!(N == 2 || N == 4 || N == 8) };
        let c = &bytes[offset..offset + N];
        let indices = offset..offset + N;
        let read = if N == 8 {
            quote::quote! { ::core::primitive::u64::from_le_bytes([#(#chunk[#indices]),*]) }
        } else if N == 4 {
            quote::quote! { ::core::primitive::u32::from_le_bytes([#(#chunk[#indices]),*]) }
        } else {
            quote::quote! { ::core::primitive::u16::from_le_bytes([#(#chunk[#indices]),*]) }
        };
        let word = Self::word(c);
        if matches!(self, Self::Folded) {
            let mask = c.iter().enumerate().fold(0u64, |mask, (idx, byte)| {
                if byte.is_ascii_lowercase() {
                    mask | (0x20u64 << (idx * 8))
                } else {
                    mask
                }
            });
            quote::quote! { ((#read as ::core::primitive::u64) | #mask) == #word }
        } else {
            quote::quote! { (#read as ::core::primitive::u64) == #word }
        }
    }

    fn word(bytes: &[u8]) -> u64 {
        bytes.iter().enumerate().fold(0u64, |word, (idx, byte)| {
            word | (u64::from(*byte) << (idx * 8))
        })
    }

    fn wrap(
        chunk: &proc_macro2::Ident,
        name: &proc_macro2::Ident,
        len: usize,
        checks: Vec<proc_macro2::TokenStream>,
    ) -> proc_macro2::TokenStream {
        use proc_macro2::Literal;
        let len = Literal::usize_unsuffixed(len);
        quote::quote! {
            #name.len() == #len
                && match #name.first_chunk::<#len>() {
                    ::core::option::Option::Some(#chunk) => true #( && #checks )*,
                    ::core::option::Option::None => false,
                }
        }
    }
}
