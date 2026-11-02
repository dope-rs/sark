use o3::buffer::pool;

use crate::field::{
    head,
    metadata::{map::record, section::storage},
};

pub(in crate::field::metadata::lower) enum Plan<'a, 'field, D> {
    Discard {
        lease: &'a mut pool::Span,
        field: record::View<'field>,
        known: head::Name,
    },
    Inspect {
        lease: &'a mut pool::Span,
        field: record::View<'field>,
        token: D,
        known: head::Name,
    },
    Skip,
    Scratch {
        lease: &'a mut pool::Span,
        name: record::Span<'field>,
    },
    Tagged {
        append: pool::Append<'a, ([u8; storage::Tag::PREFIX_LEN], [&'field [u8]; 2]), pool::Span>,
        name: record::Span<'field>,
        known: head::Name,
        tag: storage::Tag,
        indexing: crate::field::Indexing,
    },
}
