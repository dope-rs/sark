use sark_http::{
    field::metadata::{integer, strings},
    payload::compress::dynamic::insert::stages,
};

use crate::qpack::decoder::ingress::Loan;

pub(super) type Phase = stages::Owner<Ready, strings::Cursor, Value>;

#[derive(Clone, Copy)]
pub(super) enum Ready {
    Instruction,
    SetCapacity(integer::Continuation),
    Duplicate(integer::Continuation),
    DynamicName(integer::Continuation),
    StaticName(integer::Continuation),
    PlainName(integer::Continuation),
    HuffmanName(integer::Continuation),
    Commit(u64),
}

#[derive(Clone, Copy)]
pub(super) enum Value {
    Header,
    PlainLength(integer::Continuation),
    HuffmanLength(integer::Continuation),
    Literal(strings::Cursor),
    Commit,
}

pub(super) enum Action<'a> {
    SetCapacity(Loan<'a, stages::Ready>),
    Duplicate(Loan<'a, stages::Ready>),
    NameReference {
        table: Loan<'a, stages::Ready>,
        location: Location,
    },
    LiteralName {
        table: Loan<'a, stages::Ready>,
        encoding: strings::Encoding,
    },
    LiteralValue {
        table: Loan<'a, stages::Value>,
        encoding: strings::Encoding,
    },
}

#[derive(Clone, Copy)]
pub(super) enum Location {
    Dynamic,
    Static,
}

impl Action<'_> {
    pub(super) fn pending(self, number: integer::Continuation) {
        match self {
            Self::SetCapacity(table) => table.pending(Ready::SetCapacity(number)),
            Self::Duplicate(table) => table.pending(Ready::Duplicate(number)),
            Self::NameReference { table, location } => table.pending(match location {
                Location::Dynamic => Ready::DynamicName(number),
                Location::Static => Ready::StaticName(number),
            }),
            Self::LiteralName { table, encoding } => table.pending(match encoding {
                strings::Encoding::Plain => Ready::PlainName(number),
                strings::Encoding::Huffman => Ready::HuffmanName(number),
            }),
            Self::LiteralValue { table, encoding } => table.pending(match encoding {
                strings::Encoding::Plain => Value::PlainLength(number),
                strings::Encoding::Huffman => Value::HuffmanLength(number),
            }),
        }
    }
}

const _: [(); 5 * size_of::<usize>()] = [(); size_of::<Phase>()];
