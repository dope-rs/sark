use std::mem;

use sark_http::{
    field,
    field::metadata::{Metadata, section::rules},
    payload::response::status,
};

use crate::{
    Reason, metadata,
    protocol::{
        fields,
        section::{self, seal},
    },
};

#[derive(Clone, Copy)]
pub struct Fields<'a> {
    metadata: &'a metadata::Metadata,
}

impl<'a> Fields<'a> {
    pub const fn new(metadata: &'a metadata::Metadata) -> Self {
        Self { metadata }
    }
}

impl field::Source for Fields<'_> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        sink.entry(
            const {
                field::Entry::pseudo(
                    field::Pseudo::Status,
                    field::literal!(field::Value::try_bytes(b"200")),
                )
            },
        );
        sink.field(
            field::literal!(field::Lower::try_bytes(b"content-type")),
            field::literal!(field::Value::try_bytes(b"application/grpc+proto")),
        );
        fields::emit(self.metadata, sink);
    }
}

#[derive(PartialEq, Eq)]
#[repr(transparent)]
pub struct Head {
    fields: section::Valid,
}

impl Head {
    pub fn from_fields<S: Metadata + ?Sized>(fields: &S) -> Result<Self, crate::Status> {
        let fields = section::Plan::build(fields, rules::Kind::Response)?;
        Self::from_owner(fields)
    }

    pub(crate) fn from_owner(fields: section::Owner) -> Result<Self, crate::Status> {
        use section::{Valid, View};

        let fields = fields.validate(rules::Kind::Response)?;
        if View::<Valid>::new(&fields).code().is_some()
            || View::<Valid>::new(&fields).is_present(seal::Field::First)
            || View::<Valid>::new(&fields).is_present(seal::Field::Second)
        {
            return Err(crate::Status::new(
                crate::Code::Internal,
                Reason::EarlyStatus,
            ));
        }
        Ok(Self { fields })
    }

    pub(crate) fn http_status(&self) -> status::Code {
        // SAFETY: Head admits Response fields, which require a valid HTTP status.
        // Its frozen owner retains that nonzero value in the complete prefix.
        unsafe {
            self.fields
                .bytes()
                .as_ptr()
                .add(seal::HTTP_STATUS)
                .cast::<status::Code>()
                .read_unaligned()
        }
    }

    pub(crate) fn is_grpc(&self) -> bool {
        section::View::<section::Valid>::new(&self.fields).is_grpc()
    }

    pub fn metadata(&self) -> metadata::View<'_> {
        metadata::View::from(&self.fields)
    }

    pub(crate) fn into_inner(self) -> metadata::Decoded {
        metadata::Decoded::from(self.fields)
    }
}

const _: () = assert!(mem::size_of::<Head>() == mem::size_of::<metadata::Decoded>());
