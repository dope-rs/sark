use std::{mem, time::Duration};

use sark_http::{
    field,
    field::metadata::{Metadata, section::rules},
};

use crate::{
    Reason, metadata,
    protocol::{Block, fields, section},
};

pub mod timeout;

pub(crate) fn is_path<'a>(mut path: impl Iterator<Item = &'a u8>) -> bool {
    path.next() == Some(&b'/') && path.filter(|byte| **byte == b'/').count() == 1
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Scheme {
    #[default]
    Http,
    Https,
}

impl Scheme {
    pub const fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Http => b"http",
            Self::Https => b"https",
        }
    }
}

#[derive(Clone, Copy)]
pub struct Fields<'a> {
    scheme: Scheme,
    path: field::Value<&'a [u8]>,
    authority: Option<field::Value<&'a [u8]>>,
    metadata: &'a metadata::Metadata,
}

impl<'a> Fields<'a> {
    pub fn new(
        path: &'a [u8],
        host: Option<&'a [u8]>,
        metadata: &'a metadata::Metadata,
    ) -> Result<Self, crate::Status> {
        if !is_path(path.iter()) {
            return Err(crate::Status::new(crate::Code::Unimplemented, Reason::Path));
        }
        Ok(Self {
            scheme: Scheme::Http,
            path: field::Value::try_bytes(path).map_err(metadata::Error::value)?,
            authority: host
                .map(field::Value::try_bytes)
                .transpose()
                .map_err(metadata::Error::value)?,
            metadata,
        })
    }

    pub const fn with_scheme(mut self, scheme: Scheme) -> Self {
        self.scheme = scheme;
        self
    }
}

impl field::Source for Fields<'_> {
    fn emit<'field>(&'field self, sink: &mut impl field::Sink<'field>) {
        sink.entry(
            const {
                field::Entry::pseudo(
                    field::Pseudo::Method,
                    field::literal!(field::Value::try_bytes(b"POST")),
                )
            },
        );
        let scheme = match self.scheme {
            Scheme::Http => field::literal!(field::Value::try_bytes(b"http")),
            Scheme::Https => field::literal!(field::Value::try_bytes(b"https")),
        };
        sink.entry(field::Entry::pseudo(field::Pseudo::Scheme, scheme));
        sink.entry(field::Entry::pseudo(field::Pseudo::Path, self.path));
        if let Some(host) = self.authority {
            sink.entry(field::Entry::pseudo(field::Pseudo::Authority, host));
        }
        sink.field(
            field::literal!(field::Lower::try_bytes(b"content-type")),
            field::literal!(field::Value::try_bytes(b"application/grpc+proto")),
        );
        sink.field(
            field::literal!(field::Lower::try_bytes(b"te")),
            field::literal!(field::Value::try_bytes(b"trailers")),
        );
        fields::emit(self.metadata, sink);
    }
}

#[derive(PartialEq, Eq)]
pub struct Head {
    fields: section::Request,
}

impl Head {
    pub fn new(
        path: &[u8],
        host: Option<&[u8]>,
        metadata: &metadata::Metadata,
    ) -> Result<Self, crate::Status> {
        let fields = Block::request(path, host, metadata)?;
        Self::from_fields(&fields)
    }

    pub fn from_fields<S: Metadata + ?Sized>(fields: &S) -> Result<Self, crate::Status> {
        let fields = section::Plan::build(fields, rules::Kind::Request)?;
        Self::from_owner(fields)
    }

    pub(crate) fn from_owner(fields: section::Owner) -> Result<Self, crate::Status> {
        Ok(Self {
            fields: section::Request::new(fields)?,
        })
    }

    pub(crate) fn trailers(fields: section::Owner) -> Result<metadata::Decoded, crate::Status> {
        let fields = fields.validate(rules::Kind::RequestTrailers)?;
        Ok(metadata::Decoded::from(fields))
    }

    pub fn path(&self) -> &[u8] {
        section::View::<section::Request>::new(&self.fields).required()
    }

    pub fn authority(&self) -> Option<&[u8]> {
        self.fields.authority()
    }

    /// The admitted timeout; absence is unlimited and zero expires immediately.
    pub fn timeout(&self) -> Option<Duration> {
        section::View::<section::Valid>::new(self.fields.owner()).timeout()
    }

    pub fn metadata(&self) -> metadata::View<'_> {
        metadata::View::from(self.fields.owner())
    }
}

const _: () = {
    assert!(mem::size_of::<Head>() == mem::size_of::<section::Valid>());
    assert!(mem::size_of::<Option<Head>>() == mem::size_of::<Head>());
};
