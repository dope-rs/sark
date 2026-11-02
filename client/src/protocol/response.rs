use std::mem;

use o3::buffer::{self, resident, storage};
use sark_http::{
    field,
    h1::{field::Entry, head::response::Coding},
    payload::response::status,
};

use crate::reply::detach::{self, copies::Copies};

/// One retained, syntactically admitted HTTP/1 field section.
#[repr(transparent)]
#[derive(Debug)]
pub struct Fields<S> {
    owner: S,
}

impl<S: AsRef<[u8]>> Fields<S> {
    pub fn as_bytes(&self) -> &[u8] {
        self.owner.as_ref()
    }

    pub fn get(&self, name: impl AsRef<str>) -> Option<&[u8]> {
        let name = name.as_ref().as_bytes();
        let mut bytes = self.as_bytes();
        while let Some((field, value, rest)) = Entry::split(bytes) {
            if field.eq_ignore_ascii_case(name) {
                return Some(field::Error::trim_ows(value));
            }
            bytes = rest;
        }
        None
    }
}

impl<'d> Fields<resident::Shared<'d>> {
    pub(in crate::protocol) const fn admit(owner: resident::Shared<'d>) -> Self {
        Self { owner }
    }

    /// Transfers unique backing without changing this admitted field extent.
    pub(crate) fn prepare<'a>(&'a mut self) -> resident::Scope<'a, 'd> {
        self.owner = match mem::take(&mut self.owner).try_owned() {
            Ok(owner) => owner.into(),
            Err(owner) => owner,
        };
        self.owner.scope()
    }

    pub(crate) fn try_retain(
        self,
        copies: &mut Copies,
        part: detach::Part,
    ) -> Result<Fields<storage::Shared>, detach::Error> {
        Ok(Fields {
            owner: copies.try_retain(self.owner, part)?,
        })
    }
}

impl buffer::Residency for Fields<resident::Shared<'_>> {
    fn resident(&self) -> usize {
        self.owner.resident()
    }
}

/// A body-free incoming HTTP response head tied to its receive owner.
#[derive(Debug)]
pub struct Head<'d> {
    status: status::Code,
    coding: Coding,
    fields: Fields<resident::Shared<'d>>,
}

impl<'d> Head<'d> {
    pub(crate) const fn new(
        status: status::Code,
        coding: Coding,
        fields: Fields<resident::Shared<'d>>,
    ) -> Self {
        Self {
            status,
            coding,
            fields,
        }
    }

    pub const fn status(&self) -> status::Code {
        self.status
    }

    pub const fn headers(&self) -> &Fields<resident::Shared<'d>> {
        &self.fields
    }

    pub(crate) fn into_parts(self) -> (status::Code, Coding, Fields<resident::Shared<'d>>) {
        (self.status, self.coding, self.fields)
    }
}

/// Retained response body bytes admitted by the client codec.
#[repr(transparent)]
#[derive(Debug)]
pub struct Data<'d> {
    owner: resident::Shared<'d>,
}

impl<'d> Data<'d> {
    pub(crate) const fn from_owner(owner: resident::Shared<'d>) -> Self {
        Self { owner }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.owner.as_slice()
    }

    pub fn len(&self) -> usize {
        self.owner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.owner.is_empty()
    }

    pub(crate) fn into_owner(self) -> resident::Shared<'d> {
        self.owner
    }
}

impl buffer::Residency for Data<'_> {
    fn resident(&self) -> usize {
        self.owner.resident()
    }
}

/// One typed event from an HTTP/1 response.
#[derive(Debug)]
pub enum Event<'d> {
    Informational(Head<'d>),
    Head(Head<'d>),
    Data(Data<'d>),
    Trailers(Fields<resident::Shared<'d>>),
}

const _: () = assert!(
    core::mem::size_of::<Fields<resident::Shared<'static>>>()
        == core::mem::size_of::<resident::Shared<'static>>()
);
const _: () = assert!(
    core::mem::size_of::<Data<'static>>() == core::mem::size_of::<resident::Shared<'static>>()
);
