pub mod body;
pub mod coding;
pub mod crc32;
pub mod gzip;
pub mod handoff;
pub mod headers;
pub mod phase;
pub mod status;
use std::collections::TryReserveError;

use o3::{
    alloc,
    alloc::BoxSliceExt as _,
    buffer::{
        storage,
        write::{self, Sink as _, consume},
    },
};
use sark_http::{
    field,
    payload::{
        fixed,
        response::{
            Body, dispatch, encoded, exact,
            headers::{Application, Entry, Name},
            status::Code,
        },
    },
};

#[derive(Debug)]
pub enum Error {
    Allocation(alloc::Error),
    FieldWrite(consume::Error),
    FieldLength { expected: usize, actual: usize },
    Reserve(TryReserveError),
    Size(exact::Overflow),
    Exact(storage::BuildError<consume::Error>),
    Bounded(storage::BuildError<consume::Error>),
}

#[derive(Default, Debug)]
pub struct Capture {
    status: Option<Code>,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
    body: Vec<u8>,
    calls: usize,
    pointer: Option<usize>,
}

impl Capture {
    pub const fn status(&self) -> Option<Code> {
        self.status
    }

    pub fn fields(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.fields
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub const fn calls(&self) -> usize {
        self.calls
    }

    pub const fn pointer(&self) -> Option<usize> {
        self.pointer
    }

    fn head<H: field::Source<Application>>(
        mut self,
        status: Code,
        headers: &H,
    ) -> Result<Self, (Error, Self)> {
        self.status = Some(status);
        let mut fields = Fields(&mut self.fields, None);
        headers.emit(&mut fields);
        if let Some(error) = fields.1 {
            return Err((error, self));
        }
        self.calls += 1;
        Ok(self)
    }

    fn capture_body<'d, B: Body<'d>>(&mut self, body: B) -> Result<(), Error> {
        self.pointer = body.as_slice().map(|bytes| bytes.as_ptr() as usize);
        self.body
            .try_reserve_exact(body.len())
            .map_err(Error::Reserve)?;
        for segment in body.segments() {
            self.body.extend_from_slice(segment);
        }
        Ok(())
    }
}

struct Fields<'a>(&'a mut Vec<(Vec<u8>, Vec<u8>)>, Option<Error>);

impl Fields<'_> {
    fn try_field<E: field::Encode>(&mut self, name: &[u8], encoded: E) -> Result<(), Error> {
        let expected = encoded.encoded_len();
        let mut value = Vec::new();
        let mut stage = write::Staged::try_grow(&mut value, expected).map_err(Error::Allocation)?;
        encoded
            .try_write(|bytes| stage.write_slice(bytes))
            .map_err(Error::FieldWrite)?;
        stage
            .try_complete()
            .map_err(|error| {
                let (_, actual) = error.into_parts();
                Error::FieldLength { expected, actual }
            })?
            .commit();
        let name = Box::<[u8]>::try_copy(name)
            .map_err(|error| Error::Allocation(error.into()))?
            .into_vec();
        alloc::VecExt::try_reserve(self.0, 1).map_err(Error::Allocation)?;
        self.0.push((name, value));
        Ok(())
    }
}

impl<'field> field::Sink<'field, Application> for Fields<'_> {
    fn entry(&mut self, field: Entry<'field>) {
        if self.1.is_none() {
            self.1 = self.try_field(field.name(), field.value()).err();
        }
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: Name<&'field [u8]>,
        encoded: E,
        _: field::Indexing,
    ) {
        if self.1.is_none() {
            self.1 = self.try_field(name.as_bytes(), encoded).err();
        }
    }
}

impl<'d> dispatch::Accept<'d> for Capture {
    type Output = Self;
    type Error = (Error, Self);

    fn fixed<H, B>(mut self, status: Code, headers: H, body: B) -> Result<Self::Output, Self::Error>
    where
        H: field::Source<Application>,
        B: Body<'d>,
    {
        self = self.head(status, &headers)?;
        match self.capture_body(body) {
            Ok(()) => Ok(self),
            Err(error) => Err((error, self)),
        }
    }

    fn statics<'body, H>(
        mut self,
        status: Code,
        headers: H,
        body: fixed::Body<'body>,
    ) -> Result<Self::Output, Self::Error>
    where
        H: field::Source<Application>,
        'body: 'd,
    {
        self = self.head(status, &headers)?;
        let bytes = body.as_slice();
        if let Err(error) = self
            .body
            .try_reserve_exact(bytes.len())
            .map_err(Error::Reserve)
        {
            return Err((error, self));
        }
        self.body.extend_from_slice(bytes);
        Ok(self)
    }

    fn encoded<const EXACT: bool>(
        mut self,
        status: Code,
        headers: impl field::Source<Application>,
        body: impl encoded::Source<EXACT>,
    ) -> Result<Self::Output, Self::Error> {
        self = self.head(status, &headers)?;
        let plan = match body.try_plan() {
            Ok(plan) => plan,
            Err(error) => return Err((Error::Size(error.into()), self)),
        };
        let limit = plan.limit();
        if let Err(error) = self
            .body
            .try_reserve_exact(limit.saturating_sub(self.body.len()))
            .map_err(Error::Reserve)
        {
            return Err((error, self));
        }
        self.body.resize(limit, 0);
        let written = match plan.write(&mut write::Slice::new(&mut self.body)) {
            Ok(written) => written,
            Err(error) => {
                return Err((
                    if EXACT {
                        Error::Exact(error)
                    } else {
                        Error::Bounded(error)
                    },
                    self,
                ));
            }
        };
        if !EXACT {
            self.body.truncate(written);
        }
        Ok(self)
    }
}

impl<'d> dispatch::Sink<'d> for Capture {
    type Stream<S> = (Self, Option<S>);

    fn stream<H, S>(
        mut self,
        status: Code,
        headers: H,
        stream: Option<S>,
    ) -> Result<Self::Stream<S>, Self::Error>
    where
        H: field::Source<Application>,
    {
        self = self.head(status, &headers)?;
        Ok((self, stream))
    }
}
