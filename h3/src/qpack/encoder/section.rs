use o3::alloc;
use sark_http::{
    self, field,
    field::metadata::{
        block::Connect,
        map::record::{Entry, Span},
        strings,
    },
    payload::{compress::dynamic::ENTRY_OVERHEAD, response::Method},
};

use crate::{
    frame,
    qpack::{self, encoder, encoder::buffer, identity::Identity, ledger, statics},
};

pub(crate) struct Section<'a, C = ()> {
    dynamic: Option<Dynamic<'a>>,
    section: buffer::Cache<'a>,
    output: encoder::Output,
    status: Status,
    encoding: strings::Encoding,
    identity: C,
}

/// Unit capture removes identity state and work from ordinary field sections.
pub(crate) trait Capture {
    fn check(&mut self, name: &[u8], value: usize) -> Result<(), encoder::Error>;

    fn try_field(&mut self, name: &[u8], value: &[u8]) -> Result<(), alloc::Error>;
}

impl Capture for () {
    fn check(&mut self, _: &[u8], _: usize) -> Result<(), encoder::Error> {
        Ok(())
    }

    fn try_field(&mut self, _: &[u8], _: &[u8]) -> Result<(), alloc::Error> {
        Ok(())
    }
}

impl Capture for (Identity, usize) {
    fn check(&mut self, name: &[u8], value: usize) -> Result<(), encoder::Error> {
        let required = self
            .0
            .len()
            .checked_add(16)
            .and_then(|size| size.checked_add(name.len()))
            .and_then(|size| size.checked_add(value))
            .ok_or(alloc::Error::Overflow)?;
        if required > self.1 {
            return Err(encoder::Error::FieldCapacity {
                required,
                limit: self.1,
            });
        }
        Ok(())
    }

    fn try_field(&mut self, name: &[u8], value: &[u8]) -> Result<(), alloc::Error> {
        self.0
            .try_field(Span::from_slice(name), Span::from_slice(value))
    }
}

/// Peer admission and response facts from the same field emission as QPACK.
pub(crate) struct Peer<C> {
    capture: C,
    remaining: u64,
    connect: Connect,
    method: Option<Method>,
}

impl<C> Peer<C> {
    pub(crate) fn new(peer: Option<&frame::Settings>, capture: C) -> Self {
        Self {
            capture,
            remaining: peer
                .and_then(frame::Settings::field_limit)
                .map_or(u64::MAX, |limit| limit.get()),
            connect: peer.map_or(Connect::Disabled, frame::Settings::connect),
            method: None,
        }
    }

    pub(crate) fn into_parts(self) -> (C, Option<Method>) {
        (self.capture, self.method)
    }
}

impl<C: Capture> Capture for Peer<C> {
    fn check(&mut self, name: &[u8], value: usize) -> Result<(), encoder::Error> {
        if name == b":protocol" && self.connect == Connect::Disabled {
            return Err(encoder::Error::ConnectDisabled);
        }
        self.remaining = self
            .remaining
            .checked_sub(ENTRY_OVERHEAD as u64)
            .and_then(|left| left.checked_sub(name.len() as u64))
            .and_then(|left| left.checked_sub(value as u64))
            .ok_or(encoder::Error::FieldSection {
                remaining: self.remaining,
                name: name.len(),
                value,
            })?;
        self.capture.check(name, value)
    }

    fn try_field(&mut self, name: &[u8], value: &[u8]) -> Result<(), alloc::Error> {
        if name == b":method" {
            self.method = Some(Method::from_bytes(value));
        }
        self.capture.try_field(name, value)
    }
}

struct Dynamic<'a> {
    delta: encoder::Delta<'a>,
    permit: ledger::Admission<'a>,
}

const _: () =
    assert!(core::mem::size_of::<Option<Dynamic<'_>>>() == core::mem::size_of::<Dynamic<'_>>());

enum Status {
    Open,
    Failed(encoder::Error),
}

impl<'a, C: Capture> Section<'a, C> {
    pub(super) fn new(
        target: &'a mut encoder::State,
        stream: u64,
        identity: C,
    ) -> Result<Self, encoder::Error> {
        let floor = target.floor();
        let mut section = buffer::Cache(&mut target.section);
        let staged = &mut target.staged;
        let received = target.known_received_count;
        let dynamic = match &mut target.outstanding {
            encoder::Tracking::Active { table, ledger } if table.view().capacity() != 0 => {
                match ledger.admit(stream, target.max_blocked_streams, received) {
                    Some(permit) => Some(Dynamic {
                        delta: encoder::Delta::try_new(table, floor, staged)?,
                        permit,
                    }),
                    None => None,
                }
            }
            encoder::Tracking::Disabled | encoder::Tracking::Active { .. } => {
                *staged = None;
                None
            }
        };
        let output = encoder::Output::new(section.take());
        Ok(Self {
            dynamic,
            section,
            output,
            status: Status::Open,
            encoding: target.encoding,
            identity,
        })
    }

    pub(super) fn finish(self) -> Result<encoder::Prepared<'a, C>, encoder::Error> {
        let Self {
            dynamic,
            mut section,
            output,
            status,
            identity,
            ..
        } = self;
        match status {
            Status::Failed(error) => {
                if let Some(dynamic) = dynamic {
                    dynamic.delta.rollback();
                }
                section.recycle(output.into_vec());
                return Err(error);
            }
            Status::Open => {}
        }
        let (encoded, required, base, tracking, delta) = match dynamic {
            Some(Dynamic { delta, permit }) => {
                let required = permit.required();
                let encoded = delta.encode_count(required);
                let base = delta.base();
                (
                    encoded,
                    required,
                    base,
                    permit.track(),
                    Some(delta.finish()),
                )
            }
            None => (0, 0, 0, None, None),
        };
        Ok(encoder::Prepared {
            section: qpack::Section::new(encoded, required, base, output.into_vec()),
            identity,
            commit: encoder::Commit {
                tracking,
                section,
                delta,
            },
        })
    }

    fn try_encode(&mut self, field: Entry<'_>) -> Result<(), encoder::Error> {
        self.identity.check(field.name, field.value.len())?;
        self.identity.try_field(field.name, field.value)?;
        if matches!(field.indexing, field::Indexing::Never) {
            return self.output.literal(field, self.encoding);
        }
        if let Some(found) = statics::STATIC.lookup(field) {
            return match found.exact {
                Some(index) => self.output.use_static(index),
                None => self
                    .output
                    .copy_static(found.name, field.value, self.encoding),
            };
        }
        let Some(Dynamic { delta, permit }) = self.dynamic.as_mut() else {
            return self.output.literal(field, self.encoding);
        };
        let base = delta.base();
        let mut query = delta.query(field);
        if let Some(index) = query.exact(permit) {
            query.protect(index);
            return self.output.use_dynamic(index, base);
        }
        if let Some(index) = query.name(permit) {
            query.protect(index);
            return self
                .output
                .copy_dynamic(index, base, field.value, self.encoding);
        }
        self.output.literal(field, self.encoding)?;
        query.try_insert()
    }
}

impl<'field, C: Capture> field::Sink<'field> for Section<'_, C> {
    fn entry(&mut self, field: field::Entry<'field>) {
        if matches!(&self.status, Status::Failed(_)) {
            return;
        }
        if let Err(error) = self.try_encode(field.as_record()) {
            self.status = Status::Failed(error);
        }
    }

    fn encoded_with<E: field::Encode>(
        &mut self,
        name: field::Lower<&'field [u8]>,
        encoded: E,
        indexing: field::Indexing,
    ) {
        if matches!(&self.status, Status::Failed(_)) {
            return;
        }
        if let Err(error) = self.generated(name.get(), encoded, indexing) {
            self.status = Status::Failed(error);
        }
    }
}

impl<C: Capture> Section<'_, C> {
    fn generated<E: field::Encode>(
        &mut self,
        name: &[u8],
        encoded: E,
        indexing: field::Indexing,
    ) -> Result<(), encoder::Error> {
        let len = encoded.encoded_len();
        self.identity.check(name, len)?;
        if matches!(indexing, field::Indexing::Never) {
            self.output.name(name, self.encoding, indexing)?;
        } else if let Some(found) = statics::STATIC.lookup(Entry::new(name, &[])) {
            self.output.static_name(found.name)?;
        } else {
            let indexed = match self.dynamic.as_mut() {
                Some(Dynamic { delta, permit }) => {
                    let index = delta.needle(name, permit);
                    if let Some(index) = index {
                        delta.protect(index);
                    }
                    index.map(|index| (index, delta.base()))
                }
                None => None,
            };
            if let Some((index, base)) = indexed {
                self.output.dynamic_name(index, base)?;
            } else {
                self.output.name(name, self.encoding, indexing)?;
            }
        }
        let value = self.output.encoded(encoded, len)?;
        self.identity.try_field(name, value)?;
        Ok(())
    }
}
