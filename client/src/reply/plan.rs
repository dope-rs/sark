use o3::buffer::{resident, segments};
use reply::detach::{Error, Part, Parts, copies::Copies};

use crate::{protocol::response, reply};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Mode {
    Detach,
    Copy,
}

const _: () = assert!(size_of::<Mode>() == 1);

/// Keeps the exact normalized source and copy total through allocation.
pub(super) struct Plan<'source, 'a, 'd> {
    source: &'source reply::Scoped<'a, 'd>,
    bytes: usize,
    parts: Parts,
}

impl<'source, 'a, 'd> Plan<'source, 'a, 'd> {
    /// Keeps every original owner alive while unique residents become independent.
    /// Shared aliases stay bound through the subsequent immutable copy plan.
    pub(super) fn try_new(
        source: &'source mut reply::Scoped<'a, 'd>,
        mode: Mode,
    ) -> Result<Self, Error> {
        let mut bytes = 0;
        let mut parts = Parts::empty();
        Self::add_fields(&mut source.headers, Part::Headers, &mut bytes, &mut parts)?;
        match mode {
            Mode::Detach => {
                let segments = source.body.segments.count();
                if let Some(mut front) = source.body.segments.front_entry() {
                    for index in 0..segments.into_raw() {
                        let owner = front.take();
                        let owner = match owner.try_owned() {
                            Ok(owner) => owner.into(),
                            Err(owner) => owner,
                        };
                        if let Some(owner) = owner.scope() {
                            Self::add(owner.len(), Part::Body, &mut bytes, &mut parts)?;
                        }
                        front = source.body.segments.try_insert(owner).map_err(|rejected| {
                            Error::MetadataCommit {
                                segment: segments::Count::new(index),
                                cause: rejected.discard(),
                            }
                        })?;
                    }
                }
            }
            Mode::Copy => Self::add(source.body.len(), Part::Body, &mut bytes, &mut parts)?,
        }
        if let Some(fields) = source.trailers.as_mut() {
            Self::add_fields(fields, Part::Trailers, &mut bytes, &mut parts)?;
        }
        Ok(Self {
            source,
            bytes,
            parts,
        })
    }

    pub(super) fn segments(&self) -> segments::Count {
        self.source.body.segments.count()
    }

    fn add_fields(
        fields: &mut response::Fields<resident::Shared<'d>>,
        part: Part,
        bytes: &mut usize,
        parts: &mut Parts,
    ) -> Result<(), Error> {
        if let Some(owner) = fields.prepare() {
            Self::add(owner.len(), part, bytes, parts)?;
        }
        Ok(())
    }

    fn add(added: usize, part: Part, bytes: &mut usize, parts: &mut Parts) -> Result<(), Error> {
        let Some(total) = bytes.checked_add(added) else {
            return Err(Error::Overflow {
                part,
                current: *bytes,
                added,
            });
        };
        *bytes = total;
        *parts = parts.with(part);
        Ok(())
    }

    pub(super) fn allocate(self) -> Result<Copies, Error> {
        Copies::try_new(self.bytes, self.parts)
    }
}
