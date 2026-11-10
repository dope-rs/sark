use dope_quic::wire::varint;
use o3::buffer::write::Sink;
use sark_core::manifold::stream::Boundary;
use sark_http::{field, payload::response::Method};

use crate::{
    conn,
    conn::{
        io::write::{Entry, budget},
        output,
    },
    frame,
    qpack::encoder,
    stream,
};

pub(in crate::conn) struct Section<'q, 'd> {
    pub(super) target: stream::Target,
    pub(super) entry: Entry<'d>,
    pub(super) qpack: encoder::Commit<'q>,
}

impl<'q, 'd> Section<'q, 'd> {
    pub(in crate::conn) fn discard(self) {
        self.qpack.discard();
    }

    pub(in crate::conn) fn frame<S: field::Source>(
        target: stream::Target,
        kind: varint::VarInt,
        lead: Option<varint::VarInt>,
        encoder: &'q mut encoder::State,
        peer: Option<&frame::Settings>,
        source: S,
        boundary: Boundary,
    ) -> Result<(Self, Option<Method>), conn::Error> {
        let prepared = encoder::Prepared::prepare(
            encoder,
            target.stream_id().get(),
            source,
            encoder::Peer::new(peer, ()),
        )
        .map_err(conn::Error::Encode)?;
        let (fields, peer) = Self::prepared(target, kind, lead, prepared, boundary)?;
        let ((), method) = peer.into_parts();
        Ok((fields, method))
    }

    pub(in crate::conn) fn prepared<C>(
        target: stream::Target,
        kind: varint::VarInt,
        lead: Option<varint::VarInt>,
        prepared: encoder::Prepared<'q, C>,
        boundary: Boundary,
    ) -> Result<(Self, C), conn::Error> {
        let (section, mut qpack, identity) = prepared.into_parts();
        let prefix = (|| {
            let lead_len = match lead {
                Some(value) => usize::from(value.encoded_len()),
                None => 0,
            };
            let fields = section.encoded_len();
            let len = lead_len
                .checked_add(fields)
                .ok_or(conn::Error::SectionLength {
                    leading: lead_len,
                    fields,
                })?;
            let fields = fields - section.lines().len();
            let head_len = lead_len
                .checked_add(fields)
                .ok_or(conn::Error::SectionPrefix {
                    leading: lead_len,
                    fields,
                })?;
            let header = frame::Header::for_payload(kind, len).map_err(conn::Error::Parse)?;
            let cap =
                header
                    .header_len
                    .checked_add(head_len)
                    .ok_or(conn::Error::PrefixOverflow {
                        len: header.header_len,
                        additional: head_len,
                    })?;
            output::WritePrefix::try_write(cap, |writer| {
                writer.extend([header.kind.pack().as_ref(), header.length.pack().as_ref()]);
                if let Some(value) = lead {
                    writer.extend([value.pack().as_ref()]);
                }
                section.write_prefix(writer);
            })
        })();
        let mut prefix = match prefix {
            Ok(prefix) => prefix,
            Err(error) => {
                qpack.discard_vec(section.into_lines());
                return Err(error);
            }
        };
        let lines = section.into_lines();
        let entry = if let output::WritePrefix::Inline(bytes) = &mut prefix
            && bytes.write_slice(&lines).is_ok()
        {
            qpack.recycle(lines);
            Entry::single(budget::WriteClass::Regular, prefix, boundary)
        } else {
            Entry::frame(budget::WriteClass::Regular, prefix, lines, boundary)
        };
        Ok((
            Self {
                target,
                entry,
                qpack,
            },
            identity,
        ))
    }
}
