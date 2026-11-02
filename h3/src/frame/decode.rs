use std::num::NonZero;

use dope_quic::wire::varint;

use crate::{
    frame::{self, Decoded, ParseError, PayloadView, Settings},
    sources,
};

struct Pending {
    kind: varint::VarInt,
    remaining: NonZero<usize>,
}

/// The admitted remainder of one DATA or unknown frame on its receive stream.
#[derive(Default)]
pub(crate) struct Decoder {
    pending: Option<Pending>,
}

/// The next decoder state commits only after this source prefix is consumed.
pub(crate) struct Read<'a, I: sources::Chunks<'a>> {
    pub(crate) frame: Decoded<'a, I>,
    pub(crate) consumed: usize,
    pub(crate) next: Decoder,
}

impl Decoder {
    pub(crate) fn is_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub(crate) fn decode<'a, S: crate::Source + ?Sized>(
        &self,
        input: &'a S,
        limit: usize,
    ) -> Result<Read<'a, S::Chunks<'a>>, ParseError> {
        let mut cursor = crate::Source::cursor(input);
        let total = cursor.remaining();
        if let Some(pending) = &self.pending {
            if total == 0 {
                return Err(ParseError::NeedMore);
            }
            return Ok(Self::fragment(
                pending.kind,
                pending.remaining.get(),
                0,
                total,
            ));
        }
        let kind = frame::decode_varint_cursor(&mut cursor)?;
        frame::check_kind(kind)?;
        let length = frame::decode_varint_cursor(&mut cursor)?;
        let head = total - cursor.remaining();
        let Ok(len) = usize::try_from(length.get()) else {
            return Err(ParseError::FrameTooLarge);
        };
        if len > limit {
            return Err(ParseError::FrameTooLarge);
        }
        let end = head.checked_add(len).ok_or(ParseError::FrameTooLarge)?;
        let payload = || {
            cursor
                .into_span(len)
                .map(PayloadView::new)
                .ok_or(ParseError::NeedMore)
        };
        let decoded = match kind {
            frame::TYPE_HEADERS => Decoded::Headers(payload()?),
            frame::TYPE_CANCEL_PUSH => Decoded::CancelPush {
                push_id: exact(&payload()?)?,
            },
            frame::TYPE_SETTINGS => Decoded::Settings(Settings::from_view(&payload()?)?),
            frame::TYPE_PUSH_PROMISE => {
                let payload = payload()?;
                let (push_id, consumed) =
                    frame::decode_varint_source(&payload).map_err(ParseError::payload)?;
                let block = payload
                    .span
                    .suffix(consumed)
                    .map(PayloadView::new)
                    .ok_or(ParseError::NeedMore)?;
                Decoded::PushPromise { push_id, block }
            }
            frame::TYPE_GOAWAY => Decoded::GoAway {
                id: exact(&payload()?)?,
            },
            frame::TYPE_MAX_PUSH_ID => Decoded::MaxPushId {
                push_id: exact(&payload()?)?,
            },
            _ => return Ok(Self::fragment(kind, len, head, total)),
        };
        Ok(Read {
            frame: decoded,
            consumed: end,
            next: Self::default(),
        })
    }

    fn fragment<'a, I: sources::Chunks<'a>>(
        kind: varint::VarInt,
        len: usize,
        head: usize,
        total: usize,
    ) -> Read<'a, I> {
        let take = len.min(total - head);
        let consumed = head + take;
        Read {
            frame: if kind == frame::TYPE_DATA {
                Decoded::Data(head..consumed)
            } else {
                Decoded::Unknown { kind: kind.get() }
            },
            consumed,
            next: Self {
                pending: NonZero::new(len - take).map(|left| Pending {
                    kind,
                    remaining: left,
                }),
            },
        }
    }
}

fn exact<'a, I: sources::Chunks<'a>>(
    payload: &PayloadView<'a, I>,
) -> Result<varint::VarInt, ParseError> {
    let mut cursor = crate::Source::cursor(payload);
    let value = frame::decode_payload_varint(&mut cursor)?;
    if cursor.remaining() != 0 {
        return Err(ParseError::BadVarInt);
    }
    Ok(value)
}
