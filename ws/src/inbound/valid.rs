use std::str;

use crate::{
    fragment, frame,
    inbound::{Close, Fault, Peer},
};

pub(super) enum Kind {
    Data(fragment::Admission),
    Close,
    Ping,
    Pong,
}

pub(super) fn check<P: Peer>(
    parts: &fragment::Buffer,
    head: &frame::Head,
) -> Result<(Kind, P::Witness), Fault> {
    let witness = P::witness(head.mask)?;

    let kind = match head.opcode {
        0x0..=0x2 => {
            let proof = parts
                .admit(head.opcode, head.fin, head.payload_len())
                .map_err(Fault::Fragment)?;
            Kind::Data(proof)
        }
        0x8 => control(head, Kind::Close)?,
        0x9 => control(head, Kind::Ping)?,
        0xa => control(head, Kind::Pong)?,
        _ => {
            return Err(Fault::Opcode {
                actual: head.opcode,
            });
        }
    };
    Ok((kind, witness))
}

fn control(head: &frame::Head, kind: Kind) -> Result<Kind, Fault> {
    if !head.fin || head.payload_len() > frame::MAX_CONTROL_PAYLOAD {
        return Err(Fault::Control {
            fin: head.fin,
            length: head.payload_len(),
        });
    }
    Ok(kind)
}

pub(super) fn decode(data: &[u8]) -> Result<Close<'_>, Fault> {
    let (code, text) = match data {
        [] => {
            return Ok(Close {
                payload: data,
                code: None,
                reason: "",
            });
        }
        [_] => {
            return Err(Fault::CloseLength {
                actual: 1,
                required: 2,
            });
        }
        [high, low, text @ ..] => (u16::from_be_bytes([*high, *low]), text),
    };
    if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999) {
        return Err(Fault::CloseCode { actual: code });
    }
    let reason = str::from_utf8(text)
        .map_err(fragment::Utf8::from)
        .map_err(Fault::CloseText)?;
    Ok(Close {
        payload: data,
        code: Some(code),
        reason,
    })
}
