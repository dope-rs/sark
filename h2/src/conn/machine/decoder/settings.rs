use o3::buffer::{segments::queue, write::consume::Consume as _};
use sark_core::life::session;
use sark_http::field::metadata::section::rules;

use crate::{
    Endpoint,
    conn::{self, event, machine},
    frame::{self, layout, numbers},
    stream,
};

pub(super) fn apply<R, F, C, A, E>(
    conn: &mut machine::Connection<R, F, C, A, E>,
    header: layout::Header,
) -> Result<(), conn::Error>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    let idle = conn.egress.headers.idle().ok_or(conn::Error::HeaderBusy)?;
    let ack = layout::Header {
        length: numbers::Length::ZERO,
        kind: numbers::Type::Settings,
        flags: numbers::Flags(numbers::Flags::ACK),
        stream_id: stream::Id::CONNECTION,
    }
    .wire_bytes();
    let append = conn.egress.outbound.try_append([&ack])?;
    let mut next = conn.state.settings.peer;
    let mut minimum: Option<u32> = None;
    let before = conn.state.settings.peer.initial_window_size as i64;
    let mut adjust = false;
    let mut cursor = conn.ingress.input.bytes.cursor();
    let read = |cursor: &mut queue::Cursor<'_, C>, offset| -> Result<_, conn::Error> {
        let mut chunk = [0; 6];
        let start = frame::HEADER_LEN + offset;
        cursor.seek(start).map_err(conn::Error::Input)?;
        queue::Array::try_new(cursor, start)
            .map_err(conn::Error::Input)?
            .copy(&mut chunk);
        let id_raw = u16::from_be_bytes([chunk[0], chunk[1]]);
        let value = u32::from_be_bytes([chunk[2], chunk[3], chunk[4], chunk[5]]);
        Ok((numbers::SettingId::from_u16(id_raw), value))
    };
    let mut offset = 0;
    while offset < header.length.into_usize() {
        let (id, value) = read(&mut cursor, offset)?;
        if let Some(id) = id {
            match R::ENDPOINT {
                Endpoint::Client if id == numbers::SettingId::EnablePush && value != 0 => {
                    return Err(conn::Error::Setting { id, value });
                }
                Endpoint::Client | Endpoint::Server => {}
            }
            if id == numbers::SettingId::InitialWindowSize && value != next.initial_window_size {
                adjust = true;
            }
            next.apply(id, value)?;
            if id == numbers::SettingId::HeaderTableSize {
                minimum = Some(minimum.map_or(value, |minimum| minimum.min(value)));
            }
        }
        offset += 6;
    }
    if adjust {
        let mut previous = before;
        cursor = conn.ingress.input.bytes.cursor();
        offset = 0;
        while offset < header.length.into_usize() {
            let (id, value) = read(&mut cursor, offset)?;
            if id == Some(numbers::SettingId::InitialWindowSize) {
                let value = i64::from(value);
                let delta = value - previous;
                if delta != 0 {
                    conn.state
                        .active_streams
                        .adjust_send(delta as i32)
                        .map_err(conn::Error::Adjust)?;
                }
                previous = value;
            }
            offset += 6;
        }
    }
    conn.state.settings.peer = next;
    idle.apply(minimum, next.header_table_size);
    let total = frame::HEADER_LEN + header.length.into_usize();
    conn.ingress
        .input
        .bytes
        .try_consume(total)
        .map_err(conn::Error::Consume)?;
    append.commit();
    conn.ingress.events.push(event::Event::SettingsApplied)?;
    Ok(())
}
