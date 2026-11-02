use conn::{
    event,
    ingress::{events, input},
    outputs, registry,
};
use o3::buffer::{
    self,
    hold::{self, scope},
    resident,
};
use sark_core::{
    life::session,
    manifold::stream::{Boundary, Section},
};
use sark_http::{
    self,
    field::metadata::section::{rules, state::Selected},
};

use crate::{
    conn,
    conn::machine::{self, ops::state},
    flow,
    frame::{self, controls, layout, numbers, payload},
    stream,
};

#[repr(transparent)]
pub(in crate::conn::machine) struct Data<'a, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    conn: &'a mut machine::Connection<R, F, C, A, E>,
}

impl<'a, R, F, C, A, E> Data<'a, R, F, C, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    A: rules::Push,
    E: event::Evidence,
{
    pub(in crate::conn::machine) fn new(conn: &'a mut machine::Connection<R, F, C, A, E>) -> Self {
        Self { conn }
    }

    pub(super) fn apply_data(&mut self, header: layout::Header) -> Result<(), conn::Error> {
        let machine::Connection {
            ingress,
            egress,
            state,
            ..
        } = &mut *self.conn;
        apply::<R, F, C, E, _>(
            header,
            &mut ingress.input,
            &mut ingress.events,
            egress,
            state,
        )
    }

    pub(super) fn apply_window(
        &mut self,
        parsed: payload::WindowUpdate,
    ) -> Result<(), conn::Error> {
        let Some(proof) = parsed.stream_id.nonzero() else {
            let blocked = self.conn.state.flow.send_window.available() == 0;
            self.conn
                .state
                .flow
                .send_window
                .increase(parsed.increment)
                .map_err(conn::Error::Increase)?;
            if blocked && self.conn.state.flow.send_window.available() != 0 {
                self.conn.state.flow.mark_open();
            }
            return Ok(());
        };
        let id = proof.get();
        let state::State {
            active_streams: active,
            stream_history: history,
            ..
        } = &mut self.conn.state;
        match active.add_send(history, proof, parsed.increment)? {
            registry::Update::Applied => Ok(()),
            registry::Update::Past(registry::Past::Idle) => {
                Err(conn::Error::Protocol(conn::Protocol::Idle(id)))
            }
            registry::Update::Past(registry::Past::Closed) => Ok(()),
        }
    }

    pub(super) fn apply_reset(&mut self, r: controls::RstStream) -> Result<(), conn::Error> {
        let proof = r.stream();
        let id = proof.get();
        use registry::store::Lookup;
        let (key, resets) = match self
            .conn
            .state
            .active_streams
            .store
            .lookup(&self.conn.state.stream_history, proof)
        {
            Lookup::Class(registry::Past::Idle) => {
                return Err(conn::Error::Protocol(conn::Protocol::Idle(id)));
            }
            Lookup::Class(registry::Past::Closed) => {
                let retired = self
                    .conn
                    .state
                    .retire_write::<C, E>(&mut self.conn.egress, id);
                if let Some(write) = retired {
                    self.conn
                        .ingress
                        .events
                        .push(event::Event::EgressRetired { write })?;
                }
                return Ok(());
            }
            Lookup::Active(key, record) => {
                let resets = self.conn.state.lifecycle.peer_reset_count.next()?;
                record.stream.state = stream::State::Closed;
                (key, resets)
            }
        };
        let retired = self
            .conn
            .state
            .retire_write::<_, E>(&mut self.conn.egress, key.id());
        self.conn.state.active_streams.remove_key(key);
        self.conn.ingress.events.push(event::Event::StreamReset {
            stream_id: key,
            error: r.error(),
            retired,
            validation: None,
        })?;
        self.conn.state.lifecycle.peer_reset_count = resets;
        Ok(())
    }
}

impl<'d, R, F, A, E> Data<'_, R, F, resident::Shared<'d>, A, E>
where
    R: crate::Role,
    F: rules::Policy,
    A: rules::Push,
    E: event::Evidence,
{
    pub(in crate::conn::machine) fn apply_scoped<S: buffer::Cursor + hold::raw::Retain<'d>, B>(
        &mut self,
        header: layout::Header,
        source: &scope::Input<'_, 'd, S, B>,
        offset: usize,
        consumed: &mut usize,
    ) -> Result<(), conn::Error> {
        let machine::Connection {
            ingress,
            egress,
            state,
            ..
        } = &mut *self.conn;
        let mut input = ingress.input.borrowed(source, offset, consumed)?;
        apply::<R, F, resident::Shared<'d>, E, _>(
            header,
            &mut input,
            &mut ingress.events,
            egress,
            state,
        )
    }
}

fn apply<R, F, C, E, I>(
    header: layout::Header,
    input: &mut I,
    events: &mut events::Events<Selected<F>, F::Block, C, E>,
    egress: &mut outputs::Egress<C>,
    state: &mut state::State<R>,
) -> Result<(), conn::Error>
where
    R: crate::Role,
    F: rules::Policy,
    C: session::Storage,
    E: event::Evidence,
    I: input::Source<C>,
{
    use numbers::Flags;

    let proof = header.try_stream().map_err(conn::Error::ParseError)?;
    let (start, len) = input.unpadded(header)?;
    let content = start..start + len;
    let flow = header.length;
    let id = proof.get();
    let boundary = if header.flags.contains(Flags::END_STREAM) {
        Boundary::End
    } else {
        Boundary::More
    };
    let total = frame::HEADER_LEN + flow.into_usize();

    let conn_low = state.flow.recv_window_target.low();
    let flow_low = (state.settings.local.initial_window_size / 2).max(1);
    let state::State {
        flow:
            state::Flow {
                recv_window: recv,
                pending_release: pending,
                ..
            },
        active_streams: active,
        stream_history: history,
        ..
    } = &mut *state;
    use registry::store::Lookup;

    let (key, record) = match active.store.lookup(history, proof) {
        Lookup::Active(key, record) => (key, record),
        Lookup::Class(registry::Past::Idle) => {
            return input
                .prepare(total)?
                .reject(conn::Error::Protocol(conn::Protocol::Idle(id)));
        }
        Lookup::Class(registry::Past::Closed) => {
            let receive = match recv.receive_data(flow, pending, conn_low) {
                Ok(receive) => receive,
                Err(error) => {
                    return input.prepare(total)?.reject(conn::Error::from(error));
                }
            };
            let frame = input.prepare(total)?;
            egress.outbound.data_control(
                proof,
                receive.bump(),
                None,
                Some(numbers::ErrorCode::StreamClosed),
            )?;
            receive.commit();
            frame.commit();
            return Ok(());
        }
    };
    let (window, release) = (&mut record.recv_window, &mut record.pending_release);
    let next = match record.stream.state.recv(stream::Event::Data { boundary }) {
        Ok(next) if matches!(record.stream.peer_section, Section::Trailing) => Ok(next),
        Ok(_) => Err(numbers::ErrorCode::ProtocolError),
        Err(stream::Error::StreamClosed { .. }) => Err(numbers::ErrorCode::StreamClosed),
        Err(error @ stream::Error::Protocol { .. }) => {
            return input
                .prepare(total)?
                .reject(conn::Error::Transition(id, error));
        }
    };
    let next = next.and_then(|next| {
        record
            .stream
            .content
            .receive(record.remaining, len, matches!(boundary, Boundary::End))
            .map_err(numbers::ErrorCode::from)
            .map(|remaining| (next, remaining))
    });
    let (next, remaining) = match next {
        Ok(next) => next,
        Err(error) => {
            let receive = match recv.receive_data(flow, pending, conn_low) {
                Ok(receive) => receive,
                Err(error) => {
                    return input.prepare(total)?.reject(conn::Error::from(error));
                }
            };
            let reset = events.vacant()?;
            let frame = input.prepare(total)?;
            egress
                .outbound
                .data_control(proof, receive.bump(), None, Some(error))?;
            receive.commit();
            frame.commit();
            let retired = state.retire::<C, E>(egress, key);
            events::Reset::new(reset, None).commit(key, error, retired);
            return Ok(());
        }
    };
    let receive = flow::Pair {
        conn: recv,
        stream: window,
    }
    .receive_data(
        flow,
        flow::ReleasePair {
            conn: pending,
            stream: release,
            conn_threshold: conn_low,
            stream_threshold: flow_low,
        },
        boundary,
    );
    let (conn, stream) = match receive {
        Ok(flow::Admission {
            conn,
            stream: Ok(stream),
        }) => (conn, stream),
        Ok(flow::Admission {
            conn: credit,
            stream: Err(cause),
        }) => {
            let reset = events.vacant()?;
            let frame = input.prepare(total)?;
            let error = conn::Error::Receive(cause).code();
            egress
                .outbound
                .data_control(proof, credit.bump(), None, Some(error))?;
            credit.commit();
            frame.commit();
            let retired = state.retire::<C, E>(egress, key);
            events::Reset::new(reset, None).commit(key, error, retired);
            return Ok(());
        }
        Err(error) => {
            return input.prepare(total)?.reject(conn::Error::Receive(error));
        }
    };
    let data = input.prepare_data(events, content.start, content.len(), total)?;
    let conn_up = conn.bump();
    let peer_up = stream.bump();
    if conn_up.is_some() || peer_up.is_some() {
        egress
            .outbound
            .data_control(proof, conn_up, peer_up, None)?;
    }
    conn.commit();
    stream.commit();
    record.stream.state = next;
    record.remaining = remaining;
    data.frame.commit();
    data.event.push_back(event::Event::Data {
        stream_id: key,
        data: data.payload,
        boundary,
    });
    if next == stream::State::Closed {
        active.remove_key(key);
    }
    Ok(())
}
