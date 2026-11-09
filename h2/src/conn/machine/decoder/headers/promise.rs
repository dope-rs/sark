use conn::{
    event, ingress,
    ingress::{header, headers},
    outputs, registry,
};
use registry::store;
use sark_core::life::session;
use sark_http::field::metadata::section::rules;

use crate::{
    conn,
    conn::machine::{self, decoder, ops::state},
    frame::{self, numbers},
    stream,
};

pub(in crate::conn::machine::decoder) struct Promise {
    stream_id: stream::Nonzero,
    promised: stream::Nonzero,
}

impl Promise {
    pub(in crate::conn::machine::decoder) const fn new(
        id: stream::Nonzero,
        promised: stream::Nonzero,
    ) -> Self {
        Self {
            stream_id: id,
            promised,
        }
    }

    pub(super) fn commit<R, F, C, E>(
        self,
        assoc: registry::Association,
        outbound: &mut outputs::Outbound<C>,
        state: &mut state::State<R>,
        prepared: header::Prepared<'_, F, C, E>,
    ) -> Result<(), conn::Error>
    where
        R: crate::Role,
        F: rules::Policy,
        C: session::Storage,
        E: event::Evidence,
    {
        let state::State {
            settings,
            active_streams: streams,
            stream_history: seen,
            lifecycle,
            ..
        } = state;
        let goaway = lifecycle.is_goaway();
        let state::Settings { local, peer } = settings;
        match assoc {
            registry::Association => {}
        }
        let Self {
            stream_id: stream,
            promised,
        } = self;
        let id = stream.get();
        let target = promised.get();
        let limit = match local.max_concurrent_streams {
            Some(limit) => limit as usize,
            None => usize::MAX,
        };
        let (vacant, opening) = match streams.admit(seen, promised, limit, |_, _| {
            Ok::<_, conn::Error>(store::Action::Keep(()))
        })? {
            store::Admission::Exhausted(id) => {
                prepared.discard();
                return Err(conn::Error::Admission(conn::Admission::IdentityExhausted(
                    id,
                )));
            }
            store::Admission::Active(()) | store::Admission::Past(_) => {
                prepared.discard();
                return Err(conn::Error::Protocol(conn::Protocol::Peer {
                    actual: target,
                    last: seen.last_peer_id(),
                    endpoint: R::ENDPOINT,
                }));
            }
            store::Admission::Full(opening) => {
                let error = match prepared.validation() {
                    header::Validation::Valid => numbers::ErrorCode::RefusedStream,
                    header::Validation::Rejected { .. } => numbers::ErrorCode::ProtocolError,
                };
                prepared.discard();
                opening.commit();
                return outbound.reset(target, error);
            }
            store::Admission::Open(vacant, opening) if !goaway => (vacant, opening),
            store::Admission::Open(_, opening) => {
                prepared.discard();
                opening.commit();
                return outbound.reset(target, numbers::ErrorCode::RefusedStream);
            }
        };
        match prepared.validation() {
            header::Validation::Valid => {}
            header::Validation::Rejected { .. } => {
                prepared.discard();
                opening.commit();
                return outbound.reset(target, numbers::ErrorCode::ProtocolError);
            }
        }
        let mut lifecycle = stream::Lifecycle::reserved();
        lifecycle.method = prepared.method();
        let key = vacant.insert(
            lifecycle,
            peer.initial_window_size,
            local.initial_window_size,
            0,
            false,
        );
        opening.commit();
        prepared.promise(id, key);
        Ok(())
    }

    pub(in crate::conn::machine::decoder) fn handle<
        R: crate::Role,
        F: rules::Policy,
        C: session::Storage,
        A: rules::Push,
        E: event::Evidence,
    >(
        self,
        decoder: &mut decoder::Decoder<'_, R, F, C, A, E>,
        chain: frame::Block,
        start: usize,
        len: usize,
        total: usize,
    ) -> Result<(), conn::Error> {
        let Self {
            stream_id: stream,
            promised,
        } = self;
        decoder
            .conn
            .state
            .admit_push()
            .map_err(|cause| conn::Error::Protocol(conn::Protocol::Push(cause)))?;
        let assoc = decoder
            .conn
            .state
            .active_streams
            .association(&decoder.conn.state.stream_history, stream)
            .map_err(|cause| conn::Error::Protocol(conn::Protocol::Association(cause)))?;
        if matches!(chain, frame::Block::End) {
            let machine::Connection {
                ingress,
                egress,
                state,
                ..
            } = &mut *decoder.conn;
            let ingress::Ingress {
                input,
                events,
                headers,
                ..
            } = &mut *ingress;
            let mut input = header::Input::Queued(input);
            let prepared = headers.prepare::<C, A, E>(
                &mut input,
                events,
                headers::Fragment {
                    start,
                    len,
                    total,
                    kind: rules::Kind::PushPromise,
                },
            )?;
            Self {
                stream_id: stream,
                promised,
            }
            .commit::<R, F, C, E>(assoc, &mut egress.outbound, state, prepared)?;
            return Ok(());
        }
        let pending = headers::Pending {
            stream_id: stream,
            kind: headers::Kind::PushPromise {
                promised,
                associated: assoc,
            },
            continuations: headers::Continuation::ZERO,
        };
        let ingress = &mut decoder.conn.ingress;
        let ingress::Ingress { input, headers, .. } = ingress;
        let mut input = header::Input::Queued(input);
        headers.begin::<C, A>(
            Some(rules::Kind::PushPromise),
            pending,
            &mut input,
            headers::Fragment {
                start,
                len,
                total,
                kind: rules::Kind::PushPromise,
            },
        )
    }
}
