//! Validated allocation and execution plans.
use std::marker;

use dope::core::random;
use dope_quic::wire::{stream::Id, varint};
use o3::{
    buffer::{pool, segments},
    depot::{
        capacity::Count,
        map,
        queue::{group, group::capacity, ring},
    },
};
use sark_http::{
    field::metadata::section::{rules, storage},
    payload::compress::dynamic::{hash, mode, plan},
};

use crate::{
    conn,
    conn::{
        blocked, event,
        io::{
            ingress::{stream::table, wait},
            write,
            write::budget,
        },
        options, output,
    },
    frame,
    qpack::{decoder, decoder::state::State, encoder, ledger},
};

#[derive(Clone)]
pub(super) struct Protocol {
    pub(super) settings: frame::Settings,
    pub(super) max_frame_bytes: usize,
}

#[derive(Clone)]
pub(super) struct Streams {
    pub(super) table: map::Plan,
    pub(super) ready: map::Plan,
    pub(super) wait: capacity::Count<2>,
}

#[derive(Clone)]
pub(super) struct Ingress {
    pub(super) event_capacity: Count,
    pub(super) retained_bytes: usize,
    pub(super) segment_capacity: usize,
}

#[derive(Clone)]
pub(super) struct Egress {
    pub(super) write_capacity: capacity::Count<2>,
    pub(super) wire_bytes: usize,
    pub(super) resident_bytes: usize,
}

#[derive(Clone)]
pub(super) struct Qpack {
    pub(super) encoder_sections: ledger::Plan,
    pub(super) tables: plan::Plans,
    pub(super) blocked: blocked::Plan,
    pub(super) receive_credit_capacity: Count,
    pub(super) max_field_section_bytes: usize,
    pub(super) field_pool: pool::Layout,
}

#[derive(Clone)]
pub struct Config {
    pub(super) protocol: Protocol,
    pub(super) streams: Streams,
    pub(super) ingress: Ingress,
    pub(super) egress: Egress,
    pub(super) qpack: Qpack,
}

impl Config {
    pub fn new(options: options::Config) -> Result<Self, options::Error> {
        use std::alloc::Layout;

        fn validate_write(value: usize) -> Result<capacity::Count<2>, options::Error> {
            let count = capacity::Count::try_from(value).map_err(|error| {
                let value = error.requested();
                let error = match error {
                    capacity::Error::Domain(error) => options::Bound::Depot(error),
                    capacity::Error::Tagged { requested: value } => {
                        options::Bound::Tagged { requested: value }
                    }
                };
                options::Error::Capacity {
                    resource: options::Resource::Write,
                    requested: value,
                    error,
                }
            })?;
            Layout::array::<output::Write<'_>>(value).map_err(|error| {
                options::Error::Capacity {
                    resource: options::Resource::Write,
                    requested: value,
                    error: options::Bound::Layout(error),
                }
            })?;
            Ok(count)
        }

        fn validate_event(capacity: usize) -> Result<(), options::Error> {
            Layout::array::<event::Event<'_>>(capacity).map_err(|error| {
                options::Error::Capacity {
                    resource: options::Resource::Event,
                    requested: capacity,
                    error: options::Bound::Layout(error),
                }
            })?;
            Ok(())
        }

        let options::Config {
            protocol,
            streams,
            ingress,
            egress,
            qpack,
        } = options;
        for (resource, capacity) in [
            (options::Resource::Frame, protocol.max_frame_bytes),
            (options::Resource::Stream, streams.capacity),
            (options::Resource::Event, ingress.event_capacity),
            (options::Resource::Write, egress.write_capacity),
            (
                options::Resource::QpackSection,
                qpack.encoder.section_capacity,
            ),
            (options::Resource::InboundByte, ingress.retained_bytes),
            (options::Resource::InboundSegment, ingress.retained_segments),
            (options::Resource::OutboundByte, egress.wire_bytes),
            (
                options::Resource::OutboundResidentByte,
                egress.resident_bytes,
            ),
        ] {
            if capacity == 0 {
                return Err(options::Error::Zero(resource));
            }
        }
        for (resource, actual) in [
            (options::Resource::Write, egress.write_capacity),
            (options::Resource::OutboundByte, egress.wire_bytes),
            (
                options::Resource::OutboundResidentByte,
                egress.resident_bytes,
            ),
        ] {
            if actual < 2 {
                return Err(options::Error::Below {
                    resource,
                    actual,
                    minimum: 2,
                });
            }
        }
        if qpack.decoder.blocked_streams > streams.capacity {
            return Err(options::Error::Exceeds {
                resource: options::Resource::BlockedStream,
                actual: qpack.decoder.blocked_streams,
                limit: options::Resource::Stream,
                maximum: streams.capacity,
            });
        }
        let decoder = varint::VarInt::from_usize(qpack.decoder.table_bytes).ok_or(
            options::Error::Capacity {
                resource: options::Resource::QpackDecoderTable,
                requested: qpack.decoder.table_bytes,
                error: options::Bound::Wire,
            },
        )?;
        let blocked = varint::VarInt::from_usize(qpack.decoder.blocked_streams).ok_or(
            options::Error::Capacity {
                resource: options::Resource::BlockedStream,
                requested: qpack.decoder.blocked_streams,
                error: options::Bound::Wire,
            },
        )?;
        let fields = varint::VarInt::from_usize(ingress.field_section_bytes).ok_or(
            options::Error::Capacity {
                resource: options::Resource::FieldSection,
                requested: ingress.field_section_bytes,
                error: options::Bound::Wire,
            },
        )?;
        let settings =
            frame::Settings::new(decoder, Some(fields), blocked, protocol.extended_connect);
        let table = map::Plan::new(streams.capacity).map_err(|error| options::Error::Capacity {
            resource: options::Resource::Stream,
            requested: error.requested(),
            error: options::Bound::Depot(error),
        })?;
        let slots = table.capacity().get() + 3;
        let ready = map::Plan::new(slots).map_err(|error| options::Error::Capacity {
            resource: options::Resource::ReadyStream,
            requested: error.requested(),
            error: options::Bound::Depot(error),
        })?;
        let wait = capacity::Count::try_from(slots).map_err(|error| options::Error::Capacity {
            resource: options::Resource::ReadyStream,
            requested: error.requested(),
            error: match error {
                capacity::Error::Domain(error) => options::Bound::Depot(error),
                capacity::Error::Tagged { requested: value } => {
                    options::Bound::Tagged { requested: value }
                }
            },
        })?;
        Layout::array::<Id>(slots).map_err(|error| options::Error::Capacity {
            resource: options::Resource::ReadyStream,
            requested: slots,
            error: options::Bound::Layout(error),
        })?;
        let writes = validate_write(egress.write_capacity)?;
        let events =
            Count::try_from(ingress.event_capacity).map_err(|error| options::Error::Capacity {
                resource: options::Resource::Event,
                requested: error.requested(),
                error: options::Bound::Depot(error),
            })?;
        validate_event(events.get())?;
        let segments =
            ingress
                .retained_segments
                .checked_mul(2)
                .ok_or(options::Error::Arithmetic {
                    resource: options::Resource::InboundSegment,
                    operation: options::Arithmetic::Mul,
                    left: ingress.retained_segments,
                    right: 2,
                })?;
        u32::try_from(segments).map_err(|error| options::Error::Capacity {
            resource: options::Resource::InboundSegment,
            requested: segments,
            error: options::Bound::Integer(error),
        })?;
        let sections = ledger::Plan::new(qpack.encoder.section_capacity)?;
        let blocked = blocked::Plan::new(qpack.decoder.blocked_streams).map_err(|error| {
            options::Error::Capacity {
                resource: options::Resource::BlockedStream,
                requested: error.requested(),
                error: options::Bound::Map(error),
            }
        })?;
        let credits = blocked.capacity() + 1;
        let credits = Count::try_from(credits).map_err(|error| options::Error::Capacity {
            resource: options::Resource::ReceiveCredit,
            requested: error.requested(),
            error: options::Bound::Depot(error),
        })?;
        Layout::array::<(Id, usize)>(credits.get()).map_err(|error| options::Error::Capacity {
            resource: options::Resource::ReceiveCredit,
            requested: credits.get(),
            error: options::Bound::Layout(error),
        })?;
        let encoder =
            plan::Plan::<mode::Direct>::new(qpack.encoder.table_bytes).map_err(|error| {
                options::Error::Table {
                    resource: options::Resource::QpackEncoderTable,
                    requested: qpack.encoder.table_bytes,
                    error,
                }
            })?;
        let decoder =
            plan::Plan::<mode::Staged>::new(qpack.decoder.table_bytes).map_err(|error| {
                options::Error::Table {
                    resource: options::Resource::QpackDecoderTable,
                    requested: qpack.decoder.table_bytes,
                    error,
                }
            })?;
        let tables =
            plan::Plans::try_from((encoder, decoder)).map_err(|error| options::Error::Tables {
                encoder: qpack.encoder.table_bytes,
                decoder: qpack.decoder.table_bytes,
                error,
            })?;
        let fields = pool::Layout::new(
            decoder::Decoder::DEFAULT_FIELD_BLOCKS,
            ingress.field_section_bytes.max(1),
        )
        .map_err(|error| options::Error::Pool {
            slots: decoder::Decoder::DEFAULT_FIELD_BLOCKS,
            capacity: ingress.field_section_bytes.max(1),
            error,
        })?;
        Ok(Self {
            protocol: Protocol {
                settings,
                max_frame_bytes: protocol.max_frame_bytes,
            },
            streams: Streams { table, ready, wait },
            ingress: Ingress {
                event_capacity: events,
                retained_bytes: ingress.retained_bytes,
                segment_capacity: segments,
            },
            egress: Egress {
                write_capacity: writes,
                wire_bytes: egress.wire_bytes,
                resident_bytes: egress.resident_bytes,
            },
            qpack: Qpack {
                encoder_sections: sections,
                tables,
                blocked,
                receive_credit_capacity: credits,
                max_field_section_bytes: ingress.field_section_bytes,
                field_pool: fields,
            },
        })
    }

    pub fn settings(&self) -> &frame::Settings {
        &self.protocol.settings
    }

    pub(crate) fn set_connect(&mut self) {
        self.protocol.settings.enable();
    }

    pub(crate) const fn stream_plan(&self) -> map::Plan {
        self.streams.table
    }
    pub(crate) const fn ready_plan(&self) -> map::Plan {
        self.streams.ready
    }

    pub const fn footprint(&self) -> plan::Footprint {
        self.qpack.tables.footprint()
    }

    pub fn build<'d, P, A>(
        self,
        role: event::Role,
    ) -> Result<conn::Connection<'d, P, A>, options::Error>
    where
        P: rules::Policy,
        A: rules::Push,
    {
        let seed = random::Seed::acquire().map_err(options::Error::Entropy)?;
        self.build_seeded(role, hash::Seed::new(&seed))
    }

    /// Builds from an owning driver's already acquired entropy.
    pub fn build_seeded<'d, P, A>(
        self,
        role: event::Role,
        seed: hash::Seed,
    ) -> Result<conn::Connection<'d, P, A>, options::Error>
    where
        P: rules::Policy,
        A: rules::Push,
    {
        let Self {
            protocol,
            streams,
            ingress,
            egress,
            qpack,
        } = self;
        let table = table::Table::try_new(streams.ready, streams.table)?;
        let blocked = blocked::Blocked::try_new(qpack.blocked)?;
        let field_pool =
            storage::try_layout::<P::Block>(qpack.field_pool, qpack.max_field_section_bytes)
                .map_err(|(capacity, error)| options::Error::Pool {
                    slots: qpack.field_pool.slots() as usize,
                    capacity,
                    error,
                })?;
        Ok(conn::Connection {
            settings: conn::Settings {
                role,
                local_settings: protocol.settings,
                peer_settings: None,
                max_frame_size: protocol.max_frame_bytes,
            },
            qpack: conn::QpackState {
                qpack_encoder: encoder::State::try_plan(
                    qpack.tables.encoder(),
                    qpack.encoder_sections,
                    seed,
                )?,
                pending_encoder: None,
                qpack_decoder: decoder::Decoder::try_plans(
                    qpack.max_field_section_bytes,
                    qpack.tables.decoder(),
                    field_pool,
                )?,
                feedback: State::new(),
                blocked,
            },
            session: conn::SessionState::new(
                streams.table,
                match role {
                    event::Role::Client => ingress.retained_bytes,
                    event::Role::Server => egress.resident_bytes,
                },
            ),
            streams: table,
            queues: write::Queues {
                waiters: wait::Table::try_new(streams.wait)?,
                receive_credits: ring::Fifo::try_new(qpack.receive_credit_capacity)?,
                events: ring::Fifo::try_new(ingress.event_capacity)?,
                cancellations: 0,
                writes: group::Ledger::try_new(egress.write_capacity)
                    .map_err(options::Error::Queue)?,
                regular_write_limit: write::regular_limit(egress.write_capacity.get(), 64),
                write_budget: budget::WriteBudget::new(egress.wire_bytes, egress.resident_bytes),
            },
            ingress: conn::IngressState {
                budget: conn::ByteBudget::new(ingress.retained_bytes),
                segments: segments::Pool::try_new(segments::Plan {
                    segments: ingress.segment_capacity,
                    chains: ingress.segment_capacity / 2,
                })?,
                data_credit: conn::CreditMode::Open,
            },
            validation: marker::PhantomData,
        })
    }
}
