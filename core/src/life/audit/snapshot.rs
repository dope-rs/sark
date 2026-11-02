use crate::life::audit;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Request {
    pub(super) book_entries: usize,
    pub(super) pre_admission_bytes: usize,
    pub(super) pre_admission_chunks: usize,
    pub(super) ready_nodes: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Owner {
    pub(super) bytes: usize,
    pub(super) peak_bytes: usize,
    pub(super) count: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Task {
    pub(super) budget_used: usize,
    pub(super) live: usize,
    pub(super) completion_slots: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Streams {
    pub(super) receive: usize,
    pub(super) transmit: usize,
    pub(super) ready_entries: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Response {
    pub(super) h3_owner_bytes: usize,
    pub(super) transport_owner_bytes: usize,
    pub(super) staging_owner_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub(super) known: audit::Capability,
    pub(super) request: Request,
    pub(super) owner: Owner,
    pub(super) task: Task,
    pub(super) streams: Streams,
    pub(super) response: Response,
}

impl Snapshot {
    pub const fn capabilities(self) -> audit::Capability {
        self.known
    }

    pub const fn request(
        mut self,
        entries: usize,
        bytes: usize,
        chunks: usize,
        ready: usize,
    ) -> Self {
        self.request = Request {
            book_entries: entries,
            pre_admission_bytes: bytes,
            pre_admission_chunks: chunks,
            ready_nodes: ready,
        };
        self.known = self
            .known
            .union(audit::Capability::REQUEST_BOOK_ENTRIES)
            .union(audit::Capability::PRE_ADMISSION_BYTES)
            .union(audit::Capability::PRE_ADMISSION_CHUNKS)
            .union(audit::Capability::REQUEST_READY_NODES);
        self
    }

    pub const fn owner(mut self, bytes: usize, peak: usize, count: usize) -> Self {
        self.owner = Owner {
            bytes,
            peak_bytes: peak,
            count,
        };
        self.known = self
            .known
            .union(audit::Capability::REQUEST_OWNER_BYTES)
            .union(audit::Capability::REQUEST_OWNER_PEAK_BYTES)
            .union(audit::Capability::REQUEST_OWNERS);
        self
    }

    pub const fn task(mut self, budget: usize, live: usize, slots: usize) -> Self {
        self.task = Task {
            budget_used: budget,
            live,
            completion_slots: slots,
        };
        self.known = self
            .known
            .union(audit::Capability::TASK_BUDGET_USED)
            .union(audit::Capability::LIVE_TASKS)
            .union(audit::Capability::TASK_COMPLETION_SLOTS);
        self
    }

    pub const fn streams(mut self, recv: usize, send: usize, ready: usize) -> Self {
        self.streams = Streams {
            receive: recv,
            transmit: send,
            ready_entries: ready,
        };
        self.known = self
            .known
            .union(audit::Capability::RECEIVE_STREAMS)
            .union(audit::Capability::TRANSMIT_STREAMS)
            .union(audit::Capability::CONNECTION_READY_ENTRIES);
        self
    }

    pub const fn response(mut self, h3: usize, wire: usize, stage: usize) -> Self {
        self.response = Response {
            h3_owner_bytes: h3,
            transport_owner_bytes: wire,
            staging_owner_bytes: stage,
        };
        self.known = self
            .known
            .union(audit::Capability::H3_RESPONSE_OWNER_BYTES)
            .union(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES)
            .union(audit::Capability::RESPONSE_STAGING_OWNER_BYTES);
        self
    }

    pub const fn owner_bytes(mut self, bytes: usize) -> Self {
        self.owner.bytes = bytes;
        self.known = self.known.union(audit::Capability::REQUEST_OWNER_BYTES);
        self
    }

    pub const fn owners(mut self, owners: usize) -> Self {
        self.owner.count = owners;
        self.known = self.known.union(audit::Capability::REQUEST_OWNERS);
        self
    }

    pub const fn task_slots(mut self, slots: usize) -> Self {
        self.task.completion_slots = slots;
        self.known = self.known.union(audit::Capability::TASK_COMPLETION_SLOTS);
        self
    }

    pub const fn live_tasks(mut self, tasks: usize) -> Self {
        self.task.live = tasks;
        self.known = self.known.union(audit::Capability::LIVE_TASKS);
        self
    }

    pub const fn conn_ready(mut self, entries: usize) -> Self {
        self.streams.ready_entries = entries;
        self.known = self
            .known
            .union(audit::Capability::CONNECTION_READY_ENTRIES);
        self
    }

    pub const fn wire_bytes(mut self, bytes: usize) -> Self {
        self.response.transport_owner_bytes = bytes;
        self.known = self
            .known
            .union(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES);
        self
    }

    pub const fn h3_bytes(mut self, bytes: usize) -> Self {
        self.response.h3_owner_bytes = bytes;
        self.known = self.known.union(audit::Capability::H3_RESPONSE_OWNER_BYTES);
        self
    }

    pub const fn stage_bytes(mut self, bytes: usize) -> Self {
        self.response.staging_owner_bytes = bytes;
        self.known = self
            .known
            .union(audit::Capability::RESPONSE_STAGING_OWNER_BYTES);
        self
    }

    pub const fn projection(self) -> audit::Projection {
        audit::Projection::from_parts(
            self.known,
            [
                self.request.book_entries,
                self.request.pre_admission_bytes,
                self.request.pre_admission_chunks,
                self.request.ready_nodes,
                self.owner.bytes,
                self.owner.peak_bytes,
                self.owner.count,
                self.task.budget_used,
                self.task.live,
                self.task.completion_slots,
                self.streams.receive,
                self.streams.transmit,
                self.streams.ready_entries,
                self.response.h3_owner_bytes,
                self.response.transport_owner_bytes,
                self.response.staging_owner_bytes,
            ],
        )
    }

    pub const fn is_zero(self, required: audit::Capability) -> bool {
        self.known.contains(required)
            && (!required.contains(audit::Capability::REQUEST_BOOK_ENTRIES)
                || self.request.book_entries == 0)
            && (!required.contains(audit::Capability::PRE_ADMISSION_BYTES)
                || self.request.pre_admission_bytes == 0)
            && (!required.contains(audit::Capability::PRE_ADMISSION_CHUNKS)
                || self.request.pre_admission_chunks == 0)
            && (!required.contains(audit::Capability::REQUEST_READY_NODES)
                || self.request.ready_nodes == 0)
            && (!required.contains(audit::Capability::TASK_BUDGET_USED)
                || self.task.budget_used == 0)
            && (!required.contains(audit::Capability::LIVE_TASKS) || self.task.live == 0)
            && (!required.contains(audit::Capability::REQUEST_OWNER_BYTES) || self.owner.bytes == 0)
            && (!required.contains(audit::Capability::REQUEST_OWNER_PEAK_BYTES)
                || self.owner.peak_bytes == 0)
            && (!required.contains(audit::Capability::REQUEST_OWNERS) || self.owner.count == 0)
            && (!required.contains(audit::Capability::RECEIVE_STREAMS) || self.streams.receive == 0)
            && (!required.contains(audit::Capability::TRANSMIT_STREAMS)
                || self.streams.transmit == 0)
            && (!required.contains(audit::Capability::CONNECTION_READY_ENTRIES)
                || self.streams.ready_entries == 0)
            && (!required.contains(audit::Capability::H3_RESPONSE_OWNER_BYTES)
                || self.response.h3_owner_bytes == 0)
            && (!required.contains(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES)
                || self.response.transport_owner_bytes == 0)
            && (!required.contains(audit::Capability::RESPONSE_STAGING_OWNER_BYTES)
                || self.response.staging_owner_bytes == 0)
            && (!required.contains(audit::Capability::TASK_COMPLETION_SLOTS)
                || self.task.completion_slots == 0)
    }
}

const _: () = assert!(core::mem::size_of::<Snapshot>() <= 17 * core::mem::size_of::<usize>());
