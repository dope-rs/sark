use crate::life::audit;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub(super) known: audit::Capability,
    pub(super) values: [usize; 16],
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
        self.values[0] = entries;
        self.values[1] = bytes;
        self.values[2] = chunks;
        self.values[3] = ready;
        self.known = self
            .known
            .union(audit::Capability::REQUEST_BOOK_ENTRIES)
            .union(audit::Capability::PRE_ADMISSION_BYTES)
            .union(audit::Capability::PRE_ADMISSION_CHUNKS)
            .union(audit::Capability::REQUEST_READY_NODES);
        self
    }

    pub const fn owner(mut self, bytes: usize, peak: usize, count: usize) -> Self {
        self.values[4] = bytes;
        self.values[5] = peak;
        self.values[6] = count;
        self.known = self
            .known
            .union(audit::Capability::REQUEST_OWNER_BYTES)
            .union(audit::Capability::REQUEST_OWNER_PEAK_BYTES)
            .union(audit::Capability::REQUEST_OWNERS);
        self
    }

    pub const fn task(mut self, budget: usize, live: usize, slots: usize) -> Self {
        self.values[7] = budget;
        self.values[8] = live;
        self.values[9] = slots;
        self.known = self
            .known
            .union(audit::Capability::TASK_BUDGET_USED)
            .union(audit::Capability::LIVE_TASKS)
            .union(audit::Capability::TASK_COMPLETION_SLOTS);
        self
    }

    pub const fn streams(mut self, recv: usize, send: usize, ready: usize) -> Self {
        self.values[10] = recv;
        self.values[11] = send;
        self.values[12] = ready;
        self.known = self
            .known
            .union(audit::Capability::RECEIVE_STREAMS)
            .union(audit::Capability::TRANSMIT_STREAMS)
            .union(audit::Capability::CONNECTION_READY_ENTRIES);
        self
    }

    pub const fn response(mut self, h3: usize, wire: usize, stage: usize) -> Self {
        self.values[13] = h3;
        self.values[14] = wire;
        self.values[15] = stage;
        self.known = self
            .known
            .union(audit::Capability::H3_RESPONSE_OWNER_BYTES)
            .union(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES)
            .union(audit::Capability::RESPONSE_STAGING_OWNER_BYTES);
        self
    }

    pub const fn owner_bytes(mut self, bytes: usize) -> Self {
        self.values[4] = bytes;
        self.known = self.known.union(audit::Capability::REQUEST_OWNER_BYTES);
        self
    }

    pub const fn owners(mut self, owners: usize) -> Self {
        self.values[6] = owners;
        self.known = self.known.union(audit::Capability::REQUEST_OWNERS);
        self
    }

    pub const fn task_slots(mut self, slots: usize) -> Self {
        self.values[9] = slots;
        self.known = self.known.union(audit::Capability::TASK_COMPLETION_SLOTS);
        self
    }

    pub const fn live_tasks(mut self, tasks: usize) -> Self {
        self.values[8] = tasks;
        self.known = self.known.union(audit::Capability::LIVE_TASKS);
        self
    }

    pub const fn conn_ready(mut self, entries: usize) -> Self {
        self.values[12] = entries;
        self.known = self
            .known
            .union(audit::Capability::CONNECTION_READY_ENTRIES);
        self
    }

    pub const fn wire_bytes(mut self, bytes: usize) -> Self {
        self.values[14] = bytes;
        self.known = self
            .known
            .union(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES);
        self
    }

    pub const fn h3_bytes(mut self, bytes: usize) -> Self {
        self.values[13] = bytes;
        self.known = self.known.union(audit::Capability::H3_RESPONSE_OWNER_BYTES);
        self
    }

    pub const fn stage_bytes(mut self, bytes: usize) -> Self {
        self.values[15] = bytes;
        self.known = self
            .known
            .union(audit::Capability::RESPONSE_STAGING_OWNER_BYTES);
        self
    }

    pub const fn projection(self) -> audit::Projection {
        audit::Projection::from_parts(self.known, self.values)
    }

    pub const fn is_zero(self, required: audit::Capability) -> bool {
        self.known.contains(required)
            && (!required.contains(audit::Capability::REQUEST_BOOK_ENTRIES) || self.values[0] == 0)
            && (!required.contains(audit::Capability::PRE_ADMISSION_BYTES) || self.values[1] == 0)
            && (!required.contains(audit::Capability::PRE_ADMISSION_CHUNKS) || self.values[2] == 0)
            && (!required.contains(audit::Capability::REQUEST_READY_NODES) || self.values[3] == 0)
            && (!required.contains(audit::Capability::TASK_BUDGET_USED) || self.values[7] == 0)
            && (!required.contains(audit::Capability::LIVE_TASKS) || self.values[8] == 0)
            && (!required.contains(audit::Capability::REQUEST_OWNER_BYTES) || self.values[4] == 0)
            && (!required.contains(audit::Capability::REQUEST_OWNER_PEAK_BYTES)
                || self.values[5] == 0)
            && (!required.contains(audit::Capability::REQUEST_OWNERS) || self.values[6] == 0)
            && (!required.contains(audit::Capability::RECEIVE_STREAMS) || self.values[10] == 0)
            && (!required.contains(audit::Capability::TRANSMIT_STREAMS) || self.values[11] == 0)
            && (!required.contains(audit::Capability::CONNECTION_READY_ENTRIES)
                || self.values[12] == 0)
            && (!required.contains(audit::Capability::H3_RESPONSE_OWNER_BYTES)
                || self.values[13] == 0)
            && (!required.contains(audit::Capability::TRANSPORT_RESPONSE_OWNER_BYTES)
                || self.values[14] == 0)
            && (!required.contains(audit::Capability::RESPONSE_STAGING_OWNER_BYTES)
                || self.values[15] == 0)
            && (!required.contains(audit::Capability::TASK_COMPLETION_SLOTS) || self.values[9] == 0)
    }
}

const _: () = assert!(core::mem::size_of::<Snapshot>() <= 17 * core::mem::size_of::<usize>());
