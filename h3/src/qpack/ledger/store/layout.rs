use o3::depot::{capacity, map::compact};

use crate::qpack::ledger;

#[derive(Clone, Copy)]
pub(in crate::qpack::ledger) enum Node {
    Owner,
    Next,
    HeapPosition,
    BlockPrevious,
    BlockNext,
}

#[derive(Clone, Copy)]
pub(in crate::qpack::ledger) enum Count {
    Required,
    Minimum,
}

#[derive(Clone, Copy)]
pub(in crate::qpack::ledger) enum Queue {
    Head,
    Tail,
    Blocking,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Nodes {
    next: capacity::Count,
    owner: capacity::Count,
    position: capacity::Count,
    heap: capacity::Count,
    previous: capacity::Count,
    following: capacity::Count,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Counts {
    required: capacity::Count,
    minimum: capacity::Count,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Queues {
    head: capacity::Count,
    tail: capacity::Count,
    blocking: capacity::Count,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Layout {
    capacity: capacity::Count,
    nodes: Nodes,
    counts: Counts,
    pub(in crate::qpack::ledger::store) stream_id: capacity::Count,
    queues: Queues,
    index: compact::Layout,
}

impl Layout {
    pub(super) fn new(capacity: capacity::Count) -> Result<Self, ledger::Error> {
        let count = capacity.get();
        let mut cursor = Cursor(0);
        let [next, owner, position, heap, previous, after] =
            cursor.planes::<6>(count, size_of::<u32>())?;
        let nodes = Nodes {
            next,
            owner,
            position,
            heap,
            previous,
            following: after,
        };
        let [required, minimum] = cursor.planes::<2>(count, size_of::<u64>())?;
        let counts = Counts { required, minimum };
        let ids = cursor.plane(count, size_of::<u64>())?;
        let [head, tail, blocking] = cursor.planes::<3>(count, size_of::<u32>())?;
        let queues = Queues {
            head,
            tail,
            blocking,
        };
        let (index, end) =
            compact::Layout::try_new(count, cursor.0).map_err(ledger::Error::Index)?;
        capacity::Count::try_from(end).map_err(|error| ledger::Error::Length {
            len: error.requested(),
        })?;
        Ok(Self {
            capacity,
            nodes,
            counts,
            stream_id: ids,
            queues,
            index,
        })
    }

    pub(super) const fn capacity(self) -> usize {
        self.capacity.get()
    }

    pub(super) const fn capacity_raw(self) -> u32 {
        self.capacity.raw()
    }

    pub(super) const fn node(self, plane: Node) -> capacity::Count {
        match plane {
            Node::Owner => self.nodes.owner,
            Node::Next => self.nodes.next,
            Node::HeapPosition => self.nodes.position,
            Node::BlockPrevious => self.nodes.previous,
            Node::BlockNext => self.nodes.following,
        }
    }

    pub(super) const fn heap(self) -> capacity::Count {
        self.nodes.heap
    }

    pub(super) const fn count(self, plane: Count) -> capacity::Count {
        match plane {
            Count::Required => self.counts.required,
            Count::Minimum => self.counts.minimum,
        }
    }

    pub(super) const fn queue_start(self, plane: Queue) -> capacity::Count {
        match plane {
            Queue::Head => self.queues.head,
            Queue::Tail => self.queues.tail,
            Queue::Blocking => self.queues.blocking,
        }
    }

    pub(super) const fn index(self) -> compact::Layout {
        self.index
    }
}

#[derive(Clone, Copy)]
struct Cursor(usize);

impl Cursor {
    fn plane(&mut self, capacity: usize, width: usize) -> Result<capacity::Count, ledger::Error> {
        let offset = self.0;
        let failed = ledger::Error::Plane {
            offset,
            capacity,
            width,
        };
        let start = capacity::Count::try_from(offset).map_err(|_| failed)?;
        let bytes = capacity.checked_mul(width).ok_or(failed)?;
        self.0 = offset.checked_add(bytes).ok_or(failed)?;
        Ok(start)
    }

    fn planes<const N: usize>(
        &mut self,
        capacity: usize,
        width: usize,
    ) -> Result<[capacity::Count; N], ledger::Error> {
        let mut planes = [capacity::Count::EMPTY; N];
        for plane in &mut planes {
            *plane = self.plane(capacity, width)?;
        }
        Ok(planes)
    }
}
