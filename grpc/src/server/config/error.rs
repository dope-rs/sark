use o3::depot::capacity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    ZeroCapacity(Field),
    MinimumCapacity {
        field: Field,
        actual: usize,
        minimum: usize,
    },
    MaximumCapacity {
        field: Field,
        maximum: usize,
        error: capacity::Error,
    },
    Overflow {
        field: Field,
        operation: Operation,
        left: usize,
        right: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Calls,
    MessageBytes,
    InputMessages,
    InputSegments,
    FramingBytes,
    StreamWireBytes,
    ConnectionWireBytes,
    OutputMessages,
    OutputWireBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Add,
    Multiply,
}
