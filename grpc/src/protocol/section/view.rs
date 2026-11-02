use core::{marker, slice, time::Duration};

use sark_http::{field::Indexing, payload::response::status};

use crate::protocol::{
    request::timeout,
    section::{Request, Trailers, Valid, seal},
};

#[repr(transparent)]
pub(in crate::protocol) struct View<'a, K> {
    owner: &'a Valid,
    kind: marker::PhantomData<K>,
}

impl<'a> View<'a, Valid> {
    pub(in crate::protocol) const fn new(owner: &'a Valid) -> Self {
        Self {
            owner,
            kind: marker::PhantomData,
        }
    }

    pub(in crate::protocol) fn code(self) -> Option<crate::Code> {
        // SAFETY: Valid retains the complete immutable prefix written by Seal.
        let code = unsafe { self.owner.bytes().as_ptr().add(seal::CODE).read() };
        crate::Code::from_u8(code)
    }

    pub(in crate::protocol) fn http_status(self) -> Option<status::Code> {
        // SAFETY: Seal writes zero or the exact admitted nonzero HTTP status.
        // Valid retains that immutable prefix and Code preserves NonZero's niche.
        unsafe {
            self.owner
                .bytes()
                .as_ptr()
                .add(seal::HTTP_STATUS)
                .cast::<Option<status::Code>>()
                .read_unaligned()
        }
    }

    pub(in crate::protocol) fn is_grpc(&self) -> bool {
        // SAFETY: Seal writes this boolean admission result into Valid's prefix.
        unsafe {
            self.owner
                .bytes()
                .as_ptr()
                .add(seal::CONTENT_TYPE)
                .cast::<bool>()
                .read()
        }
    }

    pub(in crate::protocol) fn timeout(self) -> Option<Duration> {
        let pointer = self.owner.bytes().as_ptr();
        // SAFETY: Seal writes this count and Unit into the complete prefix.
        // Valid freezes those admitted values in this exact retained owner.
        let timeout = unsafe {
            timeout::Value {
                value: pointer.add(seal::TIMEOUT).cast::<u32>().read_unaligned(),
                unit: pointer
                    .add(seal::TIMEOUT + 4)
                    .cast::<timeout::Unit>()
                    .read(),
            }
        };
        timeout.duration()
    }
}

impl<'a> View<'a, Request> {
    pub(in crate::protocol) const fn new(request: &'a Request) -> Self {
        Self {
            owner: request.owner(),
            kind: marker::PhantomData,
        }
    }

    pub(in crate::protocol) fn required(self) -> &'a [u8] {
        let (start, len) = self.span(seal::Field::First);
        let bytes = self.owner.bytes();
        // SAFETY: `Request` requires the path in First from the validated owner.
        unsafe { slice::from_raw_parts(bytes.as_ptr().add(start), len) }
    }
}

impl<'a> View<'a, Trailers> {
    pub(super) const fn new(trailers: &'a Trailers) -> Self {
        Self {
            owner: trailers.owner(),
            kind: marker::PhantomData,
        }
    }

    pub(super) fn indexing(self) -> Indexing {
        // SAFETY: Seal writes this Indexing discriminant into Valid's immutable prefix.
        unsafe { self.owner.bytes().as_ptr().cast::<Indexing>().read() }
    }

    pub(super) fn indexing_at(&self, field: seal::Field) -> Indexing {
        let name_len = match field {
            seal::Field::First => b"grpc-message".len(),
            seal::Field::Second => b"grpc-status-details-bin".len(),
        };
        let (start, _) = self.span(field);
        if start == u32::MAX as usize {
            return Indexing::Allowed;
        }
        // SAFETY: response spans capture only these fixed names. Their admitted
        // packed prefix ends immediately before the name and retains its flag.
        unsafe {
            self.owner
                .bytes()
                .as_ptr()
                .add(start)
                .sub(name_len + 1)
                .cast::<Indexing>()
                .read()
        }
    }
}

impl<'a, K> View<'a, K> {
    pub(in crate::protocol) fn is_present(&self, field: seal::Field) -> bool {
        self.span(field).0 != u32::MAX as usize
    }

    pub(super) fn field(self, field: seal::Field) -> Option<&'a [u8]> {
        let (start, len) = self.span(field);
        if start == u32::MAX as usize {
            return None;
        }
        let bytes = self.owner.bytes();
        // SAFETY: `Valid` proves this immutable present span.
        Some(unsafe { slice::from_raw_parts(bytes.as_ptr().add(start), len) })
    }

    fn span(&self, field: seal::Field) -> (usize, usize) {
        let pointer = self.owner.bytes().as_ptr();
        // SAFETY: `Valid` proves the complete fixed-width index prefix.
        let pointer = unsafe { pointer.add(field.at()) };
        // SAFETY: both native-endian words are wholly inside that prefix.
        let start = unsafe { pointer.cast::<u32>().read_unaligned() } as usize;
        // SAFETY: the length word immediately follows the start word.
        let len = unsafe { pointer.add(4).cast::<u32>().read_unaligned() } as usize;
        (start, len)
    }
}

const _: () = assert!(core::mem::size_of::<View<'_, Request>>() == core::mem::size_of::<usize>());
