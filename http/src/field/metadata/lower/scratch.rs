use o3::buffer::write::{self, consume};

use crate::field::metadata::{block, huffman::decoder, lower, section::storage, strings};

pub(super) struct Scratch {
    bytes: Vec<u8>,
    scan: block::Value,
}

impl Scratch {
    pub(super) fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            scan: block::Value::new(),
        }
    }

    pub(super) fn into_vec(self) -> Vec<u8> {
        self.bytes
    }

    pub(super) fn into_parts(self) -> (Vec<u8>, block::Validation) {
        (self.bytes, self.scan.finish())
    }

    pub(super) fn try_string(
        &mut self,
        cursor: &mut strings::Cursor,
        input: &[u8],
    ) -> Result<(), lower::StringError> {
        let bound = cursor
            .output_bound(input.len())
            .map_err(lower::StringError::Bound)?;
        storage::try_reserve(&mut self.bytes, bound)
            .map_err(|cause| lower::StringError::Store(lower::Error::Scratch(cause)))?;
        let current = self.bytes.len();
        let rest = cursor.remaining();
        let sink = Writer {
            bytes: write::Reserved::from(&mut self.bytes).into_staged(),
            scan: &mut self.scan,
        };
        match cursor.feed_into(input, sink) {
            Ok((_, sink)) => {
                sink.bytes.commit();
                Ok(())
            }
            Err(failure) => {
                let decoder::Failure {
                    sink,
                    cause,
                    progress,
                } = failure;
                let stored = [current, current + sink.bytes.len()];
                sink.bytes.commit();
                match cause {
                    decoder::DecodeError::InvalidEncoding(cause) => {
                        Err(lower::StringError::Decode {
                            cause: decoder::DecodeError::InvalidEncoding(cause),
                            progress,
                        })
                    }
                    decoder::DecodeError::Sink(cause) => {
                        Err(lower::StringError::Store(lower::Error::SinkProgress {
                            cause,
                            stored,
                            remaining: rest,
                            input: [input.len(), progress.consumed(), progress.decoded()],
                        }))
                    }
                }
            }
        }
    }
}

struct Writer<'a> {
    bytes: write::Staged<'a, usize>,
    scan: &'a mut block::Value,
}

impl write::Sink for Writer<'_> {
    type Error = consume::Error;

    fn write_slice(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.bytes.write_slice(bytes)?;
        self.scan.extend(bytes);
        Ok(())
    }

    fn write_slices<const N: usize>(&mut self, slices: [&[u8]; N]) -> Result<(), Self::Error> {
        self.bytes.write_slices(slices)?;
        for bytes in slices {
            self.scan.extend(bytes);
        }
        Ok(())
    }
}
