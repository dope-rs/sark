use std::{cell::Cell, iter, ops::ControlFlow, slice};

use o3::buffer::write::Sink;
use sark_http::payload::response::exact::Overflow;
use sark_json::writing::{Failure, TryEncode, TryMaxSize, TrySize, Writer};

#[sark_json::json(encode)]
pub struct Row<'__w> {
    pub name: &'__w [u8],
}

#[sark_json::json(encode)]
pub struct Document<r#__W>
where
    for<'a> &'a r#__W: IntoIterator,
    for<'a> <&'a r#__W as IntoIterator>::Item: TryEncode,
{
    #[field(seq, nested)]
    pub rows: r#__W,
}

#[sark_json::json(try_encode)]
pub struct TryRow<'a> {
    pub name: &'a [u8],
}

#[sark_json::json(try_encode)]
pub struct Containers<'a> {
    #[field(seq, nested)]
    pub array: [TryRow<'a>; 1],
    #[field(seq, nested)]
    pub vector: Vec<TryRow<'a>>,
    #[field(seq, nested)]
    pub borrowed: &'a Vec<TryRow<'a>>,
}

#[sark_json::json(try_encode)]
pub struct Slice<'a, T: self::TryEncode> {
    #[field(seq, nested)]
    pub rows: &'a [T],
}

#[sark_json::json(try_encode)]
pub struct Borrowed<'a> {
    #[field(seq, nested)]
    pub rows: &'a Rows<'a>,
}

pub struct Rows<'a> {
    pub next: Cell<&'a [&'a str]>,
    pub second: &'a [&'a str],
}

pub struct Iter<'a>(slice::Iter<'a, &'a str>);

impl<'a> iter::IntoIterator for &'a Rows<'_> {
    type Item = Row<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Iter(self.next.replace(self.second).iter())
    }
}

impl<'a> iter::Iterator for Iter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let name = self.0.next()?;
        Some(Row {
            name: sark_json::Text::from(*name),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

pub struct Bounded<'a> {
    pub sizes: &'a Cell<usize>,
    pub bounds: &'a Cell<usize>,
    pub writes: &'a Cell<usize>,
}

impl TrySize for Bounded<'_> {
    fn try_size(&self) -> Result<usize, Overflow> {
        self.sizes.set(self.sizes.get() + 1);
        Ok(4)
    }
}

impl TryMaxSize for Bounded<'_> {
    type Error = Overflow;

    fn try_max_size(&self) -> Result<usize, Self::Error> {
        self.bounds.set(self.bounds.get() + 1);
        Ok(4)
    }
}

impl TryEncode for Bounded<'_> {
    fn try_write<'a, W: Sink>(
        &self,
        writer: Writer<'a, W>,
    ) -> ControlFlow<Failure<'a, W>, Writer<'a, W>> {
        self.writes.set(self.writes.get() + 1);
        writer.put(b"true")
    }
}
