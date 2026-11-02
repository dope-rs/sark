use o3::buffer::{
    pool,
    pool::owners,
    write::{Sink as _, SpareExt as _, consume},
};

use crate::h1::head::request::{
    Route, offers,
    pool::{Buf, Layout, Meta, Source},
};

pub struct Open<'d> {
    bytes: pool::Span<owners::Plain<pool::Layout>, pool::Borrowed<'d>>,
}

impl<'d> Open<'d> {
    pub(in crate::h1::head::request::pool) const fn new(
        bytes: pool::Span<owners::Plain<pool::Layout>, pool::Borrowed<'d>>,
    ) -> Self {
        Self { bytes }
    }

    pub fn buffer(mut self, source: &[u8]) -> Result<Buf<'d>, consume::Error> {
        let mut out = self.bytes.spare();
        out.write_slices([[0; Layout::BYTES].as_slice(), source])?;
        out.commit();
        Ok(Buf { bytes: self.bytes })
    }

    pub fn seal(mut self, route: Route<&[u8]>) -> Result<Meta<'d>, consume::Error> {
        let method = route.method().as_bytes();
        let target = route.target().as_bytes();
        let layout = Layout::new(
            [
                method.len() as u16,
                method.len() as u16,
                (method.len() + target.len()) as u16,
                0,
            ],
            Source::Compact,
            route.transition(),
        );
        let mut out = self.bytes.spare();
        out.write_slices([layout.encode().as_slice(), method, target])?;
        if let Some(offers) = offers::View::from_route(route) {
            offers.write_into(&mut out)?;
        }
        out.commit();
        Ok(Meta { bytes: self.bytes })
    }
}

const _: () = assert!(size_of::<Open<'_>>() == 2 * size_of::<usize>());
