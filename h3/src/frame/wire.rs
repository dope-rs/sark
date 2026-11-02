use o3::buffer::write;

pub(crate) trait Wire {
    fn encoded_len(&self) -> usize;

    fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error>;
}
