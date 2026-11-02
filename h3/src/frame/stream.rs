use o3::buffer::write;

use crate::frame::{
    Control, ParseError, STREAM_TYPE_CONTROL, STREAM_TYPE_QPACK_ENCODER, Settings, Wire,
};

pub(crate) enum Stream<'a> {
    Control(Control<'a>),
    QpackEncoder,
}

impl<'a> Stream<'a> {
    pub(crate) fn control(settings: &'a Settings) -> Result<Self, ParseError> {
        Control::settings(settings).map(Self::Control)
    }
}

impl Wire for Stream<'_> {
    fn encoded_len(&self) -> usize {
        match self {
            Self::Control(first) => {
                usize::from(STREAM_TYPE_CONTROL.encoded_len()) + first.encoded_len()
            }
            Self::QpackEncoder => usize::from(STREAM_TYPE_QPACK_ENCODER.encoded_len()),
        }
    }

    fn encode<W: write::Sink>(&self, out: &mut W) -> Result<(), W::Error> {
        match self {
            Self::Control(first) => {
                STREAM_TYPE_CONTROL.encode(out)?;
                first.encode(out)
            }
            Self::QpackEncoder => STREAM_TYPE_QPACK_ENCODER.encode(out),
        }
    }
}
