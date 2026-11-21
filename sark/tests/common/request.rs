use o3::buffer::storage;
use sark::request;
#[sark::request]
pub struct Raw {
    #[raw_body]
    pub payload: storage::Shared,
}

#[sark::request]
pub struct Len {
    #[body_len]
    pub body_len: request::BodyLen,
}

#[sark_json::json(ordered)]
pub struct Payload {
    pub value: u64,
}

#[sark::request]
#[json_body(Payload)]
pub struct Json {}

#[sark_json::json(ordered, plain)]
pub struct PlainBody {
    pub text: storage::Shared,
    #[field(raw)]
    pub number: storage::Shared,
}

#[sark::request(ordered)]
#[json_body(PlainBody)]
pub struct Plain {}

pub fn projection<R>(
    bytes: &[u8],
) -> (
    request::raw::Parsed<request::Head<&'static [u8]>, R>,
    sark::core::body::Body<&[u8]>,
)
where
    R: sark::core::route::Schema<
            Frame = request::frame::proof::Maybe,
            RawParams = (),
            Header = sark::core::route::field::header::Discard,
            Query = sark::core::route::field::query::Discard,
        >,
{
    use sark::core::body::admit::{Length, Plan, Retention, Stream};
    use sark::http::request::{Form, Layout, span::Span};

    let root = Span::checked(0, 1).expect("root span");
    let layout = Layout::try_new(Form::Path, root, None, Some(root)).expect("root layout");
    let head = request::Head::bind(b"/".as_slice(), layout).expect("root head");
    // SAFETY: the root has no captures and both field policies discard every field.
    let head = unsafe { request::raw::Parsed::new(head, (), R::RawHeaders::default()) };
    let len = bytes.len();
    let body = Plan::new(
        Retention::Retained,
        len,
        Length::Known(len),
        Stream::Buffered,
    )
    .try_finish(len, len, bytes)
    .expect("complete body");
    (head, body)
}
