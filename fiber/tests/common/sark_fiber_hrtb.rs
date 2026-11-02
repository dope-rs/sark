use sark::http::payload::response::status::Code;
use std::marker::PhantomData;

#[sark::handler]
fn by_request(_: &PhantomData<dyn for<'req> PartialEq<&'req [u8]>>) -> Code {
    Code::OK
}

#[sark::handler]
fn by_driver(_: &PhantomData<dyn for<'r#d> PartialEq<&'d [u8]>>) -> Code {
    Code::OK
}

#[sark::handler]
fn permitted<'req, 'd>(_: &()) -> Code
where
    'd: 'req,
{
    fn nested() {
        let _: PhantomData<dyn for<'req, 'd> PartialEq<(&'req [u8], &'d [u8])>> = PhantomData;
    }
    nested();
    Code::OK
}

fn main() {}
