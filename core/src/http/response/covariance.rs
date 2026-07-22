#![allow(dead_code)]

use super::{
    FixedResponse, HeadInner, HeaderItem, HeaderValueInner, Headers, HotBodyInner, HotHeadInner,
    MonoResponseInner, Serve, StaticResponseInner, TextBody, TextItem,
};

fn _text_item<'short, 'long: 'short>(x: TextItem<'long>) -> TextItem<'short> {
    x
}
fn _hot_text<'short, 'long: 'short>(x: TextBody<'long>) -> TextBody<'short> {
    x
}
fn _hot_body<'short, 'long: 'short>(x: HotBodyInner<'long>) -> HotBodyInner<'short> {
    x
}
fn _direct_header_value<'short, 'long: 'short>(
    x: HeaderValueInner<'long>,
) -> HeaderValueInner<'short> {
    x
}
fn _direct_header_item<'short, 'long: 'short>(x: HeaderItem<'long>) -> HeaderItem<'short> {
    x
}
fn _direct_headers<'short, 'long: 'short>(x: Headers<'long>) -> Headers<'short> {
    x
}
fn _direct_head<'short, 'long: 'short>(x: HeadInner<'long>) -> HeadInner<'short> {
    x
}
fn _hot_head<'short, 'long: 'short>(x: HotHeadInner<'long>) -> HotHeadInner<'short> {
    x
}
fn _fixed_response<'short, 'long: 'short>(x: FixedResponse<'long>) -> FixedResponse<'short> {
    x
}
fn _mono_response<'short, 'long: 'short>(x: MonoResponseInner<'long>) -> MonoResponseInner<'short> {
    x
}
fn _static_response<'short, 'long: 'short>(
    x: StaticResponseInner<'long>,
) -> StaticResponseInner<'short> {
    x
}
fn _serve_response<'short, 'long: 'short>(x: Serve<'long>) -> Serve<'short> {
    x
}
