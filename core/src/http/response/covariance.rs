#![allow(dead_code)]

use super::{
    FixedResponseInner, HeadInner, HeaderItemInner, HeaderValueInner, HeadersInner, HotBodyInner,
    HotHeadInner, HotTextInner, MonoResponseInner, ServeInner, TextItemInner,
};

fn _text_item<'short, 'long: 'short>(x: TextItemInner<'long>) -> TextItemInner<'short> {
    x
}
fn _hot_text<'short, 'long: 'short>(x: HotTextInner<'long>) -> HotTextInner<'short> {
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
fn _direct_header_item<'short, 'long: 'short>(
    x: HeaderItemInner<'long>,
) -> HeaderItemInner<'short> {
    x
}
fn _direct_headers<'short, 'long: 'short>(x: HeadersInner<'long>) -> HeadersInner<'short> {
    x
}
fn _direct_head<'short, 'long: 'short>(x: HeadInner<'long>) -> HeadInner<'short> {
    x
}
fn _hot_head<'short, 'long: 'short>(x: HotHeadInner<'long>) -> HotHeadInner<'short> {
    x
}
fn _fixed_response<'short, 'long: 'short>(
    x: FixedResponseInner<'long>,
) -> FixedResponseInner<'short> {
    x
}
fn _mono_response<'short, 'long: 'short>(x: MonoResponseInner<'long>) -> MonoResponseInner<'short> {
    x
}
fn _serve_response<'short, 'long: 'short>(x: ServeInner<'long>) -> ServeInner<'short> {
    x
}
