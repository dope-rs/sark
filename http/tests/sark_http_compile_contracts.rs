//! Types enforce admission, policy identity, and owner lifetimes; consumed response progress cannot be retried and remains tied to its source owner.
//! ```compile_fail,E0382
//! use sark_http::h1::head::{Limit, response::State};
//! State::with(Vec::<u8>::new(), Limit::RESPONSE, |source, progress| {
//!     let _ = progress.scan(&source);
//!     let _ = progress.scan(&source);
//! });
//! ```
//!
//! ```compile_fail,E0521
//! use sark_http::h1::head::{Limit, response::State};
//! State::with(Vec::<u8>::new(), Limit::RESPONSE, |_source, progress| {
//!     State::with(Vec::<u8>::new(), Limit::RESPONSE, |other, _progress| {
//!         let _ = progress.scan(&other);
//!     });
//! });
//! ```
//!
//! Owned field proofs cannot hide allocation behind an infallible clone.
//! ```compile_fail,E0277
//! use sark_http::field::Name;
//! fn copied<T: Clone>() {}
//! copied::<Name<String>>();
//! ```
//!
//! ```compile_fail,E0277
//! use sark_http::field::Value;
//! fn copied<T: Clone>() {}
//! copied::<Value<Vec<u8>>>();
//! ```
//!
//! ```compile_fail,E0277
//! use std::borrow::Cow;
//! use sark_http::field::Lower;
//! fn copied<T: Clone>() {}
//! copied::<Lower<Cow<'static, str>>>();
//! ```
//!
//! ```compile_fail,E0277
//! use sark_http::payload::response::headers::Name;
//! fn copied<T: Clone>() {}
//! copied::<Name<String>>();
//! ```
//!
//! ```compile_fail,E0277
//! use sark_http::payload::response::{headers, statics};
//! fn cloned<T: Clone>() {}
//! cloned::<statics::Direct<'static, Vec<headers::Entry<'static>>>>();
//! ```
//!
//! Borrowed proofs retain their allocation-free copy and clone contracts.
//! ```
//! use sark_http::{field, payload::response::{headers, statics}};
//! fn copied<T: Copy + Clone>() {}
//! copied::<field::Name<&[u8]>>();
//! copied::<field::Value<&str>>();
//! copied::<field::Lower<&[u8]>>();
//! copied::<headers::Name<&[u8]>>();
//! fn cloned<T: Clone>() {}
//! cloned::<statics::Direct<'_, &[headers::Entry<'_>]>>();
//! ```
//!
//! ```compile_fail,E0080
//! use sark_http::field::metadata::section::storage::Slot;
//! const _: Slot = Slot::exact::<65530>();
//! ```
//!
//! ```compile_fail,E0080
//! use sark_http::field::{self, Lower};
//! let _ = field::literal!(Lower::try_bytes(b"Upper"));
//! ```
//!
//! ```compile_fail,E0435
//! use sark_http::field::{self, Lower};
//! fn admit(name: &[u8]) {
//!     let _ = field::literal!(Lower::try_bytes(name));
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use sark_http::field::metadata::{block, section::{self, state::{RetainedHead, Selected, decode::Final}}};
//! fn forge<P: section::Policy>(head: RetainedHead<P::Block, Selected<P>, Option<block::Plan>>) -> Final<P> {
//!     Final::Request(head)
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use sark_http::field::metadata::{block, section::{self, state::{RetainedHead, Selected, decode::Final}}};
//! fn relabel<P: section::Policy>(head: RetainedHead<P::Block, Selected<P>, Option<block::Plan>>) -> Final<P> {
//!     Final::Other(head)
//! }
//! ```
//!
//! ```compile_fail,E0451
//! use sark_http::field::metadata::{block, section::{self, state::{RetainedHead, Selected, decode::Rejected}}};
//! fn forge<P: section::Policy>(head: RetainedHead<P::Block, Selected<P>, ()>) -> Rejected<P> {
//!     Rejected { head, sealed: block::Validation::Valid, finished: block::Validation::Valid, response: None }
//! }
//! ```
//!
//! ```compile_fail,E0599
//! use sark_http::field::metadata::{block, section::state::RetainedHead};
//! fn forge<B, S>(fields: B, pick: S, plan: block::Plan) -> RetainedHead<B, S> {
//!     RetainedHead::<B, S>::admitted(fields, pick, plan)
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use sark_http::field::metadata::section::{Policy, state::Selected};
//! fn substitute<P, Q>(pick: Selected<Q>) -> Selected<P>
//! where P: Policy, Q: Policy<Selection = P::Selection, Block = P::Block> {
//!     pick
//! }
//! ```
//!
//! ```compile_fail,E0277
//! use sark_http::{field::metadata::{packed, section::{Policy, state::{RetainedHead, Selected}}}, request::raw::Bind};
//! fn bind<P: Policy<Block = packed::Decoded>>(head: RetainedHead<packed::Decoded, Selected<P>>) {
//!     let _ = unsafe { Bind::try_bind(head, None, None, None, None) };
//! }
//! ```
//!
//! ```compile_fail,E0624
//! use sark_http::field::metadata::section::{Policy, state::decode::{Decoded, Final}};
//! fn publish<P: Policy>(decoded: Decoded<P>) -> Final<P> {
//!     Final::new(decoded)
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use sark_http::field::metadata::{lower, section::{rules, storage, state::{complete, decode}}};
//! fn publish<P: rules::Policy>(state: P::State, writer: storage::Writer<P::Block>, scratch: &mut storage::Scratch) -> Result<decode::Final<P>, lower::Error> {
//!     <P::State as complete::Sink<P>>::finish(state, writer, scratch).map_err(Into::into)
//! }
//! ```
//!
//! ```compile_fail,E0308
//! use sark_http::field::metadata::{lower, section::{rules, storage, state::decode}};
//! fn publish<P: rules::Policy>(state: P::State, input: lower::Input<P::Block>, scratch: &mut storage::Scratch) -> Result<decode::Final<P>, lower::Error> {
//!     <P::State as decode::Sink<P>>::seal(state, input, scratch).map_err(Into::into)
//! }
//! ```
//!
//! ```compile_fail,E0599
//! use sark_http::field::{head, Indexing, metadata::{map::record::Span, section::{rules, state::complete::Decoder, storage::Tag}}};
//! fn replace(decoder: &mut Decoder<'_, rules::Discard>, bytes: Span<'_>) {
//!     decoder.push::<rules::RejectPush>(rules::Decision::<()>::Tagged(head::Name::Regular, Tag::PATH), bytes, bytes, Indexing::Allowed);
//! }
//! ```
//!
//! ```compile_fail,E0061
//! use sark_http::field::{head, Indexing, metadata::{block::{Named, Valued}, section::{rules, state::decode::Session, storage::Tag}}};
//! fn replace(mut session: Session<rules::Discard>, name: Named<'_>, value: Valued<'_>) {
//!     session.field::<rules::RejectPush>(rules::Decision::<()>::Tagged(head::Name::Regular, Tag::PATH), name, value, Indexing::Allowed);
//! }
//! ```
//!
//! ```compile_fail,E0451
//! use sark_http::field::metadata::{block::{Valued, Validation}, map::record::Span};
//! fn forge() {
//!     let _ = Valued { span: Span::from_slice(b"\0"), validation: Validation::Valid };
//! }
//! ```
//!
//! ```
//! use sark_http::payload::response::dispatch::chunk::Chunk;
//! fn retain<'a: 'd, 'd>(bytes: &'a [u8; 6], _scope: &'d ()) -> Chunk<'d> {
//!     bytes.into()
//! }
//! ```
//!
//! ```compile_fail,E0515
//! use sark_http::payload::response::dispatch::chunk::Chunk;
//! fn escape() -> Chunk<'static> {
//!     let bytes = [0u8; 6];
//!     (&bytes).into()
//! }
//! ```

use std::env;

#[cfg(test)]
use dope_test::checks::{Error, Suite};

#[test]
fn contracts() -> Result<(), Error> {
    let suite = Suite {
        toolchain: "stable",
        ..Suite::new(env!("CARGO_MANIFEST_DIR"), file!(), &["sark-http"])
    };
    let exe = env::current_exe()?;
    let output = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.output()?;
    let artifacts = suite.try_collect(&exe, output)?;
    assert_eq!(
        artifacts.try_errors(include_str!("sark_http_compile_contracts.rs"))?,
        23
    );
    let status = artifacts.try_rustdoc()?.status()?;
    assert!(status.success(), "compiler contracts: {status}");
    Ok(())
}
