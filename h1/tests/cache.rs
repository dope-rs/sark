pub mod common;

use std::cell::Cell;

use common::{
    body::{Bounded, Counted},
    fields::{self, DATE, ENTRIES},
};
use o3::buffer::{Residency as _, storage, write::Slice};
use sark_h1::{
    route,
    wire::{
        cache::{self, Entry, Error},
        output,
    },
};
use sark_http::{
    h1::head::{
        request::Coding,
        response::{Date, Server},
    },
    owner::{evidence, handoff},
    payload::{
        fixed::Body,
        response::{statics, status::Code},
    },
};

#[test]
fn rejects_date_overflow() {
    let result = Entry::try_new(
        storage::Shared::new(),
        Some(usize::MAX),
        0,
        fields::RESPONSE,
    );
    let error = result.err().expect("date offset must be rejected");
    assert_eq!(
        error.discard(),
        Error::Date {
            offset: usize::MAX,
            available: 0
        }
    );
}

#[test]
fn rejects_date_over_body() {
    let bytes = storage::Shared::try_copy(&[0; 30]).expect("cache storage");
    let pointer = bytes.as_slice().as_ptr();
    let (bytes, error) = Entry::try_new(bytes, Some(1), 1, fields::RESPONSE)
        .err()
        .expect("date must fit before the body")
        .into_parts();
    assert_eq!(bytes.as_slice().as_ptr(), pointer);
    assert_eq!(
        error,
        Error::Date {
            offset: 1,
            available: 29,
        }
    );
}

#[test]
fn rejects_body_overflow() {
    let bytes = storage::Shared::try_copy(b"head").expect("cache storage");
    let pointer = bytes.as_slice().as_ptr();
    let (bytes, error) = Entry::try_new(bytes, None, usize::MAX, fields::RESPONSE)
        .err()
        .expect("body must fit storage")
        .into_parts();
    assert_eq!(bytes.as_slice().as_ptr(), pointer);
    assert_eq!(
        error,
        Error::Body {
            length: usize::MAX,
            available: 4
        }
    );
}

#[test]
fn date_ends_at_body() {
    let bytes = storage::Shared::try_copy(&[b'x'; 30]).expect("cache storage");
    let entry = Entry::try_new(bytes, Some(0), 1, fields::RESPONSE)
        .expect("complete date at head boundary");
    let mut short = [0xa5; 28];
    assert!(
        entry
            .try_write(o3::buffer::write::Slice::new(&mut short).reborrow(), DATE)
            .expect("short output")
            .is_none()
    );
    assert_eq!(short, [0xa5; 28]);
    let mut head = [0; 29];
    let mut writer = Slice::new(&mut head);
    let cached = entry
        .try_write(writer.reborrow(), DATE)
        .expect("head output");
    assert!(matches!(cached, Some(cache::Cached::Shared { body })
        if body.as_slice() == b"x"));
    assert_eq!(writer.into_mut(), DATE);
    let mut full = [0; 30];
    let mut writer = Slice::new(&mut full);
    let cached = entry
        .try_write(writer.reborrow(), DATE)
        .expect("full output");
    assert!(matches!(cached, Some(cache::Cached::Fixed { body: 1 })));
    assert_eq!(writer.len(), 30);
    assert_eq!(&full[..29], DATE);
    assert_eq!(full[29], b'x');
}

#[test]
fn empty_prefix() {
    let empty =
        Entry::try_new(storage::Shared::new(), None, 0, fields::RESPONSE).expect("empty entry");
    let mut writer = Slice::new(&mut []);
    assert!(matches!(
        empty.try_write(writer.reborrow(), DATE),
        Ok(Some(cache::Cached::Fixed { body: 0 }))
    ));
    let bytes = storage::Shared::try_copy(b"body").expect("cache storage");
    let entry = Entry::try_new(bytes, None, 4, fields::RESPONSE).expect("body without head");
    assert!(matches!(entry.try_write(writer.reborrow(), DATE),
        Ok(Some(cache::Cached::Shared { body })) if body.as_slice() == b"body"
    ));
}

#[test]
fn bounded_cache_prefix() {
    for source in [b"body".as_slice(), b"".as_slice()] {
        let encodes = Cell::new(0);
        let mut entry = None;
        let mut cold = [0xa5; 256];
        let mut writer = Slice::new(&mut cold);
        let outcome = output::Egress::new(writer.reborrow(), DATE, fields::RESPONSE).route(
            Bounded {
                fields: (),
                body: Counted {
                    bytes: source,
                    encodes: &encodes,
                },
            },
            handoff::Slot::ready(None),
            cache::Cache::new(&mut entry),
            &mut (),
            Coding::Plain,
            route::Policy::new(
                route::Cache::Store,
                Coding::Plain,
                Date::Include,
                Server::Omit,
            ),
        );
        let written = match outcome {
            output::Outcome::SendCopied { copy, .. } if copy.written == source.len() => {
                Some(writer.len())
            }
            _ => None,
        }
        .expect("bounded cache output");
        let head = cold[..written]
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .expect("complete cached head")
            + 4;
        assert_eq!(&cold[head..written], source);

        let mut warm = [0xa5; 256];
        let mut writer = Slice::new(&mut warm);
        let outcome = output::Egress::new(writer.reborrow(), DATE, fields::RESPONSE)
            .cached(&mut cache::Cache::new(&mut entry), route::Cache::Store)
            .expect("cached prefix");
        assert!(matches!(outcome, output::Outcome::SendCopied { .. }));
        assert_eq!(writer.len(), written);
        assert_eq!(warm, cold);
    }
}

#[test]
fn retains_large() {
    let mut entry = None;
    let mut cold = [0xa5; 256];
    let mut writer = Slice::new(&mut cold);
    let (head, first) = {
        let source = [b'x'; 512];
        let response = statics::Direct::new(Code::OK, ENTRIES, Body::new(&source));
        let outcome = output::Egress::new(writer.reborrow(), DATE, fields::RESPONSE).route(
            response,
            handoff::Slot::ready(None),
            cache::Cache::new(&mut entry),
            &mut (),
            Coding::Plain,
            route::Policy::new(
                route::Cache::Store,
                Coding::Plain,
                Date::Include,
                Server::Omit,
            ),
        );
        match outcome {
            output::Outcome::SendShared { body, .. } => Some((writer.len(), body)),
            _ => None,
        }
        .expect("cache must retain the body when only its head fits")
    };
    assert_eq!(first.as_slice(), &[b'x'; 512]);
    assert!(cold[head..].iter().all(|byte| *byte == 0xa5));
    let resident = first.resident();
    assert!(resident >= head + first.len());

    let mut short = [0xa5; 256];
    assert!(
        output::Egress::new(
            o3::buffer::write::Slice::new(&mut short[..head - 1]).reborrow(),
            DATE,
            fields::RESPONSE
        )
        .cached(&mut cache::Cache::new(&mut entry), route::Cache::Store)
        .is_none()
    );
    assert!(short.iter().all(|byte| *byte == 0xa5));

    let mut date = *DATE;
    date[17] = b'1';
    let mut hot = [0xa5; 256];
    let mut writer = Slice::new(&mut hot[..head]);
    let outcome = output::Egress::new(writer.reborrow(), &date, fields::RESPONSE)
        .cached(&mut cache::Cache::new(&mut entry), route::Cache::Store)
        .expect("cached head fits");
    let owner = match outcome.retention() {
        output::Retention::Response(owner) => Some(owner),
        _ => None,
    }
    .expect("cache must retain response storage");
    assert_eq!(owner.len, first.len());
    assert_eq!(owner.charge.resident_bytes, resident);
    assert_eq!(owner.charge.owners, 1);
    assert!(matches!(outcome.copy(), output::Copy::Uncopied));
    let (len, body) = match outcome {
        output::Outcome::SendShared { body, .. } => Some((writer.len(), body)),
        _ => None,
    }
    .expect("transport must own the cached body");
    assert_eq!(len, head);
    assert_eq!(body.as_slice().as_ptr(), first.as_slice().as_ptr());
    assert!(
        hot[..head]
            .windows(date.len())
            .any(|bytes| bytes == date.as_slice())
    );
    assert!(hot[head..].iter().all(|byte| *byte == 0xa5));

    let mut inline = [0; 1024];
    let mut writer = Slice::new(&mut inline);
    let outcome = output::Egress::new(writer.reborrow(), &date, fields::RESPONSE)
        .cached(&mut cache::Cache::new(&mut entry), route::Cache::Store)
        .expect("whole cached response fits");
    assert!(matches!(
        outcome,
        output::Outcome::SendCopied {
            copy: evidence::Copy {
                written: 512,
                cause: evidence::Cause::Cached,
                ..
            },
            ..
        } if writer.len() == head + 512
    ));
    assert_eq!(&inline[head..head + 512], first.as_slice());

    drop(entry);
    drop(first);
    assert_eq!(body.as_slice(), &[b'x'; 512]);
}
