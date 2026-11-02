pub mod common;

use common::field::sections::{HOST, PATH, REQUEST};
use o3::buffer::pool;
use sark_http::{
    field::metadata::{
        block,
        map::record::Span,
        section::{rules, state::decode::Session, storage},
    },
    request,
};

#[test]
fn retains_owner_and_shape() {
    for length in [PATH.len(), 1] {
        let lease = pool::Span::try_new(256).expect("lease");
        let mut session = Session::<rules::RetainRaw>::try_new(
            lease,
            rules::Kind::Request,
            block::Connect::Disabled,
            storage::Scratch::default(),
        )
        .map_err(|(_, error)| error)
        .expect("session");
        for field in REQUEST.into_iter().chain([HOST]) {
            let name = block::Named::from_slice(field.name);
            session.indexed(Some(name)).expect("select");
            session
                .field::<rules::RejectPush>(
                    name,
                    block::Valued::from_span(Span::from_slice(field.value)),
                    field.indexing,
                )
                .expect("field");
        }
        let (head, _, _, _) = session.finish().1.expect("finish").into_parts();
        let head = head.try_request().ok().expect("request");
        let bytes = head.fields().as_bytes().as_ptr();
        let path = head
            .fields()
            .ranges()
            .find(|(field, _)| field.name == REQUEST[2].name)
            .map(|(_, range)| range)
            .expect("path");
        let pseudo = head
            .fields()
            .ranges()
            .find(|(field, _)| field.name == REQUEST[3].name)
            .map(|(_, range)| range)
            .expect("authority");
        let host = head
            .fields()
            .ranges()
            .find(|(field, _)| field.name == HOST.name)
            .map(|(_, range)| range)
            .expect("host");
        // SAFETY: all field ranges came from this exact native RetainRaw head.
        let bound = unsafe {
            request::raw::Bind::try_bind(head, Some(path), Some(length), Some(pseudo), Some(host))
        };
        if length != PATH.len() {
            assert_eq!(bound.err(), Some(request::Error::InvalidRange));
            continue;
        }
        let (owner, layout) = bound.expect("bound").into_parts();
        assert_eq!(owner.as_bytes().as_ptr(), bytes);
        assert_eq!(layout.route_path(owner.as_bytes()), Some(PATH));
        assert_eq!(
            layout.authority_in(owner.as_bytes()),
            Some(REQUEST[3].value)
        );
    }
}
