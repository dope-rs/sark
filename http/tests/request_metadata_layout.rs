pub mod common;

use common::request::LAYOUTS;
use o3::buffer::pool;
use sark_http::{
    field::metadata::{
        block::{self, Connect, Validation},
        map::record::Span,
        section::{
            rules,
            state::decode::{Final, Indexed, Session},
            storage,
        },
    },
    request,
};

#[test]
fn binds_native_request_layouts() {
    for case in LAYOUTS {
        let modes: &[Connect] = if matches!(case.form, request::Form::Extended) {
            &[Connect::Disabled, Connect::Enabled]
        } else {
            &[Connect::Disabled]
        };
        for &connect in modes {
            let lease = pool::Span::try_new(256).expect("lease");
            let mut session = Session::<rules::RetainRaw>::try_new(
                lease,
                rules::Kind::Request,
                connect,
                storage::Scratch::default(),
            )
            .map_err(|(_, error)| error)
            .expect("session");
            for field in case.fields {
                let name = block::Named::from_slice(field.name);
                if matches!(
                    session.indexed(Some(name)).expect("select"),
                    Indexed::Buffered(())
                ) {
                    session
                        .field::<rules::RejectPush>(
                            name,
                            block::Valued::from_span(Span::from_slice(field.value)),
                            field.indexing,
                        )
                        .expect("field");
                }
            }
            let complete = session.finish().1.expect("finish");
            if matches!(case.form, request::Form::Extended) && matches!(connect, Connect::Disabled)
            {
                assert!(matches!(complete, Final::Rejected(rejected)
                    if rejected.validation().1 == Validation::Malformed));
                continue;
            }
            let (head, _, _, _) = complete.into_parts();
            let head = head.try_request().ok().expect("request");
            let (mut path, mut path_len, mut pseudo, mut host) = (None, None, None, None);
            for (field, range) in head.fields().ranges() {
                match field.name {
                    b":path" => {
                        path_len = Some(
                            field
                                .value
                                .iter()
                                .position(|byte| *byte == b'?')
                                .unwrap_or(field.value.len()),
                        );
                        path = Some(range);
                    }
                    b":authority" => pseudo = Some(range),
                    b"host" => host = Some(range),
                    _ => {}
                }
            }
            // SAFETY: every range came from this exact native RetainRaw head;
            // its original immutable owner and completed admission remain together.
            let bound = unsafe { request::raw::Bind::try_bind(head, path, path_len, pseudo, host) }
                .expect("bound");
            let (owner, layout) = bound.into_parts();
            let layout = layout.require().expect("authority");
            let bytes = owner.as_bytes();
            assert!(layout.target().form() == case.form);
            assert_eq!(layout.target().get(bytes), Some(case.target));
            assert_eq!(layout.authority_in(bytes), Some(case.authority));
            assert_eq!(layout.route_path(bytes), Some(case.route));
            let query = layout.route().and_then(|route| {
                if route.end() == layout.target().end() {
                    None
                } else {
                    bytes.get(route.end() + 1..layout.target().end())
                }
            });
            assert_eq!(query, case.query);
        }
    }
}
