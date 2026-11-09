use o3::buffer::pool;
use sark_http::field::metadata::{
    self, block,
    map::record,
    section::{
        rules,
        state::{complete, decode, fixed},
        storage,
    },
};

use crate::{
    Reason, protocol,
    protocol::section::{Owner, Plan, seal},
};

pub(crate) fn build<S: metadata::Metadata + ?Sized>(
    fields: &S,
    kind: rules::Kind,
) -> Result<Owner, crate::Status> {
    let fields = fields.fields();
    let capacity = fields.clone().try_fold(seal::LEN, |capacity, field| {
        protocol::Block::add_capacity(capacity, field.name, field.value.len())
    })?;
    let lease = pool::Span::try_new(capacity)?;
    let mut writer = storage::Writer::<Owner>::try_new(lease)?;
    let mut section = <fixed::Raw as decode::Sink<Plan>>::h2(kind, block::Connect::Disabled);
    let mut scratch = storage::Scratch::default();
    for field in fields {
        let name = record::Span::from_slice(field.name);
        let value = block::Valued::from_span(record::Span::from_slice(field.value));
        rules::Validate::value(&mut section, value.validation());
        let next = <fixed::Raw as complete::Sink<Plan>>::select(
            &mut section,
            block::Named::from_span(name),
        );
        if let decode::Indexed::Buffered(rule) = next {
            <fixed::Raw as complete::Sink<Plan>>::push::<rules::RejectPush>(
                &mut section,
                &mut writer,
                &mut scratch,
                rule,
                name,
                value.span(),
                field.indexing,
            )?;
        }
    }
    let ((), check) = section.finish();
    if check != block::Validation::Valid {
        return Err(crate::Status::new(
            crate::Code::InvalidArgument,
            Reason::FieldSection,
        ));
    }
    Ok(<Owner as storage::Block>::from(writer.finish()))
}
