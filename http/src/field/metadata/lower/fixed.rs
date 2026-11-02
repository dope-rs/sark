use o3::buffer::pool;

use crate::field::{
    head,
    metadata::{
        lower,
        map::record,
        section::{rules, state::fixed, storage},
    },
};

#[must_use = "push the validated authority field"]
pub struct Authority<'a, 'field> {
    append: pool::Append<'a, ([u8; storage::Tag::PREFIX_LEN], [&'field [u8]; 2]), pool::Span>,
    scratch: &'a storage::Scratch,
    state: &'a mut fixed::Discard,
    name: record::Span<'field>,
}

impl<'a, 'field> Authority<'a, 'field> {
    pub fn try_new(
        lease: &'a mut pool::Span,
        scratch: &'a storage::Scratch,
        state: &'a mut fixed::Discard,
        name: record::Span<'field>,
        value: record::Span<'field>,
        indexing: crate::field::Indexing,
    ) -> Result<Self, lower::Error> {
        let start = lease.len();
        let len = value.len();
        let prefix = storage::Tag::AUTHORITY
            .prefix_with(len, indexing)
            .ok_or(lower::Error::Prefix(storage::Tag::AUTHORITY, len))?;
        let value_at = start
            .checked_add(prefix.len())
            .ok_or(lower::Error::Layout([start, name.len(), len]))?;
        value_at
            .checked_add(len)
            .ok_or(lower::Error::Layout([start, name.len(), len]))?;
        let (first, second) = value.parts();
        let append = pool::Append::try_prefixed(lease, prefix, [first, second])
            .map_err(lower::Error::Write)?;
        Ok(Self {
            append,
            scratch,
            state,
            name,
        })
    }

    pub fn push<A: rules::Push>(self) {
        let Self {
            append,
            scratch,
            state,
            name,
        } = self;
        let (prefix, [first, second]) = append.input();
        let indexing =
            crate::field::Indexing::from_never(prefix[storage::Tag::PREFIX_LEN - 1] != 0);
        let field = record::View::new(name, record::Span::new(first, second)).with_index(indexing);
        let keep = state.retain::<A>(
            head::Name::Authority,
            field,
            append.as_slice(),
            scratch.authority(),
        );
        match keep {
            rules::Retention::Discard => {}
            rules::Retention::Retain => {
                let value_at = append.as_slice().len() + storage::Tag::PREFIX_LEN;
                let (lease, _) = append.commit();
                state.store(value_at..lease.len());
            }
        }
    }
}

const _: () = {
    assert!(
        size_of::<Authority<'static, 'static>>()
            == size_of::<record::View<'static>>() + 4 * size_of::<usize>()
    );
    assert!(
        size_of::<Option<Authority<'static, 'static>>>()
            == size_of::<Authority<'static, 'static>>()
    );
};
