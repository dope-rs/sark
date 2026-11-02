use dope::manifold::client::session::{codec, response::Outcome};
use o3::buffer::{self, hold};

use crate::protocol::{
    Error,
    codec::{
        Codec, State,
        chunks::Chunks,
        output::{self, Parsed, Preview},
    },
};

mod item;
mod more;

pub(super) use item::Item;
pub(super) use more::More;

pub(super) enum Stage<'id, 'input, 'd, R: buffer::Cursor + 'input> {
    More(More<'id, 'input, 'd>),
    Item(Item<'id, 'input, 'd, R>),
}

impl<'id, 'input, 'd, R: buffer::Cursor + hold::raw::Retain<'d>> Stage<'id, 'input, 'd, R> {
    pub(super) fn parse<T: Outcome, F>(
        chunks: Chunks<'d>,
        state: &mut State<'d>,
        input: &codec::Input<'id, 'input, 'd, R>,
        admit: impl FnOnce(Result<Preview<'_>, &Error>) -> codec::Admission<F>,
    ) -> Result<codec::Parse<'id, T>, T>
    where
        F: FnOnce(Result<Parsed<'d>, Error>, &mut State<'d>) -> T::Accepted,
    {
        let stage = match chunks.prepare(&mut state.framing, input) {
            Ok(stage) => stage,
            Err(error) => return Err(output::rejected(error, state, admit)),
        };
        match stage {
            Self::More(stage) => match stage.try_commit(&mut state.framing, state.budget) {
                Ok((framing, consumed)) => {
                    state.framing = framing;
                    Ok(Codec::more(input, consumed))
                }
                Err(error) => Err(output::rejected(error, state, admit)),
            },
            Self::Item(stage) => {
                let emit = match admit(Ok(stage.preview())) {
                    codec::Admission::RetryInput => {
                        state.framing = stage.cancel();
                        return Err(T::retry_input());
                    }
                    codec::Admission::Ready(emit) => emit,
                };
                match stage.try_commit(&mut state.framing, state.budget) {
                    Ok((framing, parsed, consumed)) => {
                        state.framing = framing;
                        Ok(codec::Parse::Item {
                            head: T::accept(emit(Ok(parsed), state)),
                            consumed,
                        })
                    }
                    Err(error) => Err(T::accept(emit(Err(error), state))),
                }
            }
        }
    }
}
