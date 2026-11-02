use crate::body::{self, admit, buffered, credit, ingress, input};

/// One input mutation holding the exact admitted ingress and retained storage.
pub(super) struct Receive<'state, 'ledger, S, E, O: credit::Origin> {
    pub(super) turn: ingress::Turn<'state, 'ledger, E, O>,
    pub(super) storage: &'state mut Option<S>,
}

impl<S, E: credit::Evidence, O: credit::Origin> Receive<'_, '_, S, E, O> {
    pub(super) fn try_push<D, F, R>(
        mut self,
        input: input::Input<D>,
        rmax: credit::Limits,
        cmax: credit::Limits,
        retain: F,
    ) -> buffered::Push<D, R>
    where
        F: FnOnce(Option<S>, D) -> buffered::Retain<S, D, R>,
    {
        let (data, bytes, chunks) = input.into_parts();
        let mode = self.turn.state().retention();
        let next = match self.turn.try_advance(bytes) {
            Ok(next) => next,
            Err(error) => {
                return buffered::Push::Rejected {
                    input: data,
                    error: buffered::Error::Body(error),
                };
            }
        };
        match mode {
            admit::Retention::Discarded => {
                next.commit();
                buffered::Push::Discarded(data)
            }
            admit::Retention::Retained => {
                let storage = self.storage;
                let amount = credit::Amount::new(bytes, chunks);
                let next = match next.try_reserve(amount, rmax, cmax) {
                    Ok(next) => next,
                    Err(error) => {
                        return buffered::Push::Rejected {
                            input: data,
                            error: buffered::Error::Body(body::Error::Credit(error)),
                        };
                    }
                };
                match retain(storage.take(), data) {
                    buffered::Retain::Accepted { storage: retained } => {
                        next.commit().commit();
                        *storage = retained;
                        buffered::Push::Accepted
                    }
                    buffered::Retain::Rejected {
                        storage: retained,
                        input,
                        error,
                    } => {
                        *storage = retained;
                        buffered::Push::Rejected {
                            input,
                            error: buffered::Error::Retain(error),
                        }
                    }
                }
            }
        }
    }
}
