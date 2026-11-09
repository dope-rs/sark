use crate::body::{self, buffered, credit, ingress, stream};

// SAFETY: only the account proof changes; the exact amount remains owned once.
unsafe impl<O: credit::Origin> credit::raw::Bind for credit::Release<O> {
    type Origin = O;
    type Bound<Q: credit::Origin> = credit::Release<Q>;

    unsafe fn bind<Q: credit::Origin>(self, origin: Q) -> Self::Bound<Q> {
        credit::Release::new(self.amount, origin)
    }
}

// SAFETY: the exact track and unreleased credit move unchanged with the new proof.
unsafe impl<O: credit::Origin> credit::raw::Bind for ingress::State<O> {
    type Origin = O;
    type Bound<Q: credit::Origin> = ingress::State<Q>;

    unsafe fn bind<Q: credit::Origin>(self, origin: Q) -> Self::Bound<Q> {
        ingress::State {
            track: self.track,
            credit: self.credit,
            origin,
        }
    }
}

// SAFETY: storage, track and credit all move together; only the proof changes.
unsafe impl<S, O: credit::Origin> credit::raw::Bind for buffered::State<S, O> {
    type Origin = O;
    type Bound<Q: credit::Origin> = buffered::State<S, Q>;

    unsafe fn bind<Q: credit::Origin>(self, origin: Q) -> Self::Bound<Q> {
        buffered::State {
            // SAFETY: this is the exact ingress owned by the authenticated buffer.
            state: unsafe { self.state.bind(origin) },
            storage: self.storage,
        }
    }
}

// SAFETY: every failure variant retains the exact same source and obligation.
unsafe impl<S, T, O: credit::Origin> credit::raw::Bind for buffered::Rejected<S, T, O> {
    type Origin = O;
    type Bound<Q: credit::Origin> = buffered::Rejected<S, T, Q>;

    unsafe fn bind<Q: credit::Origin>(self, origin: Q) -> Self::Bound<Q> {
        match self {
            Self::Admission { state, error } => buffered::Rejected::Admission {
                // SAFETY: the caller authenticates this exact rejected owner.
                state: unsafe { state.bind(origin) },
                error,
            },
            Self::Storage { failure, release } => buffered::Rejected::Storage {
                failure,
                // SAFETY: the original release remains paired with its source.
                release: unsafe { release.bind(origin) },
            },
            Self::Credit {
                body,
                release,
                error,
            } => buffered::Rejected::Credit {
                body,
                // SAFETY: the original release remains paired with its source.
                release: unsafe { release.bind(origin) },
                error,
            },
        }
    }
}

// SAFETY: the builder/output and ingress ownership move unchanged together.
unsafe impl<'d, B: body::Build<'d>, O: credit::Origin> credit::raw::Bind
    for stream::Stream<'d, B, O>
{
    type Origin = O;
    type Bound<Q: credit::Origin> = stream::Stream<'d, B, Q>;

    unsafe fn bind<Q: credit::Origin>(self, origin: Q) -> Self::Bound<Q> {
        // SAFETY: the caller authenticates this exact stream's persistent custody.
        stream::Stream::from_parts(unsafe { self.state.bind(origin) }, self.flow)
    }
}
