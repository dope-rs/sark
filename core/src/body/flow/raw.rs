use super::Ready;
use crate::body::{self, credit};

// SAFETY: the finished owner and built output move unchanged with the new proof.
unsafe impl<'d, B: body::Build<'d>, O: credit::Origin> credit::raw::Bind for Ready<'d, B, O> {
    type Origin = O;
    type Bound<Q: credit::Origin> = Ready<'d, B, Q>;

    unsafe fn bind<Q: credit::Origin>(self, origin: Q) -> Self::Bound<Q> {
        Ready {
            // SAFETY: this is the exact ingress retained by the authenticated output.
            state: unsafe { self.state.bind(origin) },
            output: self.output,
            lifetime: self.lifetime,
        }
    }
}
