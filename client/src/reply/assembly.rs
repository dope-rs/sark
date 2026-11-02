use std::ops::ControlFlow;

use o3::buffer::{resident, segments};

use crate::{
    Error,
    protocol::{self, response, sequence},
    reply::{self, body},
};

/// Waits for one response head while retaining its exact metadata-pool borrow.
pub struct Assembly<'a, 'd> {
    pool: &'a segments::Pool<resident::Shared<'d>>,
}

/// Aggregates body events with the response head already admitted and owned.
pub struct Body<'a, 'd> {
    head: response::Head<'d>,
    body: body::Scoped<'a, 'd>,
    trailers: Option<response::Fields<resident::Shared<'d>>>,
}

impl<'a, 'd> Assembly<'a, 'd> {
    pub const fn new(pool: &'a segments::Pool<resident::Shared<'d>>) -> Self {
        Self { pool }
    }

    /// Continues through informational heads or transfers the admitted final head.
    pub fn accept(
        self,
        event: response::Event<'d>,
    ) -> Result<ControlFlow<Body<'a, 'd>, Self>, Error> {
        use response::Event;

        match event {
            Event::Informational(_) => Ok(ControlFlow::Continue(self)),
            Event::Head(head) => Ok(ControlFlow::Break(Body {
                head,
                body: body::Scoped::new(self.pool),
                trailers: None,
            })),
            Event::Data(_) => Err(sequence(sequence::Phase::Head, sequence::Event::Data)),
            Event::Trailers(_) => Err(sequence(sequence::Phase::Head, sequence::Event::Trailers)),
        }
    }

    /// The exact failure for a stream that ended before its head was admitted.
    pub fn missing(&self) -> Error {
        sequence(sequence::Phase::Head, sequence::Event::End)
    }
}

impl<'a, 'd> Body<'a, 'd> {
    pub fn accept(&mut self, event: response::Event<'d>) -> Result<(), Error> {
        use response::Event;

        match event {
            Event::Informational(_) => Err(self.sequence(sequence::Event::Informational)),
            Event::Head(_) => Err(self.sequence(sequence::Event::Head)),
            Event::Data(data) if self.trailers.is_none() => {
                self.body.push(data.into_owner()).map_err(Error::Capacity)
            }
            Event::Data(_) => Err(self.sequence(sequence::Event::Data)),
            Event::Trailers(trailers) if self.trailers.is_none() => {
                self.trailers = Some(trailers);
                Ok(())
            }
            Event::Trailers(_) => Err(self.sequence(sequence::Event::Trailers)),
        }
    }

    /// Finishes without rechecking the head already owned by this body phase.
    pub fn finish(self) -> reply::Scoped<'a, 'd> {
        let (status, coding, headers) = self.head.into_parts();
        reply::Scoped::from_parts(status, coding, headers, self.trailers, self.body)
    }

    fn sequence(&self, event: sequence::Event) -> Error {
        let phase = if self.trailers.is_none() {
            sequence::Phase::Body
        } else {
            sequence::Phase::Trailers
        };
        sequence(phase, event)
    }
}

fn sequence(phase: sequence::Phase, event: sequence::Event) -> Error {
    Error::Protocol(protocol::Error::Sequence(sequence::Error::new(
        phase, event,
    )))
}
