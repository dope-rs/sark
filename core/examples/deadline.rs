//! One handler deadline per request, with completion canceling its reservation.
//! Run with `cargo run -p sark-core --example deadline`.
//! Completion cancels its deadline; rescheduling uses a later instant without sleep.

use std::{
    io::{self, Write as _},
    time::{Duration, Instant},
};

use o3::{
    alloc,
    depot::{
        arena::slab::{self, key::Handle},
        capacity::Count,
        heap,
    },
};
use sark_core::deadline::{self, Book as _, Phase, Scheduled};

#[derive(Debug)]
pub enum Error {
    Storage(alloc::Error),
    Deadlines(heap::Error),
    Full(u32),
    Schedule(deadline::Error<Handle>),
    Clock,
    Missing(Handle),
    Output(io::Error),
}

fn main() -> Result<(), Error> {
    let capacity = Count::new(2);
    let mut requests = slab::Store::<u32>::try_new(capacity).map_err(Error::Storage)?;
    let mut book = deadline::Exact::try_new(capacity).map_err(Error::Deadlines)?;
    let done = requests.insert(1).map_err(Error::Full)?;
    let slow = requests.insert(2).map_err(Error::Full)?;
    let now = Instant::now();
    let later = now
        .checked_add(Duration::from_secs(30))
        .ok_or(Error::Clock)?;

    for request in [done, slow] {
        book.schedule(request, Scheduled::expire(now, Phase::Handler))
            .map_err(Error::Schedule)?;
    }
    book.schedule(slow, Scheduled::expire(later, Phase::Handler))
        .map_err(Error::Schedule)?;

    let change = book.cancel(done);
    let request = requests.remove(done).ok_or(Error::Missing(done))?;
    let mut output = io::stdout().lock();
    writeln!(output, "completed: {request}").map_err(Error::Output)?;
    if change.is_earliest() {
        writeln!(
            output,
            "next timer: {:?}",
            book.peek().map(|(_, item)| item.at)
        )
        .map_err(Error::Output)?;
    }

    while let Some(expired) = book.pop_due(later) {
        let request = requests
            .remove(expired.key)
            .ok_or(Error::Missing(expired.key))?;
        writeln!(output, "handler deadline expired: {request}").map_err(Error::Output)?;
    }
    Ok(())
}
