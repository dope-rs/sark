use dope_quic::wire::{stream::Id, varint::VarInt};
use o3::{
    alloc,
    depot::map::{self, flat},
};
use sark_http::payload::response::Method;

use crate::qpack::identity::Identity;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Unpermitted {
        push_id: u64,
        maximum: Option<u64>,
    },
    Unknown {
        push_id: u64,
    },
    Duplicate {
        push_id: u64,
    },
    Mismatch {
        push_id: u64,
    },
    GoAway {
        push_id: u64,
        cutoff: u64,
    },
    Capacity {
        push_id: u64,
        capacity: usize,
    },
    Storage {
        retained: usize,
        additional: usize,
        limit: usize,
    },
    Allocation(alloc::Error),
}

/// Lifetime push IDs: published promises on servers, promised or received IDs on clients.
pub(in crate::conn) struct History {
    plan: map::Plan,
    entries: Option<flat::Table<Entry>>,
    retained: usize,
    limit: usize,
}

struct Entry {
    id: u64,
    stream: Option<Id>,
    fields: Option<(Identity, Method)>,
}

impl History {
    pub(in crate::conn) const fn new(plan: map::Plan, limit: usize) -> Self {
        Self {
            plan,
            entries: None,
            retained: 0,
            limit,
        }
    }

    pub(in crate::conn) fn contains(&self, id: u64) -> bool {
        self.entries
            .as_ref()
            .is_some_and(|entries| entries.get(id, |stored| stored.id == id).is_some())
    }

    pub(in crate::conn) fn receive(
        &mut self,
        id: VarInt,
        stream: Id,
    ) -> Result<Option<Method>, Error> {
        let id = id.get();
        let entries = entries(&mut self.entries, self.plan)?;
        match entries.entry(id, |stored| stored.id == id) {
            Some(flat::Entry::Occupied(entry)) => {
                let entry = entry.into_mut();
                if entry.stream.is_some() {
                    return Err(Error::Duplicate { push_id: id });
                }
                entry.stream = Some(stream);
                Ok(entry.fields.as_ref().map(|(_, method)| *method))
            }
            Some(flat::Entry::Vacant(slot)) => {
                slot.insert(Entry {
                    id,
                    stream: Some(stream),
                    fields: None,
                });
                Ok(None)
            }
            None => Err(Error::Capacity {
                push_id: id,
                capacity: self.plan.capacity().get(),
            }),
        }
    }

    pub(in crate::conn) fn promised(
        &mut self,
        id: VarInt,
        fields: Identity,
        method: Method,
    ) -> Result<Option<Id>, Error> {
        let prepared = self.prepare(id, fields, method)?;
        let wake = match &prepared.slot {
            flat::Entry::Occupied(entry) if entry.get().fields.is_none() => entry.get().stream,
            flat::Entry::Occupied(_) | flat::Entry::Vacant(_) => None,
        };
        prepared.commit();
        Ok(wake)
    }

    pub(in crate::conn) fn method(&self, id: u64) -> Option<Method> {
        self.entries
            .as_ref()?
            .get(id, |entry| entry.id == id)?
            .fields
            .as_ref()
            .map(|(_, method)| *method)
    }

    pub(crate) const fn limit(&self) -> usize {
        self.limit
    }

    pub(in crate::conn) fn prepare(
        &mut self,
        id: VarInt,
        fields: Identity,
        method: Method,
    ) -> Result<Prepared<'_>, Error> {
        let fields = (fields, method);
        let entries = entries(&mut self.entries, self.plan)?;
        let id = id.get();
        let slot = entries
            .entry(id, |stored| stored.id == id)
            .ok_or(Error::Capacity {
                push_id: id,
                capacity: self.plan.capacity().get(),
            })?;
        let prior = match &slot {
            flat::Entry::Occupied(entry) => entry.get().fields.as_ref(),
            flat::Entry::Vacant(_) => None,
        };
        let next = match prior {
            Some(prior) if *prior == fields => self.retained,
            Some(_) => return Err(Error::Mismatch { push_id: id }),
            None => self
                .retained
                .checked_add(fields.0.capacity())
                .filter(|&size| size <= self.limit)
                .ok_or(Error::Storage {
                    retained: self.retained,
                    additional: fields.0.capacity(),
                    limit: self.limit,
                })?,
        };
        Ok(Prepared {
            slot,
            id,
            fields,
            retained: &mut self.retained,
            next,
        })
    }
}

pub(in crate::conn) struct Prepared<'a> {
    slot: flat::Entry<'a, Entry>,
    id: u64,
    fields: (Identity, Method),
    retained: &'a mut usize,
    next: usize,
}

impl Prepared<'_> {
    pub(in crate::conn) fn commit(self) {
        match self.slot {
            flat::Entry::Occupied(entry) => {
                entry.into_mut().fields.get_or_insert(self.fields);
            }
            flat::Entry::Vacant(slot) => {
                slot.insert(Entry {
                    id: self.id,
                    stream: None,
                    fields: Some(self.fields),
                });
            }
        }
        *self.retained = self.next;
    }
}

fn entries(
    store: &mut Option<flat::Table<Entry>>,
    plan: map::Plan,
) -> Result<&mut flat::Table<Entry>, Error> {
    Ok(match store {
        Some(entries) => entries,
        empty @ None => empty.insert(flat::Table::try_new(plan).map_err(Error::Allocation)?),
    })
}

pub(in crate::conn) fn check(id: u64, maximum: Option<u64>) -> Result<(), Error> {
    if maximum.is_some_and(|maximum| id <= maximum) {
        Ok(())
    } else {
        Err(Error::Unpermitted {
            push_id: id,
            maximum,
        })
    }
}

const _: () = assert!(size_of::<History>() == size_of::<[usize; 8]>());
