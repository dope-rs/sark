use core::cmp;
use std::collections;

use crate::app::route;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Path<'a> {
    index: usize,
    segments: &'a [route::Seg],
    literals: usize,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StateKey<'a> {
    depth: usize,
    routes: Vec<Path<'a>>,
}

pub(super) struct State {
    pub(super) depth: usize,
    pub(super) accept: Vec<usize>,
    pub(super) literals: collections::BTreeMap<Vec<u8>, usize>,
    pub(super) fallback: Option<usize>,
}

#[derive(Default)]
pub(super) struct Machine {
    pub(super) starts: collections::BTreeMap<route::Method, usize>,
    pub(super) states: Vec<State>,
    pub(super) asterisk: collections::BTreeMap<route::Method, Vec<usize>>,
}

impl Machine {
    pub(super) fn build(plan: &route::Plan) -> Self {
        let mut asterisk = collections::BTreeMap::<route::Method, Vec<usize>>::new();
        let mut keys = collections::BTreeMap::<StateKey<'_>, usize>::new();
        let mut queue = collections::VecDeque::<StateKey<'_>>::new();
        let mut paths = collections::BTreeMap::<route::Method, Vec<Path<'_>>>::new();
        for (index, entry) in plan.routes.iter().enumerate() {
            match entry.target.path() {
                Some(segments) => paths.entry(entry.method).or_default().push(Path {
                    index,
                    segments,
                    literals: entry.literal_count,
                }),
                None => asterisk.entry(entry.method).or_default().push(index),
            }
        }
        let mut initial = Vec::with_capacity(paths.len());
        for (method, routes) in paths {
            let key = StateKey { depth: 0, routes };
            let id = Self::intern(key, &mut keys, &mut queue);
            initial.push((method, id));
        }
        let starts = initial.into_iter().collect();

        let mut states = Vec::new();
        while let Some(key) = queue.pop_front() {
            let mut accept = key
                .routes
                .iter()
                .copied()
                .filter(|path| path.segments.len() == key.depth)
                .collect::<Vec<_>>();
            accept.sort_by_key(|path| cmp::Reverse(path.literals));
            let accept = accept.into_iter().map(|path| path.index).collect();

            let active = key
                .routes
                .iter()
                .copied()
                .filter(|path| path.segments.len() > key.depth)
                .collect::<Vec<_>>();
            let params = active
                .iter()
                .copied()
                .filter(|path| {
                    path.segments
                        .get(key.depth)
                        .is_some_and(|segment| !segment.is_literal())
                })
                .collect::<Vec<_>>();
            let fallback = if params.is_empty() {
                None
            } else {
                Some(Self::intern(
                    StateKey {
                        depth: key.depth + 1,
                        routes: params.clone(),
                    },
                    &mut keys,
                    &mut queue,
                ))
            };
            let literals = active
                .iter()
                .filter_map(|path| path.segments.get(key.depth))
                .filter_map(route::Seg::literal)
                .collect::<collections::BTreeSet<_>>()
                .into_iter()
                .map(|literal| {
                    let mut routes = params.clone();
                    routes.extend(active.iter().copied().filter(|path| {
                        path.segments.get(key.depth).and_then(route::Seg::literal) == Some(literal)
                    }));
                    routes.sort_unstable();
                    let next = Self::intern(
                        StateKey {
                            depth: key.depth + 1,
                            routes,
                        },
                        &mut keys,
                        &mut queue,
                    );
                    (literal.to_vec(), next)
                })
                .collect();
            states.push(State {
                depth: key.depth,
                accept,
                literals,
                fallback,
            });
        }
        Self {
            starts,
            states,
            asterisk,
        }
    }

    fn intern<'a>(
        key: StateKey<'a>,
        keys: &mut collections::BTreeMap<StateKey<'a>, usize>,
        queue: &mut collections::VecDeque<StateKey<'a>>,
    ) -> usize {
        use collections::btree_map::Entry;

        let id = keys.len();
        match keys.entry(key.clone()) {
            Entry::Occupied(slot) => *slot.get(),
            Entry::Vacant(slot) => {
                let id = *slot.insert(id);
                queue.push_back(key);
                id
            }
        }
    }
}
