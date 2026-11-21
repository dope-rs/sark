use std::collections;

use crate::app::route;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Path<'a> {
    index: usize,
    segments: &'a [route::Seg],
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StateKey<'a> {
    depth: usize,
    routes: Vec<Path<'a>>,
}

pub(super) struct State {
    pub(super) depth: usize,
    pub(super) accept: Vec<usize>,
    pub(super) literals: Vec<(Vec<u8>, usize)>,
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
                Some(segments) => paths
                    .entry(entry.method)
                    .or_default()
                    .push(Path { index, segments }),
                None => asterisk.entry(entry.method).or_default().push(index),
            }
        }
        let starts = paths
            .into_iter()
            .map(|(method, routes)| {
                (
                    method,
                    Self::intern(StateKey { depth: 0, routes }, &mut keys, &mut queue),
                )
            })
            .collect();

        let mut states = Vec::new();
        while let Some(key) = queue.pop_front() {
            let mut accept = Vec::new();
            let mut params = Vec::new();
            let mut branches = collections::BTreeMap::<&[u8], Vec<Path<'_>>>::new();
            for path in key.routes {
                let Some(segment) = path.segments.get(key.depth) else {
                    accept.push(path.index);
                    continue;
                };
                if let route::Seg::Literal(name) = segment {
                    // A newly encountered literal also matches every earlier parameter.
                    let branch = branches.entry(name).or_insert_with(|| {
                        let mut branch = Vec::with_capacity(params.len() + 1);
                        branch.extend_from_slice(&params);
                        branch
                    });
                    branch.push(path);
                } else {
                    for branch in branches.values_mut() {
                        branch.push(path);
                    }
                    params.push(path);
                }
            }
            // All paths ending here have the same depth.
            accept.sort_by_key(|&index| plan.route(index).params.len());
            let mut next = |routes| {
                Self::intern(
                    StateKey {
                        depth: key.depth + 1,
                        routes,
                    },
                    &mut keys,
                    &mut queue,
                )
            };
            let fallback = (!params.is_empty()).then(|| next(params));
            let literals = branches
                .into_iter()
                .map(|(literal, routes)| (literal.to_vec(), next(routes)))
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
        match keys.entry(key) {
            Entry::Occupied(slot) => *slot.get(),
            Entry::Vacant(slot) => {
                queue.push_back(slot.key().clone());
                slot.insert(id);
                id
            }
        }
    }
}
