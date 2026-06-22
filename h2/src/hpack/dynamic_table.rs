use std::collections::VecDeque;

pub(super) struct DynamicTable {
    entries: VecDeque<(Vec<u8>, Vec<u8>)>,
    current_size: usize,
    max_size: usize,
}

impl DynamicTable {
    pub(super) const OVERHEAD: usize = 32;

    pub(super) fn new(max_size: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            current_size: 0,
            max_size,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn current_size(&self) -> usize {
        self.current_size
    }

    pub(super) fn max_size(&self) -> usize {
        self.max_size
    }

    pub(super) fn set_max(&mut self, max: usize) {
        self.max_size = max;
        self.evict_to_fit(0);
    }

    pub(super) fn insert(&mut self, name: Vec<u8>, value: Vec<u8>) {
        let entry_size = name.len() + value.len() + Self::OVERHEAD;
        if entry_size > self.max_size {
            self.entries.clear();
            self.current_size = 0;
            return;
        }
        self.evict_to_fit(entry_size);
        self.current_size += entry_size;
        self.entries.push_front((name, value));
    }

    pub(super) fn get(&self, index: usize) -> Option<(&[u8], &[u8])> {
        self.entries
            .get(index)
            .map(|(n, v)| (n.as_slice(), v.as_slice()))
    }

    pub(super) fn find(&self, name: &[u8], value: &[u8]) -> Option<usize> {
        for (i, (n, v)) in self.entries.iter().enumerate() {
            if n.as_slice() == name && v.as_slice() == value {
                return Some(i);
            }
        }
        None
    }

    pub(super) fn find_name(&self, name: &[u8]) -> Option<usize> {
        for (i, (n, _)) in self.entries.iter().enumerate() {
            if n.as_slice() == name {
                return Some(i);
            }
        }
        None
    }

    fn evict_to_fit(&mut self, incoming: usize) {
        while !self.entries.is_empty() && self.current_size + incoming > self.max_size {
            if let Some((n, v)) = self.entries.pop_back() {
                self.current_size -= n.len() + v.len() + Self::OVERHEAD;
            }
        }
    }
}
