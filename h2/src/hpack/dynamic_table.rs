#[derive(Copy, Clone, Default)]
struct Entry {
    start: u32,
    name_len: u32,
    value_len: u32,
}

impl Entry {
    fn data_len(self) -> usize {
        self.name_len as usize + self.value_len as usize
    }

    fn size(self) -> usize {
        self.data_len() + DynamicTable::OVERHEAD
    }
}

pub(super) struct DynamicTable {
    arena: Box<[u8]>,
    entries: Box<[Entry]>,
    front: usize,
    len: usize,
    write: usize,
    current_size: usize,
    max_size: usize,
    capacity: usize,
}

impl DynamicTable {
    pub(super) const OVERHEAD: usize = 32;

    pub(super) fn new(max_size: usize) -> Self {
        assert!(
            u32::try_from(max_size).is_ok(),
            "dynamic table size overflow"
        );
        Self {
            arena: vec![0; max_size.saturating_mul(2)].into_boxed_slice(),
            entries: vec![Entry::default(); max_size / Self::OVERHEAD].into_boxed_slice(),
            front: 0,
            len: 0,
            write: 0,
            current_size: 0,
            max_size,
            capacity: max_size,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn current_size(&self) -> usize {
        self.current_size
    }

    pub(super) fn max_size(&self) -> usize {
        self.max_size
    }

    pub(super) fn set_max(&mut self, max: usize) {
        assert!(u32::try_from(max).is_ok(), "dynamic table size overflow");
        if max > self.capacity {
            self.grow(max);
        }
        self.max_size = max;
        self.evict_to_fit(0);
    }

    pub(super) fn insert(&mut self, name: &[u8], value: &[u8]) {
        let Some(data_len) = name.len().checked_add(value.len()) else {
            self.clear();
            return;
        };
        let Some(entry_size) = data_len.checked_add(Self::OVERHEAD) else {
            self.clear();
            return;
        };
        if entry_size > self.max_size {
            self.clear();
            return;
        }
        self.evict_to_fit(entry_size);
        debug_assert!(self.len < self.entries.len());
        let start = self.write;
        Self::write_mirrored(&mut self.arena, self.capacity, start, name);
        self.write = Self::advance(self.write, name.len(), self.capacity);
        Self::write_mirrored(&mut self.arena, self.capacity, self.write, value);
        self.write = Self::advance(self.write, value.len(), self.capacity);
        self.front = if self.len == 0 {
            0
        } else if self.front == 0 {
            self.entries.len() - 1
        } else {
            self.front - 1
        };
        self.entries[self.front] = Entry {
            start: start as u32,
            name_len: name.len() as u32,
            value_len: value.len() as u32,
        };
        self.len += 1;
        self.current_size += entry_size;
    }

    pub(super) fn get(&self, index: usize) -> Option<(&[u8], &[u8])> {
        let entry = self.entry(index)?;
        let start = entry.start as usize;
        let name_end = start + entry.name_len as usize;
        let value_end = name_end + entry.value_len as usize;
        Some((
            &self.arena[start..name_end],
            &self.arena[name_end..value_end],
        ))
    }

    pub(super) fn find(&self, name: &[u8], value: &[u8]) -> Option<usize> {
        (0..self.len).find(|&index| {
            self.get(index)
                .is_some_and(|(entry_name, entry_value)| entry_name == name && entry_value == value)
        })
    }

    pub(super) fn find_name(&self, name: &[u8]) -> Option<usize> {
        (0..self.len).find(|&index| {
            self.get(index)
                .is_some_and(|(entry_name, _)| entry_name == name)
        })
    }

    fn entry(&self, index: usize) -> Option<Entry> {
        if index >= self.len {
            return None;
        }
        let physical = Self::advance(self.front, index, self.entries.len());
        Some(self.entries[physical])
    }

    fn evict_to_fit(&mut self, incoming: usize) {
        while self.len != 0 && self.current_size + incoming > self.max_size {
            let index = Self::advance(self.front, self.len - 1, self.entries.len());
            self.current_size -= self.entries[index].size();
            self.len -= 1;
        }
        if self.len == 0 {
            self.front = 0;
        }
    }

    fn clear(&mut self) {
        self.front = 0;
        self.len = 0;
        self.write = 0;
        self.current_size = 0;
    }

    #[cold]
    fn grow(&mut self, capacity: usize) {
        let mut arena = vec![0; capacity.saturating_mul(2)].into_boxed_slice();
        let mut entries = vec![Entry::default(); capacity / Self::OVERHEAD].into_boxed_slice();
        let mut write = 0;
        for index in 0..self.len {
            let (name, value) = self.get(index).unwrap();
            let start = write;
            Self::write_mirrored(&mut arena, capacity, write, name);
            write = Self::advance(write, name.len(), capacity);
            Self::write_mirrored(&mut arena, capacity, write, value);
            write = Self::advance(write, value.len(), capacity);
            entries[index] = Entry {
                start: start as u32,
                name_len: name.len() as u32,
                value_len: value.len() as u32,
            };
        }
        self.arena = arena;
        self.entries = entries;
        self.front = 0;
        self.write = write;
        self.capacity = capacity;
    }

    fn write_mirrored(arena: &mut [u8], capacity: usize, start: usize, src: &[u8]) {
        if src.is_empty() {
            return;
        }
        let first_len = src.len().min(capacity - start);
        let second_len = src.len() - first_len;
        arena[start..start + first_len].copy_from_slice(&src[..first_len]);
        arena[start + capacity..start + capacity + first_len].copy_from_slice(&src[..first_len]);
        if second_len != 0 {
            arena[..second_len].copy_from_slice(&src[first_len..]);
            arena[capacity..capacity + second_len].copy_from_slice(&src[first_len..]);
        }
    }

    fn advance(index: usize, amount: usize, capacity: usize) -> usize {
        if capacity == 0 {
            return 0;
        }
        let next = index + amount;
        if next >= capacity {
            next - capacity
        } else {
            next
        }
    }
}
