use std::ops::Range;

const INLINE_PATH_PARAM_CAP: usize = 2;

type PathParam = (Box<str>, Range<usize>);

#[derive(Clone)]
enum PathParamStorage {
    Inline {
        items: [Option<PathParam>; INLINE_PATH_PARAM_CAP],
        len: u8,
    },
    Heap(Vec<PathParam>),
}

#[derive(Clone)]
pub struct PathParamRanges(PathParamStorage);

impl Default for PathParamRanges {
    fn default() -> Self {
        Self::new()
    }
}

impl PathParamRanges {
    pub fn new() -> Self {
        Self(PathParamStorage::Inline {
            items: [const { None }; INLINE_PATH_PARAM_CAP],
            len: 0,
        })
    }

    pub fn with_capacity(cap: usize) -> Self {
        if cap > INLINE_PATH_PARAM_CAP {
            Self(PathParamStorage::Heap(Vec::with_capacity(cap)))
        } else {
            Self::new()
        }
    }

    pub fn push(&mut self, key: Box<str>, range: Range<usize>) {
        match &mut self.0 {
            PathParamStorage::Heap(heap) => heap.push((key, range)),
            PathParamStorage::Inline { items, len }
                if usize::from(*len) < INLINE_PATH_PARAM_CAP =>
            {
                items[usize::from(*len)] = Some((key, range));
                *len += 1;
            }
            PathParamStorage::Inline { items, len } => {
                let mut heap = Vec::with_capacity(usize::from(*len).saturating_add(1));
                for slot in items.iter_mut().take(usize::from(*len)) {
                    if let Some(item) = slot.take() {
                        heap.push(item);
                    }
                }
                heap.push((key, range));
                self.0 = PathParamStorage::Heap(heap);
            }
        }
    }

    pub fn find_last(&self, key: &str) -> Option<&Range<usize>> {
        match &self.0 {
            PathParamStorage::Heap(heap) => heap
                .iter()
                .rev()
                .find(|(k, _)| k.as_ref() == key)
                .map(|(_, r)| r),
            PathParamStorage::Inline { items, len } => items
                .iter()
                .take(usize::from(*len))
                .rev()
                .find_map(|slot| slot.as_ref().filter(|(k, _)| k.as_ref() == key))
                .map(|(_, r)| r),
        }
    }
}
