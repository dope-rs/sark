use cartel_redis::value::source::ScoredText;

pub(crate) enum Scores<T> {
    Rows(T),
    Empty,
}

#[sark_json::json(try_encode)]
pub(crate) struct Entry<'a> {
    user: &'a [u8],
    score: f64,
}

impl<'a> From<(&'a str, f64)> for Entry<'a> {
    fn from((user, score): (&'a str, f64)) -> Self {
        Self {
            user: user.into(),
            score,
        }
    }
}

pub(crate) struct Iter<I>(Option<I>);

impl<'a, I: Iterator<Item = (&'a str, f64)>> Iterator for Iter<I> {
    type Item = Entry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.as_mut()?.next().map(Entry::from)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.0 {
            Some(iter) => iter.size_hint(),
            None => (0, Some(0)),
        }
    }
}

impl<'a, T: ScoredText> IntoIterator for &'a Scores<T> {
    type Item = Entry<'a>;
    type IntoIter = Iter<T::Iter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        Iter(match self {
            Scores::Rows(rows) => Some(rows.iter()),
            Scores::Empty => None,
        })
    }
}
