use std::{num::NonZero, task::Poll};

use sark_json::{
    parsing,
    segment::{Decode, Document, Field, Materialize, Order, Outcome, Project, Projection, Ready},
};

pub struct Compound;

pub struct Number {
    pub value: u64,
}

/// Supplies input beyond its reported reservation hint.
pub struct Short<I>(pub I);

impl<I: Iterator> Iterator for Short<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(0))
    }
}

impl<I: Iterator> ExactSizeIterator for Short<I> {}

pub struct RawField<const SEGMENTED: bool = false>(pub sark_json::Raw<o3::buffer::storage::Shared>);

impl<const SEGMENTED: bool> Decode for RawField<SEGMENTED> {
    type State<'d> = Ready<'d, Self>;
    const ORDER: Order = Order::Ordered;

    fn fields<'d>() -> &'d [Field<'d>] {
        const {
            &[Field::new(
                b"value",
                if SEGMENTED {
                    Projection::Segments
                } else {
                    Projection::Scalar
                },
            )]
        }
    }

    fn start<'d>(doc: Document<'d>, _: ()) -> Self::State<'d> {
        let value = doc.raw(0).map(Self).map_err(parsing::Error::from);
        Ready::try_new(value, doc)
    }

    fn release(&mut self, _: &mut usize) -> Poll<()> {
        Poll::Ready(())
    }
}

pub struct Projected<const CANCEL: bool = false> {
    pub pending: usize,
}

pub struct ProjectState<'d, const CANCEL: bool = false> {
    project: Project<'d>,
    pending: usize,
}

impl<const CANCEL: bool> Decode for Projected<CANCEL> {
    type State<'d> = ProjectState<'d, CANCEL>;
    const ORDER: Order = Order::Ordered;

    fn fields<'d>() -> &'d [Field<'d>] {
        const FIELDS: &[Field<'_>] = &[Field::new(b"value", Projection::Segments)];
        FIELDS
    }

    fn start<'d>(doc: Document<'d>, _: ()) -> Self::State<'d> {
        ProjectState {
            project: doc.project(0),
            pending: 0,
        }
    }

    fn release(&mut self, _: &mut usize) -> Poll<()> {
        Poll::Ready(())
    }
}

impl<'d, const CANCEL: bool> Materialize<'d, Projected<CANCEL>> for ProjectState<'d, CANCEL> {
    fn step(mut self, budget: NonZero<usize>) -> Outcome<'d, Self, Projected<CANCEL>> {
        if CANCEL && (1..=3).contains(&self.pending) {
            let mut zero = 0;
            let _ = self.project.release(&mut zero);
            let mut left = budget.get();
            let _ = self.project.release(&mut left);
            self.pending += 1;
            return Outcome::Pending(self);
        }
        match self.project.step(budget) {
            Outcome::Pending(project) => Outcome::Pending(Self {
                project,
                pending: self.pending + 1,
            }),
            Outcome::Ready(_) => Outcome::Ready(Projected {
                pending: self.pending,
            }),
            Outcome::Failed(error) => Outcome::Failed(error),
        }
    }

    fn release(&mut self, budget: &mut usize) -> Poll<()> {
        self.project.release(budget)
    }
}

pub struct CancelSequence;

pub struct CancelSequenceState<'d> {
    state: <super::schema::Sequence as Decode>::State<'d>,
    released: bool,
}

impl Decode for CancelSequence {
    type State<'d> = CancelSequenceState<'d>;
    const ORDER: Order = <super::schema::Sequence as Decode>::ORDER;

    fn fields<'d>() -> &'d [Field<'d>] {
        <super::schema::Sequence as Decode>::fields()
    }

    fn start<'d>(doc: Document<'d>, _: ()) -> Self::State<'d> {
        CancelSequenceState {
            state: <super::schema::Sequence as Decode>::start(doc, ()),
            released: false,
        }
    }

    fn release(&mut self, _: &mut usize) -> Poll<()> {
        Poll::Ready(())
    }
}

impl<'d> Materialize<'d, CancelSequence> for CancelSequenceState<'d> {
    fn step(mut self, budget: NonZero<usize>) -> Outcome<'d, Self, CancelSequence> {
        if !self.released {
            let mut zero = 0;
            let _ = self.state.release(&mut zero);
            let mut left = budget.get();
            let _ = self.state.release(&mut left);
            self.released = true;
            return Outcome::Pending(self);
        }
        match self.state.step(budget) {
            Outcome::Pending(state) => Outcome::Pending(Self {
                state,
                released: true,
            }),
            Outcome::Ready(_) => Outcome::Ready(CancelSequence),
            Outcome::Failed(error) => Outcome::Failed(error),
        }
    }

    fn release(&mut self, budget: &mut usize) -> Poll<()> {
        self.state.release(budget)
    }
}

pub const COPIED: &[u8] = &[b'x'; 128];
pub const MESSAGE: [&[u8]; 3] = [b"{\"message\":\"", COPIED, b"\"}"];
pub const ARRAY: [&[u8]; 3] = [b"[\"", COPIED, b"\"]"];
pub const ESCAPED_ARRAY: &[u8] = br#"["prefix\n\t\uD83D\uDE00suffix\\\"tail"]"#;
pub const ESCAPED_VALUE: &[u8] = "prefix\n\t\u{1f600}suffix\\\"tail".as_bytes();

impl Decode for Compound {
    type State<'d> = Ready<'d, Self>;

    const ORDER: Order = Order::Ordered;

    fn fields<'d>() -> &'d [Field<'d>] {
        const FIELDS: &[Field<'_>] = &[Field::new(b"value", Projection::None)];
        FIELDS
    }

    fn start<'d>(doc: Document<'d>, _: ()) -> Self::State<'d> {
        Ready::new(Self, doc)
    }

    fn release(&mut self, _: &mut usize) -> Poll<()> {
        Poll::Ready(())
    }
}

impl Decode for Number {
    type State<'d> = Ready<'d, Self>;

    const ORDER: Order = Order::Ordered;

    fn fields<'d>() -> &'d [Field<'d>] {
        const FIELDS: &[Field<'_>] = &[Field::new(b"value", Projection::Scalar)];
        FIELDS
    }

    fn start<'d>(doc: Document<'d>, _: ()) -> Self::State<'d> {
        let value = doc
            .u64(0)
            .map(|value| Self { value })
            .map_err(parsing::Error::from);
        Ready::try_new(value, doc)
    }

    fn release(&mut self, _: &mut usize) -> Poll<()> {
        Poll::Ready(())
    }
}

impl Compound {
    pub fn arrays(depth: NonZero<usize>) -> Vec<u8> {
        let depth = depth.get();
        let mut value = Vec::with_capacity(depth * 2);
        value.resize(depth, b'[');
        value.resize(depth * 2, b']');
        value
    }

    pub fn object(depth: NonZero<usize>) -> Vec<u8> {
        let mut value = b"{\"value\":".to_vec();
        value.extend(Self::arrays(depth));
        value.push(b'}');
        value
    }
}

pub struct Wide {
    pub value: u64,
}

impl Decode for Wide {
    type State<'d> = Ready<'d, Self>;
    const ORDER: Order = Order::Unordered;

    fn fields<'d>() -> &'d [Field<'d>] {
        const FIELDS: [Field<'static>; 64] = {
            let mut fields = [Field::new(b"other", Projection::None); 64];
            fields[63] = Field::new(b"value", Projection::Scalar);
            fields
        };
        &FIELDS
    }

    fn start<'d>(doc: Document<'d>, _: ()) -> Self::State<'d> {
        let value = doc
            .u64(63)
            .map(|value| Self { value })
            .map_err(parsing::Error::from);
        Ready::try_new(value, doc)
    }

    fn release(&mut self, _: &mut usize) -> Poll<()> {
        Poll::Ready(())
    }
}

#[sark_json::json(ordered)]
pub struct Scalars {
    #[field(raw)]
    pub a: sark_json::Token<3>,
    #[field(raw)]
    pub b: sark_json::Token<3>,
    #[field(raw)]
    pub c: sark_json::Token<3>,
    #[field(raw)]
    pub d: sark_json::Token<3>,
    #[field(raw)]
    pub e: sark_json::Token<3>,
    #[field(raw)]
    pub f: sark_json::Token<3>,
    #[field(raw)]
    pub g: sark_json::Token<3>,
    #[field(raw)]
    pub h: sark_json::Token<3>,
}

pub const SCALARS: &[u8] = br#"{"a":[1],"b":[1],"c":[1],"d":[1],"e":[1],"f":[1],"g":[1],"h":[1]}"#;
pub const COMPOUND: &[u8] = br#"{"values":["first","second"]}"#;
pub const PROJECT: &[u8] = br#"{"value":[1,2,3]}"#;
pub const FRAMES: &[u8] = br#"["first","second"]"#;
pub const VALUES: [&[u8]; 2] = [b"first", b"second"];
pub const NESTED: &[u8] = br#"[{"value":1},{"value":2}]"#;
pub const INVALID_FIRST: &[u8] = b"!...............................";
pub const INVALID_AFTER_VALUE: &[u8] = br#"{"value":123}!trailing"#;
pub const INVALID_MATERIALIZE: &[u8] =
    br#"{"values":["first","second","third","fourth","fifth","sixth","seventh","eighth","bad\n"]}"#;
pub const INVALID_FRAMES: &[u8] =
    br#"["first","second","third","fourth","fifth","sixth","seventh","eighth",!]"#;
pub const INVALID_NESTED: &[u8] =
    br#"[{"values":["first","second","third","fourth","fifth","sixth","seventh","eighth"]},{}]"#;
