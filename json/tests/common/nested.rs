use o3::{alloc::VecExt, buffer::storage::Shared};
use sark_json::{Error, Text};

#[sark_json::json(ordered)]
pub struct Empty {}

#[sark_json::json(ordered)]
pub struct Message {
    pub message: Shared,
}

#[sark_json::json(ordered)]
pub struct World {
    pub id: u64,
    #[field(name = "randomNumber")]
    pub random_number: u64,
}

#[sark_json::json(ordered)]
pub struct Fortune {
    pub id: u64,
    pub message: Shared,
}

#[sark_json::json(ordered)]
pub struct Rating {
    pub score: u64,
    pub count: u64,
}

#[sark_json::json(ordered)]
pub struct Stock {
    pub price: u64,
    pub quantity: u64,
    pub active: bool,
}

#[sark_json::json(ordered)]
pub struct Item {
    pub id: u64,
    pub name: Shared,
    pub category: Shared,
    #[field(nested)]
    pub stock: Stock,
    #[field(seq)]
    pub tags: Vec<Shared>,
    #[field(nested)]
    pub rating: Rating,
    pub total: u64,
}

#[sark_json::json(ordered)]
pub struct Catalog {
    #[field(seq, nested)]
    pub items: Vec<Item>,
    pub count: u64,
}

#[sark_json::json]
pub struct Node {
    #[field(seq, nested)]
    pub children: Vec<Node>,
}

impl Node {
    pub fn json(depth: NonZero<usize>) -> String {
        let depth = depth.get();
        let mut input = String::with_capacity(depth * 14 + 2);
        for _ in 0..depth {
            input.push_str("{\"children\":[");
        }
        for _ in 0..depth {
            input.push_str("]}");
        }
        input
    }
}

impl Item {
    pub fn try_new() -> Result<Self, Error> {
        Ok(Self {
            id: 1,
            name: Text::try_copy("Alpha")?,
            category: Text::try_copy("electronics")?,
            stock: Stock {
                price: 328,
                quantity: 15,
                active: true,
            },
            tags: {
                let mut tags = Vec::try_new(2).map_err(Error::Allocation)?;
                tags.push(Text::try_copy("fast")?);
                tags.push(Text::try_copy("new")?);
                tags
            },
            rating: Rating {
                score: 48,
                count: 127,
            },
            total: 14760,
        })
    }
}

impl Catalog {
    pub fn try_new(count: u32) -> Result<Self, Error> {
        let mut items = Vec::new();
        for _ in 0..count {
            let item = Item::try_new()?;
            VecExt::try_reserve(&mut items, 1).map_err(Error::Allocation)?;
            items.push(item);
        }
        Ok(Self {
            items,
            count: u64::from(count),
        })
    }
}
use std::num::NonZero;
