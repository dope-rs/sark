#[derive(Clone, Copy)]
#[sark_json::json(encode)]
pub struct Price {
    pub price: u64,
    pub quantity: u64,
}

#[sark_json::json(encode)]
pub struct Rating {
    pub score: u64,
    pub count: u64,
}

#[sark_json::json(encode)]
pub struct Item<'a> {
    pub id: u64,
    pub name: &'a [u8],
    pub category: &'a [u8],
    #[field(flatten)]
    pub price: Price,
    pub active: bool,
    #[field(seq)]
    pub tags: Vec<&'a [u8]>,
    #[field(nested)]
    pub rating: Rating,
    pub total: u64,
}

impl<'a> Item<'a> {
    pub fn new(name: &'a str, category: &'a str) -> Self {
        Self {
            id: 7,
            name: sark_json::Text::from(name),
            category: sark_json::Text::from(category),
            price: Price {
                price: 3,
                quantity: 4,
            },
            active: true,
            tags: vec![sark_json::Text::from("x"), sark_json::Text::from("y")],
            rating: Rating { score: 5, count: 6 },
            total: 12,
        }
    }
}

#[sark_json::json(encode)]
pub struct Offer<'a> {
    #[field(flatten)]
    pub price: Price,
    pub name: &'a [u8],
    #[field(flatten)]
    pub rating: Rating,
}

#[sark_json::json(encode)]
pub struct Envelope<'a> {
    #[field(flatten)]
    pub offer: Offer<'a>,
}

#[sark_json::json(encode)]
pub struct Generic<const r#__FIRST: usize, const __W: usize = 0, const __SIZE: usize = 0> {
    #[field(flatten)]
    pub price: Price,
    pub token: sark_json::Token<r#__FIRST>,
}
