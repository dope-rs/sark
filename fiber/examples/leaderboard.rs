use std::{
    convert::Infallible,
    process::{ExitCode, Termination},
    str::Utf8Error,
};

use cartel_redis::{
    api::{Client, Storage},
    value::source::{self, Scored as _},
};
use o3::{buffer::storage::Shared, mem::Rejected};
use sark::http::request::Bytes;
use sark_http::payload::response::status;
use scores::Scores;

const BOARD_KEY: &[u8] = b"sark:leaderboard";
const MAX_CONNECTIONS: usize = 1024;
const HTTP_LISTENER_ID: u8 = 0;
const DATE_UPDATER_ID: u8 = 1;
const REDIS_CONNECTOR_ID: u8 = 2;

mod error;
pub mod input;
mod scores;
pub mod store;
mod worker;

type Failure<'region, E> = store::Failure<E, Infallible, Bytes<'region>>;

#[sark_json::json(ordered)]
struct Score {
    user: Shared,
    value: Shared,
}

impl Score {
    fn try_parse<'region, E>(
        input: ScoreScoped<'region>,
    ) -> Result<(sark_json::Text<Bytes<'region>>, f64), Failure<'region, E>> {
        let ScoreScoped { user, value, .. } = input;
        if user.is_empty() {
            return Err(store::Failure::EmptyUser);
        }
        let number = match value.as_str().parse::<f64>() {
            Ok(number) => number,
            Err(error) => {
                return Err(store::Failure::Number {
                    value: value.into_inner(),
                    error,
                });
            }
        };
        if !number.is_finite() {
            return Err(store::Failure::Nonfinite {
                value: value.into_inner(),
                number,
            });
        }
        Ok((user, number))
    }
}

#[sark_json::json(try_encode)]
struct Added<'region, E: source::Error> {
    ok: bool,
    added: u64,
    total: u64,
    #[field(nested)]
    error: Failure<'region, E>,
}

#[sark_json::json(try_encode)]
struct Listing<C: Storage> {
    ok: bool,
    #[field(seq, nested)]
    scores: Scores<C::Text>,
    #[field(nested)]
    error: store::Failure<C::Error, C::Scored>,
}

#[sark_json::json(try_encode)]
struct Position<'region, E: source::Error> {
    ok: bool,
    found: bool,
    user: Option<Bytes<'region>>,
    rank: u64,
    #[field(nested)]
    error: Failure<'region, E>,
}

#[sark::response(json)]
#[header("content-type", "application/json")]
struct Post<'region, E: source::Error> {
    status: status::Code,
    body: Added<'region, E>,
}

impl<'region, E: source::Error> Post<'region, E> {
    fn failure(added: u64, error: Failure<'region, E>) -> Self {
        Self {
            status: error.status(),
            body: Added {
                ok: false,
                added,
                total: 0,
                error,
            },
        }
    }
}

impl<'region, E: source::Error> From<Failure<'region, E>> for Post<'region, E> {
    fn from(error: Failure<'region, E>) -> Self {
        Self::failure(0, error)
    }
}

#[sark::response(json)]
#[header("content-type", "application/json")]
struct Leaders<C: Storage> {
    status: status::Code,
    body: Listing<C>,
}

impl<C: Storage> From<store::Failure<C::Error, C::Scored>> for Leaders<C> {
    fn from(error: store::Failure<C::Error, C::Scored>) -> Self {
        Self {
            status: error.status(),
            body: Listing {
                ok: false,
                scores: Scores::Empty,
                error,
            },
        }
    }
}

#[sark::response(json)]
#[header("content-type", "application/json")]
struct Placement<'region, E: source::Error> {
    status: status::Code,
    body: Position<'region, E>,
}

impl<'region, E: source::Error> Placement<'region, E> {
    fn failure(user: Option<sark_json::Text<Bytes<'region>>>, error: Failure<'region, E>) -> Self {
        Self {
            status: error.status(),
            body: Position {
                ok: false,
                found: false,
                user,
                rank: 0,
                error,
            },
        }
    }

    fn invalid_text(rejected: Rejected<Bytes<'region>, Utf8Error>) -> Self {
        let (value, error) = rejected.into_parts();
        Self::failure(None, store::Failure::Utf8 { value, error })
    }
}

#[sark::request]
#[json_body(Score)]
struct Submit {}

#[sark::request(ordered)]
struct Top {
    #[query("n", default = "10")]
    n: usize,
}

#[sark::request]
struct Rank {
    #[path]
    user: Shared,
}

#[sark::handler]
async fn post_score<C>(request: Submit, redis: &C) -> Result<Post<'_, C::Error>, Post<'_, C::Error>>
where
    C: Client<'_, REDIS_CONNECTOR_ID>,
{
    let (user, value) = Score::try_parse(request.body)?;
    let added = redis
        .sorted_sets()
        .add(BOARD_KEY, value, user.as_slice())
        .await
        .map_err(store::Failure::Reply)?;
    let total = redis
        .sorted_sets()
        .len(BOARD_KEY)
        .await
        .map_err(|error| Post::failure(added, store::Failure::Reply(error)))?;
    Ok(Post {
        status: status::Code::CREATED,
        body: Added {
            ok: true,
            added,
            total,
            error: store::Failure::Empty,
        },
    })
}

#[sark::handler]
async fn get_top<C>(request: Top, redis: &C) -> Result<Leaders<C>, Leaders<C>>
where
    C: Client<'_, REDIS_CONNECTOR_ID>,
{
    if !(1..=100).contains(&request.n) {
        return Err(store::Failure::InvalidCount(request.n).into());
    }
    let rows = redis
        .sorted_sets()
        .rev_range(BOARD_KEY, 0, request.n as i64 - 1)
        .await
        .map_err(store::Failure::Reply)?;
    let rows = rows.try_text().map_err(store::Failure::Member)?;
    Ok(Leaders {
        status: status::Code::OK,
        body: Listing {
            ok: true,
            scores: Scores::Rows(rows),
            error: store::Failure::Empty,
        },
    })
}

#[sark::handler]
async fn get_rank<C>(
    request: Rank,
    redis: &C,
) -> Result<Placement<'_, C::Error>, Placement<'_, C::Error>>
where
    C: Client<'_, REDIS_CONNECTOR_ID>,
{
    let user = sark_json::Text::try_new(request.user).map_err(Placement::invalid_text)?;
    if user.is_empty() {
        return Err(Placement::failure(Some(user), store::Failure::EmptyUser));
    }
    let rank = match redis
        .sorted_sets()
        .reverse_rank(BOARD_KEY, user.as_slice())
        .await
    {
        Ok(rank) => rank,
        Err(error) => {
            return Err(Placement::failure(Some(user), store::Failure::Reply(error)));
        }
    };
    let error = match rank {
        Some(_) => store::Failure::Empty,
        None => store::Failure::Missing,
    };
    Ok(Placement {
        status: error.status(),
        body: Position {
            ok: true,
            found: rank.is_some(),
            user: Some(user),
            rank: rank.unwrap_or(0),
            error,
        },
    })
}

sark::define_route! {
    App<C>: C where C: Client<'_, REDIS_CONNECTOR_ID> => {
        POST "/score" => async(capacity = MAX_CONNECTIONS, body = json) post_score,
        GET "/top" => request(query) async(capacity = MAX_CONNECTIONS, body = discarded) get_top,
        GET "/rank/:user" => request(path) async(capacity = MAX_CONNECTIONS, body = discarded) get_rank,
    }
}

fn main() -> ExitCode {
    let mut signal = std::mem::MaybeUninit::uninit();
    match store::start(o3::mem::slot::Vacant::new(&mut signal), "leaderboard") {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => error.report(),
    }
}
