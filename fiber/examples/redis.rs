use std::process::{ExitCode, Termination};

use cartel_redis::{api::Client, client::nonempty, value::source};
use sark_http::payload::response::status;

mod error;
pub mod input;
pub mod store;
mod worker;

const MAX_CONNECTIONS: usize = 1024;
const HTTP_LISTENER_ID: u8 = 0;
const DATE_UPDATER_ID: u8 = 1;
const REDIS_CONNECTOR_ID: u8 = 2;
const VISIT_KEY: &[u8] = b"sark:visits";
#[sark_json::json(try_encode)]
struct Body<E: source::Error> {
    ok: bool,
    visits: i64,
    #[field(nested)]
    error: store::Failure<E>,
}

#[sark::response(json)]
#[header("content-type", "application/json")]
struct Response<E: source::Error> {
    status: status::Code,
    body: Body<E>,
}

impl<E: source::Error> Response<E> {
    fn new(status: status::Code, ok: bool, visits: i64, error: store::Failure<E>) -> Self {
        Self {
            status,
            body: Body { ok, visits, error },
        }
    }

    fn success(visits: i64) -> Self {
        Self::new(status::Code::OK, true, visits, store::Failure::Empty)
    }

    fn failure(error: E) -> Self {
        Self::new(
            status::Code::BAD_GATEWAY,
            false,
            0,
            store::Failure::Reply(error),
        )
    }
}

#[sark::handler]
async fn visit<C>(redis: &C) -> Response<C::Error>
where
    C: Client<'_, REDIS_CONNECTOR_ID>,
{
    match redis.basic().incr(VISIT_KEY).await {
        Ok(visits) => Response::success(visits),
        Err(error) => Response::failure(error),
    }
}

#[sark::handler]
async fn reset<C>(redis: &C) -> Response<C::Error>
where
    C: Client<'_, REDIS_CONNECTOR_ID>,
{
    let keys = nonempty::Slice::from_ref(&VISIT_KEY);
    match redis.basic().del(keys).await {
        Ok(_) => Response::success(0),
        Err(error) => Response::failure(error),
    }
}

sark::define_route! {
    App<C>: C where C: Client<'_, REDIS_CONNECTOR_ID> => {
        GET "/" => async(capacity = MAX_CONNECTIONS, body = discarded) visit,
        POST "/reset" => async(capacity = MAX_CONNECTIONS, body = discarded) reset,
    }
}

fn main() -> ExitCode {
    let mut signal = std::mem::MaybeUninit::uninit();
    match store::start(o3::mem::slot::Vacant::new(&mut signal), "redis") {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => error.report(),
    }
}
