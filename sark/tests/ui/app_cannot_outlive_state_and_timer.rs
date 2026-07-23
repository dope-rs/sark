#[sark_gen::request]
struct Request {}

#[sark_gen::response(raw)]
struct Reply {
    status: http::StatusCode,
    body: &'static [u8],
}

#[sark_gen::handler]
fn handle(_request: Request, _state: &()) -> Reply {
    Reply {
        status: http::StatusCode::OK,
        body: b"ok",
    }
}

sark_gen::define_route! {
    App: () => {
        GET "/" => handle,
    }
}

fn escape<'d>() -> impl sark::Application<'d, Wire = dope_net::wire::identity::Identity> {
    let state = ();
    let timer = sark::Timer::with_capacity(1);
    App::new(
        &state,
        &timer,
        sark::app::Config {
            timer_capacity: 1,
            task_capacity: 1,
        },
    )
}

fn main() {}
