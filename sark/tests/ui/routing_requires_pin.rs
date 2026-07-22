use http::StatusCode;
use sark::dispatch::Routing;

#[sark_gen::request]
struct Request {}

#[sark_gen::response(raw)]
struct Reply {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::handler]
fn handle(_request: Request, _state: &()) -> Reply {
    Reply {
        status: StatusCode::OK,
        body: b"ok",
    }
}

sark_gen::define_route! {
    App: () => {
        GET "/" => handle,
    }
}

fn main() {
    let mut app = App::new(
        (),
        sark::app::Config {
            timer_capacity: 1,
            task_capacity: 1,
        },
    );
    let mut write = [0; 1024];
    let mut conn = sark::dispatch::conn_state::ConnState::default();
    Routing::try_consume(
        &mut app,
        sark::dispatch::conn_state::DispatchPermit::new(),
        b"GET / HTTP/1.1\r\n\r\n",
        &mut write,
        &mut conn,
    );
}
