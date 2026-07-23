use http::StatusCode;

struct State {
    value: &'static [u8],
}

#[sark_gen::response(raw)]
struct Output {
    status: StatusCode,
    body: &'static [u8],
}

#[sark_gen::handler]
async fn state_only(state: &State) -> Output {
    Output {
        status: StatusCode::OK,
        body: state.value,
    }
}

sark_gen::define_route! {
    App: State => {
        GET "/" => async(capacity = 1) state_only,
    }
}

#[test]
fn state_only_handler_builds_without_request_plumbing() {
    let state = State { value: b"ok" };
    let timer = sark::Timer::with_capacity(1);
    let _ = App::new::<dope_net::wire::identity::Identity>(
        &state,
        &timer,
        sark::app::Config {
            timer_capacity: 1,
            task_capacity: 1,
        },
    );
}
