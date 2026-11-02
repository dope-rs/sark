use sark_grpc::{
    message,
    server::config::{
        Input,
        error::{Error, Field, Operation},
    },
};

#[test]
fn segment_capacity() {
    let maximum = u32::MAX as usize;
    let admitted = Input::try_new(1, maximum, 0, 0).expect("maximum segment count");
    assert_eq!(admitted.segments(), maximum);
    let Some(demand) = maximum.checked_add(1) else {
        return;
    };
    std::assert_matches!(
        Input::try_new(1, demand, 0, 0)
            .err()
            .expect("segment count exceeds its index range"),
        Error::MaximumCapacity {
            field: Field::InputSegments,
            maximum: actual,
            error,
        } if actual == maximum && error.requested() == demand
    );
}

#[test]
fn error_precedence() {
    std::assert_matches!(
        Input::try_new(usize::MAX, 0, usize::MAX, usize::MAX)
            .err()
            .expect("zero segments precede other invalid fields"),
        Error::ZeroCapacity(Field::InputSegments)
    );
    let maximum = u32::MAX as usize;
    let Some(demand) = maximum.checked_add(1) else {
        return;
    };
    std::assert_matches!(
        Input::try_new(demand, demand, usize::MAX, usize::MAX)
            .err()
            .expect("message count precedes segment narrowing"),
        Error::MaximumCapacity {
            field: Field::InputMessages,
            maximum: actual,
            error,
        } if actual == maximum && error.requested() == demand
    );
    for (stream, field) in [
        (usize::MAX, Field::StreamWireBytes),
        (0, Field::ConnectionWireBytes),
    ] {
        std::assert_matches!(
            Input::try_new(1, demand, stream, usize::MAX)
                .err()
                .expect("wire overflow precedes segment narrowing"),
            Error::Overflow {
                field: actual,
                operation: Operation::Add,
                left: usize::MAX,
                right,
            } if actual == field && right == message::HEADER_LEN
        );
    }
}
