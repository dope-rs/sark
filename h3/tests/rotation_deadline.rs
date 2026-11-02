use crate::common::conn::bridge::{Pair, certificate};
pub mod common;

use dope::core::driver::schedule::clock;
use dope_quic::wire::stream::Id;
use o3::base::time::Positive;
use sark_core::life::session::Config;
use sark_h3::{
    conn::event::Role,
    manifold::{
        Session,
        server::rotation::{Drain, Expired, Failure, State},
    },
};

#[test]
fn closes_once() {
    clock::Scope::run(|clock| {
        let mut rotation = Drain::default();
        let policy = Config::new(Positive::from_secs::<1>(), Positive::from_secs::<1>());
        let now = clock.now();
        rotation.try_open(&policy, now).expect("open");
        let age = rotation.deadline().expect("age deadline");
        let age_due = now.moment_at(age.into_raw());
        let mut h3 = Session::with_role(Role::Server).expect("H3 session");
        assert!(matches!(
            rotation.expire(&policy, &mut h3, age_due),
            Err(Failure::GoAway(sark_h3::conn::Error::ControlMissing))
        ));
        assert_eq!(rotation.deadline(), Some(age));

        let Pair {
            server: mut quic, ..
        } = Pair::try_new(certificate).expect("QUIC pair");
        h3.egress().start(&mut quic).expect("control stream");
        assert_eq!(
            rotation.expire(&policy, &mut h3, age_due).expect("drain"),
            Some(Expired::Draining)
        );
        let drain = rotation.deadline().expect("drain deadline");
        assert!(drain > age);
        assert_eq!(
            rotation.expire(&policy, &mut h3, age_due).expect("waiting"),
            None
        );
        assert_eq!(rotation.deadline(), Some(drain));
        let due = now.moment_at(drain.into_raw());
        assert_eq!(
            rotation.expire(&policy, &mut h3, due).expect("close"),
            Some(Expired::Closing)
        );
        assert_eq!(rotation.deadline(), None);
        assert_eq!(
            rotation.expire(&policy, &mut h3, due).expect("closed"),
            None
        );
        assert!(rotation.accepting().is_err());
        assert!(rotation.admit(Id::ZERO).is_err());
        assert!(rotation.try_open(&policy, due).is_err());
    });
}

#[test]
fn drains_exhausted_ids() {
    clock::Scope::run(|clock| {
        let mut rotation = Drain::default();
        let policy = Config::new(Positive::from_secs::<1>(), Positive::from_secs::<1>());
        let now = clock.now();
        rotation.try_open(&policy, now).expect("open");
        let due = now.moment_at(rotation.deadline().expect("age").into_raw());
        let mut h3 = Session::with_role(Role::Server).expect("H3 session");
        rotation
            .admit(Id::new((1 << 62) - 8).expect("penultimate request"))
            .expect("admit");
        assert!(matches!(
            rotation.expire(&policy, &mut h3, due),
            Err(Failure::GoAway(sark_h3::conn::Error::ControlMissing))
        ));
        rotation
            .admit(Id::new((1 << 62) - 4).expect("last request"))
            .expect("admit");
        assert_eq!(
            rotation.expire(&policy, &mut h3, due),
            Ok(Some(Expired::Draining))
        );
        assert!(rotation.accepting().is_err());
        let end = now.moment_at(rotation.deadline().expect("drain").into_raw());
        assert!(end > due);
        assert_eq!(
            rotation.expire(&policy, &mut h3, end),
            Ok(Some(Expired::Closing))
        );
    });
}
