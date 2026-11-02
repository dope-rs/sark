pub mod common;

use common::protocol::conn::Controls;
use sark_h2::{
    ClientRole,
    conn::{Error, config::Push, event::Event, machine::Connection},
    frame::{Ack, numbers::SettingId},
};

#[test]
fn accepts_only_zero() {
    for value in [0, 1, 2] {
        let mut conn = Connection::<ClientRole>::try_new().expect("client fixture");
        assert!(matches!(
            conn.ingress().poll_event(),
            Some(Event::PrefaceComplete)
        ));
        conn.transfer().drain(usize::MAX);
        let settings = Controls::new(Ack::Required)
            .try_settings(&[(SettingId::EnablePush as u16, value)])
            .expect("settings fixture");
        let result = conn.ingress().ingest(&settings);
        if value == 0 {
            result.expect("server may disable push");
            assert!(conn.settings().peer().enable_push == Push::Disabled);
            assert_eq!(
                conn.output().first(),
                Controls::new(Ack::Complete)
                    .try_settings(&[])
                    .expect("settings ACK fixture")
            );
            assert!(matches!(
                conn.ingress().poll_event(),
                Some(Event::SettingsApplied)
            ));
        } else {
            assert!(matches!(
                result,
                Err(Error::Setting {
                    id: SettingId::EnablePush,
                    value: actual,
                }) if actual == value
            ));
            assert!(conn.output().first().is_empty());
            assert!(conn.ingress().poll_event().is_none());
        }
    }
}
