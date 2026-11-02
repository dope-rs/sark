use sark_h2::{
    ClientRole, ServerRole,
    conn::{CLIENT_PREFACE, config, machine::Connection},
    frame::{HEADER_LEN, controls::Settings, layout::Header, numbers::SettingId},
};
use sark_http::field::metadata::block::Connect;

fn settings(bytes: &[u8]) -> Settings<'_> {
    let header = Header::parse(bytes).expect("initial frame header");
    let end = HEADER_LEN + header.length.into_usize();
    Settings::parse(header, &bytes[HEADER_LEN..end]).expect("initial settings payload")
}

#[test]
fn exchanges_role_settings() {
    for (push, value) in [(config::Push::Disabled, 0), (config::Push::Enabled, 1)] {
        for connect in [Connect::Disabled, Connect::Enabled] {
            for (streams, limit) in [(None, 32), (Some(16), 16)] {
                let plan = config::Config {
                    local_settings: config::Settings {
                        enable_push: push,
                        enable_connect_protocol: connect,
                        max_concurrent_streams: streams,
                        max_header_list_size: Some(32_768),
                        ..config::Settings::DEFAULT
                    },
                    stream_capacity: 32,
                    ..config::Config::default()
                };
                let mut client =
                    Connection::<ClientRole>::with_config(plan).expect("client configuration");
                let mut server =
                    Connection::<ServerRole>::with_config(plan).expect("server configuration");
                let request = settings(&client.output().first()[CLIENT_PREFACE.len()..]);
                let reply = settings(server.output().first());
                for (sent, count) in [(&request, 6), (&reply, 5)] {
                    assert_eq!(
                        sent.iter().count(),
                        count + usize::from(connect == Connect::Enabled)
                    );
                    assert_eq!(
                        sent.iter()
                            .find(|(id, _)| *id == Some(SettingId::MaxConcurrentStreams)),
                        Some((Some(SettingId::MaxConcurrentStreams), limit))
                    );
                    assert_eq!(
                        sent.iter()
                            .find(|(id, _)| *id == Some(SettingId::MaxHeaderListSize)),
                        Some((Some(SettingId::MaxHeaderListSize), 32_768))
                    );
                }
                assert_eq!(
                    request
                        .iter()
                        .find(|(id, _)| *id == Some(SettingId::EnablePush)),
                    Some((Some(SettingId::EnablePush), value))
                );
                assert!(
                    reply
                        .iter()
                        .all(|(id, _)| id != Some(SettingId::EnablePush))
                );
                assert!(
                    server.ingress().ingest(client.output().first()).is_ok(),
                    "client preface"
                );
                assert!(
                    client.ingress().ingest(server.output().first()).is_ok(),
                    "server preface"
                );
            }
        }
    }
}
