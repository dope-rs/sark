use std::net::SocketAddr;

use cartel_pg::protocol::PickPolicy;
pub use clap::Error;
use clap::{Arg, ArgMatches, Command, error::ErrorKind, parser::MatchesError};

pub struct Bind {
    pub bind: SocketAddr,
}

pub struct Redis {
    pub bind: Bind,
    pub redis_addr: SocketAddr,
}

pub struct Postgres {
    pub bind: Bind,
    pub pg_addr: SocketAddr,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
    pub pg_pick_policy: PickPolicy,
}

impl Bind {
    pub fn try_parse() -> Result<Self, Error> {
        Self::take(&mut command("download").try_get_matches()?)
    }

    fn take(matches: &mut ArgMatches) -> Result<Self, Error> {
        Ok(Self {
            bind: value("bind", matches.try_remove_one::<SocketAddr>("bind"))?,
        })
    }
}

impl Redis {
    pub fn try_parse() -> Result<Self, Error> {
        let mut matches = command("redis")
            .arg(argument("redis-addr", "127.0.0.1:6379").value_parser(str::parse::<SocketAddr>))
            .try_get_matches()?;
        Ok(Self {
            bind: Bind::take(&mut matches)?,
            redis_addr: value(
                "redis-addr",
                matches.try_remove_one::<SocketAddr>("redis-addr"),
            )?,
        })
    }
}

impl Postgres {
    pub fn try_parse() -> Result<Self, Error> {
        let mut matches = command("pg")
            .arg(argument("pg-addr", "127.0.0.1:5432").value_parser(str::parse::<SocketAddr>))
            .arg(argument("pg-user", "postgres"))
            .arg(
                argument("pg-password", "postgres")
                    .hide_default_value(true)
                    .allow_hyphen_values(true),
            )
            .arg(argument("pg-database", "postgres"))
            .arg(argument("pg-pick-policy", "round-robin").value_parser(policy))
            .try_get_matches()?;
        Ok(Self {
            bind: Bind::take(&mut matches)?,
            pg_addr: value("pg-addr", matches.try_remove_one::<SocketAddr>("pg-addr"))?,
            pg_user: value("pg-user", matches.try_remove_one::<String>("pg-user"))?,
            pg_password: value(
                "pg-password",
                matches.try_remove_one::<String>("pg-password"),
            )?,
            pg_database: value(
                "pg-database",
                matches.try_remove_one::<String>("pg-database"),
            )?,
            pg_pick_policy: value(
                "pg-pick-policy",
                matches.try_remove_one::<PickPolicy>("pg-pick-policy"),
            )?,
        })
    }
}

fn command(name: &'static str) -> Command {
    Command::new(name)
        .arg(argument("bind", "127.0.0.1:8080").value_parser(str::parse::<SocketAddr>))
}

fn argument(name: &'static str, default: &'static str) -> Arg {
    Arg::new(name).long(name).default_value(default)
}

fn value<T>(id: &str, result: Result<Option<T>, MatchesError>) -> Result<T, Error> {
    match result {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(Error::raw(
            ErrorKind::MissingRequiredArgument,
            format_args!("missing --{id}"),
        )),
        Err(error) => Err(Error::raw(
            ErrorKind::InvalidValue,
            format_args!("invalid --{id}: {error}"),
        )),
    }
}

fn policy(value: &str) -> Result<PickPolicy, Error> {
    if ["round-robin", "round_robin", "rr"]
        .iter()
        .any(|alias| value.eq_ignore_ascii_case(alias))
    {
        Ok(PickPolicy::RoundRobin)
    } else if ["least-inflight", "least_inflight", "li"]
        .iter()
        .any(|alias| value.eq_ignore_ascii_case(alias))
    {
        Ok(PickPolicy::LeastInflight)
    } else {
        Err(Error::raw(
            ErrorKind::InvalidValue,
            "expected round-robin or least-inflight",
        ))
    }
}
