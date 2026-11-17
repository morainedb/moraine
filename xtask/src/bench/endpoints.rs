//! Round trips to the endpoints a run talks to. Every catalog here costs
//! several sequential requests per commit, so a result is only readable
//! beside the latency of the endpoint that served it.

use std::{
    net::{TcpStream, ToSocketAddrs as _},
    time::{Duration, Instant},
};

use super::timing::median;

/// How many connects a probe times. The median of five is steady enough
/// to quote and costs nothing beside a sweep.
const SAMPLES: usize = 5;

/// How long a probe waits before calling an endpoint unreachable.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// One endpoint's measured round trip.
pub struct Endpoint {
    pub name: String,
    pub target: String,
    pub seconds: f64,
}

/// The `host:port` a Postgres DSN points at, or `None` when it names a
/// Unix socket or no host at all.
pub fn postgres_target(dsn: &str) -> Option<String> {
    let mut host = None;
    let mut port = "5432";
    for field in dsn.split_whitespace() {
        match field.split_once('=') {
            Some(("host", value)) => host = Some(value),
            Some(("port", value)) => port = value,
            _ => {}
        }
    }

    let host = host?;
    (!host.starts_with('/')).then(|| format!("{host}:{port}"))
}

/// The median time to open a TCP connection to `target`, which is one
/// round trip plus the kernel's work on either side.
pub fn measure(target: &str) -> anyhow::Result<f64> {
    let address = target
        .to_socket_addrs()
        .map_err(|error| anyhow::anyhow!("resolving {target}: {error}"))?
        .next()
        .ok_or_else(|| anyhow::anyhow!("{target} resolved to no address"))?;

    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)
            .map_err(|error| anyhow::anyhow!("connecting to {target}: {error}"))?;
        samples.push(started.elapsed().as_secs_f64());
    }
    median(&mut samples)
}

/// Probes each named endpoint, reporting what it measured and what it
/// could not reach. A probe failure is not a run failure: the sweep can
/// still be measured without its latency recorded.
pub fn probe(targets: &[(String, String)]) -> Vec<Endpoint> {
    let mut endpoints = Vec::new();
    for (name, target) in targets {
        match measure(target) {
            Ok(seconds) => endpoints.push(Endpoint {
                name: name.clone(),
                target: target.clone(),
                seconds,
            }),
            Err(error) => println!("notice: no round trip measured for {name} — {error:#}"),
        }
    }
    endpoints
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dsn_yields_its_host_and_port() {
        assert_eq!(
            postgres_target("host=db.example.com port=5433 user=bench"),
            Some("db.example.com:5433".to_owned())
        );
        assert_eq!(
            postgres_target("host=db.example.com user=bench"),
            Some("db.example.com:5432".to_owned())
        );
    }

    #[test]
    fn a_unix_socket_dsn_has_no_round_trip_to_measure() {
        assert_eq!(postgres_target("host=/tmp/pgsocket user=bench"), None);
        assert_eq!(postgres_target("user=bench"), None);
    }
}
