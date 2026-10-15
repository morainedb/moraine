//! Interrupting a statement cancels the metadata read it is blocked in.
//!
//! DuckLake issues its metadata SQL on a connection it creates and never
//! hands out, so that context's own interrupt flag is one nobody can set.
//! The registered metadata manager records the driving context on it, and a
//! read blocked in the core polls that instead — without the hop a blocked
//! read could only end by finishing.

use std::{
    io::{self, BufRead, BufReader, Read, Write},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};

use crate::helpers::*;

/// How long the armed read stalls: far longer than cancellation needs, so a
/// statement that ends early can only have observed the interrupt. The
/// statement deadline is an hour, so nothing else can end the read first.
const STALL: Duration = Duration::from_mins(1);

/// How long the session is given to report the cancelled read.
const DEADLINE: Duration = Duration::from_secs(20);

struct Session(Child);

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[ignore = "needs the built DuckDB CLI and packaged Moraine extension"]
fn interrupting_a_statement_cancels_the_metadata_read_it_blocked_on() {
    let store = TempDir::new("cancel-read-store");
    let data = TempDir::new("cancel-read-data");
    run_ducklake_sql(
        store.path(),
        data.path(),
        "CREATE TABLE lake.main.t(a BIGINT); INSERT INTO lake.main.t VALUES (1);",
    );

    let mut session = Session(
        Command::new(cli_path())
            .args(["-unsigned", "-csv"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn duckdb CLI"),
    );
    let output = read_lines(session.0.stdout.take().expect("piped stdout"));

    // Attach and read once unstalled, so the arming below lands on the read
    // the interrupt is aimed at rather than on a reader still warming up.
    write_statements(
        &mut session,
        &format!(
            "SET threads=1;\n{}\nATTACH 'ducklake:moraine:{}' AS lake (DATA_PATH '{}');\n\
             SELECT count(*) FROM lake.main.t;\nSELECT 'warm';\n",
            load_statement(),
            store.path().display(),
            data.path().display(),
        ),
    );
    wait_for_line(&output, "warm", DEADLINE).expect("wait for warm-up");

    write_statements(
        &mut session,
        &format!(
            "CALL moraine_fault_stall_next_read({});\nSELECT count(*) FROM lake.main.t;\n",
            STALL.as_millis(),
        ),
    );
    // The stall is the first thing the read awaits, so it is blocked well
    // inside this wait.
    std::thread::sleep(Duration::from_secs(2));

    let interrupted_at = Instant::now();
    assert!(
        Command::new("kill")
            .args(["-INT", &session.0.id().to_string()])
            .status()
            .expect("failed to signal the session")
            .success()
    );

    drop(session.0.stdin.take());
    let status = wait_for_exit(&mut session);
    assert!(
        interrupted_at.elapsed() < STALL / 2,
        "the session outlasted the stall, so the interrupt never reached the blocked read"
    );
    // The shell reports an interrupted statement by its exit status rather
    // than a message, and `Some(1)` is an error exit: a session killed by
    // the signal would carry no code at all.
    assert_eq!(
        status.code(),
        Some(1),
        "the interrupted session must exit on the failed statement"
    );

    let remainder = output
        .into_iter()
        .map(|line| line.expect("read session output"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        remainder.contains(&STALL.as_millis().to_string()),
        "the arming never ran: {remainder}"
    );
    assert!(
        !remainder.contains("count_star()"),
        "the stalled read answered instead of being cancelled: {remainder}"
    );
}

fn write_statements(session: &mut Session, sql: &str) {
    let input = session.0.stdin.as_mut().expect("piped stdin");
    input.write_all(sql.as_bytes()).expect("write statements");
    input.flush().expect("flush statements");
}

fn read_lines(output: impl Read + Send + 'static) -> Receiver<io::Result<String>> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    receiver
}

fn wait_for_line(
    output: &Receiver<io::Result<String>>,
    marker: &str,
    timeout: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let line = output
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))??;
        if line.trim() == marker {
            return Ok(());
        }
    }
}

#[test]
#[cfg(unix)]
fn silent_output_obeys_the_warmup_deadline() {
    let (reader, writer) = std::os::unix::net::UnixStream::pair().unwrap();
    let output = read_lines(reader);
    let start = Instant::now();
    let error = wait_for_line(&output, "warm", Duration::from_millis(20)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(start.elapsed() < Duration::from_secs(2));
    drop(writer);
}

fn wait_for_exit(session: &mut Session) -> std::process::ExitStatus {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(status) = session.0.try_wait().expect("poll the session") {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "the session never ended: the interrupt did not reach the blocked read"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
