//! The conditions line every measurement prints first, so a recorded
//! number stays interpretable: the commit measured, the host, and the
//! build profile.

use std::process::Command;

/// Prints the measurement's title and the conditions it ran under.
pub fn stamp(title: &str) {
    println!("\n# {title}");
    println!("# conditions: {}", describe());
}

fn describe() -> String {
    let commit = git(&["rev-parse", "--short", "HEAD"]);
    let date = git(&["log", "-1", "--format=%cs"]);
    let cpus = std::thread::available_parallelism().map_or(0, usize::from);
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    format!(
        "moraine {commit} ({date}), {}/{}, {cpus} cpus, {profile} build",
        std::env::consts::OS,
        std::env::consts::ARCH
    )
}

fn git(arguments: &[&str]) -> String {
    Command::new("git")
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or_else(
            || "unknown".to_owned(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        )
}
