//! The `check-release-assets` task: a release carries a build for every
//! supported DuckDB version on every platform the workflows publish.
//!
//! A build that never happened leaves almost nothing behind to read. The
//! matrix is generated inside a reusable upstream workflow, and a leg it
//! declines to start produces no job, no check run and no annotation: the
//! run goes red with every visible job green. This is what names the
//! builds that are missing — and what a release is missing is a DuckDB
//! version that cannot load moraine on that platform at all, since a
//! C++-ABI extension is refused by any DuckDB but the one it names.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};

use crate::duckdb::{self, supported_duckdb_versions};

/// Exercises the bundled DuckLake's patch series against a published build.
const RELEASE_SMOKE: &str = "patches/ducklake/release-smoke.sql";

/// The platforms the extension workflows publish: extension-ci-tools'
/// distribution matrix, minus the entries that are opt-in there and the
/// ones `exclude_archs` names in `extension.yml` and `release.yml`.
const PUBLISHED_PLATFORMS: [&str; 4] = ["linux_amd64", "linux_arm64", "osx_amd64", "osx_arm64"];

/// The workflows that build the extension, each naming what it excludes.
/// Read only by the test holding `PUBLISHED_PLATFORMS` to them.
#[cfg(test)]
const BUILD_WORKFLOWS: [&str; 2] = [
    ".github/workflows/extension.yml",
    ".github/workflows/release.yml",
];

/// Fails unless `directory` holds one extension per supported DuckDB
/// version per published platform, naming every build that is missing.
pub fn check_release_assets(arguments: &[String]) -> anyhow::Result<()> {
    let Some(directory) = arguments.first() else {
        bail!(
            "usage: cargo xtask check-release-assets <directory>, e.g. `… check-release-assets dist`"
        );
    };

    let present = directory_entries(directory)?;

    let versions = supported_duckdb_versions();
    let missing = missing_assets(&present, &versions);
    ensure!(
        missing.is_empty(),
        "{directory} holds {} of the {} builds a release needs; missing:\n  - {}\n\
         A matrix leg that never starts leaves no failed job, so check that every \
         version's build jobs exist before re-running.",
        expected_assets(&versions).len() - missing.len(),
        expected_assets(&versions).len(),
        missing.join("\n  - ")
    );

    println!(
        "ok: {} builds present — {} on {}",
        present.len(),
        versions.join(", "),
        PUBLISHED_PLATFORMS.join(", ")
    );
    Ok(())
}

/// Loads one build and proves that its bundled DuckLake backfills row-ID
/// statistics and prunes a three-file scan to one file.
///
/// `version` names the DuckDB the build is for, so a bump that leaves a
/// stale invocation behind fails here instead of smoke-testing the wrong
/// artifact.
pub fn validate_release_artifact(arguments: &[String]) -> anyhow::Result<()> {
    let [version, artifact] = arguments else {
        bail!("usage: cargo xtask validate-release-artifact <duckdb-version> <artifact>");
    };
    ensure!(
        version == duckdb::duckdb_pin(),
        "this build is for DuckDB {}, not {version}: only the pinned version has a CLI \
         that can load it",
        duckdb::duckdb_pin()
    );
    let artifact = fs::canonicalize(artifact)
        .with_context(|| format!("resolving the extension artifact {artifact}"))?;
    let cli = duckdb::ensure_duckdb_cli()?;
    let script_path = duckdb::workspace_root().join(RELEASE_SMOKE);
    let script = fs::read_to_string(&script_path)
        .with_context(|| format!("reading {}", script_path.display()))?;
    let artifact_literal = artifact.display().to_string().replace('\'', "''");
    let script = format!("LOAD '{artifact_literal}';\n{script}");
    let validation = TemporaryDirectory::create()?;

    let output = Command::new(&cli)
        .arg("-unsigned")
        .args(["-c", &script])
        .current_dir(validation.path())
        .output()
        .with_context(|| format!("spawning {}", cli.display()))?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    ensure!(
        output.status.success(),
        "the bundled DuckLake validation failed for DuckDB {version}"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let row_id_stat_rows = stdout.matches("2147483540").count();
    ensure!(
        row_id_stat_rows == 3 && stdout.contains("Total Files Read: 1"),
        "the build for DuckDB {version} recorded {row_id_stat_rows} of 3 expected row-ID \
         statistic rows or did not prune the scan to one file"
    );
    ensure!(
        stdout.contains("remaining=8"),
        "the build for DuckDB {version} did not stage a positional delete carrying existing \
         delete positions; its bundled DuckLake is missing part of the patch series"
    );
    println!(
        "ok: the bundled DuckLake records and prunes by row IDs and takes existing delete \
         positions under DuckDB {version}"
    );
    Ok(())
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn create() -> anyhow::Result<Self> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system time is before the Unix epoch")?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "moraine-release-validation-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).with_context(|| format!("creating {}", path.display()))?;
        Ok(Self { path })
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn directory_entries(directory: &str) -> anyhow::Result<Vec<String>> {
    fs::read_dir(directory)
        .with_context(|| format!("reading the release directory {directory}"))?
        .map(|entry| {
            entry
                .with_context(|| format!("reading an entry of {directory}"))
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
        })
        .collect()
}

/// The release version an uploaded artifact's directory name carries.
///
/// The reusable build names each artifact `moraine-<token>-extension-<arch>`,
/// where `<token>` is whatever it resolved `DUCKDB_VERSION` to: the release
/// when it was given one, and the checkout's commit when it built the pinned
/// submodule instead. Both have to land on the release the manifest names,
/// since that is the version a loadable's footer carries and the one the
/// asset is named for.
fn asset_version(token: &str, pins: &[(String, String)]) -> Option<String> {
    pins.iter()
        .find(|(version, commit)| token == version || token == commit)
        .map(|(version, _)| version.clone())
}

/// Stages every uploaded artifact under the name a release asset takes.
///
/// Replaces deriving the name in shell: the token an artifact carries is not
/// always the version, and a mis-derived name reads as a missing build rather
/// than a wrong one.
pub fn stage_release_assets(arguments: &[String]) -> anyhow::Result<()> {
    let (Some(artifacts), Some(destination)) = (arguments.first(), arguments.get(1)) else {
        bail!(
            "usage: cargo xtask stage-release-assets <artifacts-dir> <dist-dir>, \
             e.g. `… stage-release-assets artifacts dist`"
        );
    };
    fs::create_dir_all(destination)
        .with_context(|| format!("creating the staging directory {destination}"))?;

    let pins = release_pins();
    let mut staged = 0;
    for entry in directory_entries(artifacts)? {
        let Some((token, architecture)) = parse_artifact_directory(&entry) else {
            continue;
        };
        let version = asset_version(token, &pins).with_context(|| {
            format!(
                "artifact {entry} names DuckDB `{token}`, which is neither a supported \
                 release nor the commit one of them pins"
            )
        })?;
        let from = Path::new(artifacts)
            .join(&entry)
            .join("moraine.duckdb_extension");
        let to = Path::new(destination)
            .join(format!("moraine.{version}.{architecture}.duckdb_extension"));
        fs::copy(&from, &to)
            .with_context(|| format!("staging {} to {}", from.display(), to.display()))?;
        staged += 1;
    }

    ensure!(
        staged > 0,
        "{artifacts} holds no moraine extension artifact"
    );
    println!("ok: staged {staged} artifacts into {destination}");
    Ok(())
}

/// The `(token, architecture)` an artifact directory name carries.
fn parse_artifact_directory(entry: &str) -> Option<(&str, &str)> {
    let rest = entry.strip_prefix("moraine-")?;
    let (token, architecture) = rest.split_once("-extension-")?;
    (!token.is_empty() && !architecture.is_empty()).then_some((token, architecture))
}

/// Every supported release and the DuckDB commit it pins, where it names
/// one. Only the primary entry carries commits.
fn release_pins() -> Vec<(String, String)> {
    let primary = crate::duckdb::primary_submodule_pins()
        .into_iter()
        .find(|(path, _)| path == "duckdb")
        .map(|(_, commit)| commit)
        .unwrap_or_default();
    supported_duckdb_versions()
        .into_iter()
        .enumerate()
        .map(|(index, version)| {
            let commit = if index == 0 {
                primary.clone()
            } else {
                String::new()
            };
            (version, commit)
        })
        .collect()
}

/// Every asset a release must carry, in the name the publish step gives it.
fn expected_assets(versions: &[String]) -> Vec<String> {
    versions
        .iter()
        .flat_map(|version| {
            PUBLISHED_PLATFORMS
                .iter()
                .map(move |platform| format!("moraine.{version}.{platform}.duckdb_extension"))
        })
        .collect()
}

/// The expected assets that `present` does not name.
fn missing_assets(present: &[String], versions: &[String]) -> Vec<String> {
    expected_assets(versions)
        .into_iter()
        .filter(|asset| !present.iter().any(|candidate| candidate == asset))
        .collect()
}

/// The architectures a workflow's `exclude_archs` input names.
#[cfg(test)]
fn excluded_architectures(contents: &str) -> Vec<&str> {
    let marker = "exclude_archs: \"";
    let Some(start) = contents.find(marker).map(|index| index + marker.len()) else {
        return Vec::new();
    };
    let Some(rest) = contents.get(start..) else {
        return Vec::new();
    };
    let Some(end) = rest.find('"') else {
        return Vec::new();
    };
    rest[..end]
        .split(';')
        .filter(|arch| !arch.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An artifact built from the pinned submodule carries the commit, not
    /// the release, and still has to land on the release's asset name.
    #[test]
    fn an_artifact_named_for_the_pinned_commit_resolves_to_its_release() {
        let pins = vec![("v2.0.0".to_owned(), "d54cbbe3".to_owned())];

        assert_eq!(asset_version("d54cbbe3", &pins).as_deref(), Some("v2.0.0"));
        assert_eq!(asset_version("v2.0.0", &pins).as_deref(), Some("v2.0.0"));
    }

    /// A token naming neither is a build of something nobody asked for,
    /// which must not be staged under a supported release's name.
    #[test]
    fn an_unrecognized_artifact_token_resolves_to_nothing() {
        let pins = vec![("v2.0.0".to_owned(), "d54cbbe3".to_owned())];

        assert_eq!(asset_version("v2.0.0-alpha49307", &pins), None);
    }

    #[test]
    fn an_artifact_directory_yields_its_token_and_architecture() {
        assert_eq!(
            parse_artifact_directory("moraine-d54cbbe3-extension-linux_amd64"),
            Some(("d54cbbe3", "linux_amd64"))
        );
        assert_eq!(parse_artifact_directory("something-else"), None);
        assert_eq!(
            parse_artifact_directory("moraine--extension-linux_amd64"),
            None
        );
    }

    fn versions() -> Vec<String> {
        vec!["v1.5.5".to_owned(), "v1.5.4".to_owned()]
    }

    #[test]
    fn a_complete_set_is_accepted_and_a_gap_names_what_is_absent() {
        let complete = expected_assets(&versions());
        assert_eq!(complete.len(), 8);
        assert!(missing_assets(&complete, &versions()).is_empty());

        // The failure this exists for: one version built for Linux only.
        let partial: Vec<String> = complete
            .iter()
            .filter(|asset| !(asset.contains("v1.5.4") && asset.contains("osx")))
            .cloned()
            .collect();
        assert_eq!(
            missing_assets(&partial, &versions()),
            vec![
                "moraine.v1.5.4.osx_amd64.duckdb_extension".to_owned(),
                "moraine.v1.5.4.osx_arm64.duckdb_extension".to_owned(),
            ]
        );
    }

    /// Assets are matched by their whole name: another version's build is
    /// not one of these, however many of them there are.
    #[test]
    fn a_build_of_another_version_does_not_stand_in() {
        let present = vec![
            "moraine.v1.5.3.osx_arm64.duckdb_extension".to_owned(),
            "moraine.v1.5.5.osx_arm64.duckdb_extension".to_owned(),
        ];
        let missing = missing_assets(&present, &["v1.5.5".to_owned()]);
        assert_eq!(missing.len(), PUBLISHED_PLATFORMS.len() - 1);
        assert!(!missing.contains(&"moraine.v1.5.5.osx_arm64.duckdb_extension".to_owned()));
    }

    #[test]
    fn exclusions_are_read_out_of_the_workflow_input() {
        assert_eq!(
            excluded_architectures("      exclude_archs: \"wasm_mvp;windows_amd64\"\n"),
            vec!["wasm_mvp", "windows_amd64"]
        );
        assert!(excluded_architectures("no exclusions here").is_empty());
    }

    /// The platform list and the workflows' exclusions describe one set:
    /// a platform cannot be both published and excluded, and the two
    /// workflows must exclude the same thing or they publish different
    /// releases.
    #[test]
    fn the_published_platforms_are_the_ones_the_workflows_do_not_exclude() {
        let mut exclusions = Vec::new();
        for file in BUILD_WORKFLOWS {
            let contents = fs::read_to_string(crate::duckdb::workspace_root().join(file))
                .expect("reading a build workflow");
            let excluded: Vec<String> = excluded_architectures(&contents)
                .iter()
                .map(|arch| (*arch).to_owned())
                .collect();
            assert!(!excluded.is_empty(), "{file} names no exclude_archs");
            for platform in PUBLISHED_PLATFORMS {
                assert!(
                    !excluded.iter().any(|arch| arch == platform),
                    "{file} excludes `{platform}`, which check-release-assets requires"
                );
            }
            exclusions.push(excluded);
        }
        assert_eq!(
            exclusions[0], exclusions[1],
            "{} and {} exclude different architectures",
            BUILD_WORKFLOWS[0], BUILD_WORKFLOWS[1]
        );
    }
}
