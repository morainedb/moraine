//! Checks release coverage across supported DuckDB versions and platforms,
//! including matrix legs that never produced a build job.

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

/// Loads one published build into the stock CLI for the DuckDB it was
/// built for and proves that its bundled DuckLake backfills row-ID
/// statistics and prunes a three-file scan to one file.
///
/// `version` and `platform` name the build the artifact is: a loadable
/// carries both in its footer and loads into nothing else, so a wrong pair
/// fails on the load rather than smoke-testing the wrong artifact.
pub fn validate_release_artifact(arguments: &[String]) -> anyhow::Result<()> {
    let [version, platform, artifact] = arguments else {
        bail!(
            "usage: cargo xtask validate-release-artifact <duckdb-version> <platform> <artifact>, \
             e.g. `… validate-release-artifact v2.0.0 linux_amd64 artifact/moraine.duckdb_extension`"
        );
    };
    let artifact = fs::canonicalize(artifact)
        .with_context(|| format!("resolving the extension artifact {artifact}"))?;
    let cli = duckdb::ensure_released_duckdb_cli(version, platform)?;
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
        "the bundled DuckLake validation failed for the {platform} build of DuckDB {version}"
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
        "ok: the {platform} build's bundled DuckLake records and prunes by row IDs and takes \
         existing delete positions under a stock DuckDB {version}"
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

/// The release an uploaded artifact's directory name carries.
///
/// The reusable build names each artifact `moraine-<token>-extension-<arch>`
/// from the DuckDB version it was given, which is the version a loadable's
/// footer carries and the one the asset is named for.
fn asset_version(token: &str, versions: &[String]) -> Option<String> {
    versions.iter().find(|version| *version == token).cloned()
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

    let versions = supported_duckdb_versions();
    let mut staged = 0;
    for entry in directory_entries(artifacts)? {
        let Some((token, architecture)) = parse_artifact_directory(&entry) else {
            continue;
        };
        let version = asset_version(token, &versions).with_context(|| {
            format!("artifact {entry} names DuckDB `{token}`, which is not a supported release")
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
    #[test]
    fn publication_requires_the_full_ci_workflow_at_the_release_commit() {
        let root = crate::duckdb::workspace_root();
        let ci = std::fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
        let release = std::fs::read_to_string(root.join(".github/workflows/release.yml")).unwrap();
        assert!(ci.contains("  workflow_call:"));
        assert!(release.contains("    uses: ./.github/workflows/ci.yml"));
        let publish = release.split("  publish:\n").nth(1).unwrap();
        assert!(publish.contains("    needs: [should-release, build-extension, validate, ci]"));
        assert!(!publish.contains("always()"));
    }

    use super::*;

    /// A build of something nobody asked for must not be staged under a
    /// supported release's name.
    #[test]
    fn only_a_supported_release_names_an_asset() {
        let versions = vec!["v2.0.0".to_owned()];

        assert_eq!(
            asset_version("v2.0.0", &versions).as_deref(),
            Some("v2.0.0")
        );
        assert_eq!(asset_version("v2.0.0-alpha49307", &versions), None);
        assert_eq!(asset_version("d54cbbe3", &versions), None);
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

    /// Validation covers exactly what a release publishes: a platform built
    /// and never loaded ships unproven.
    #[test]
    fn every_published_platform_is_validated() {
        let contents = fs::read_to_string(
            crate::duckdb::workspace_root().join(crate::pins::VALIDATION_WORKFLOW),
        )
        .expect("reading the validation workflow");
        for platform in PUBLISHED_PLATFORMS {
            assert!(
                contents.contains(&format!("platform: {platform}")),
                "{} never validates {platform}",
                crate::pins::VALIDATION_WORKFLOW
            );
        }
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
