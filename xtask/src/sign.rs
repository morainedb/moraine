//! Signs staged extension builds for a DuckDB extension repository: the
//! trailing 256 bytes of each build become an RSA signature over the rest,
//! and the result is gzipped under the repository's asset name.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, bail, ensure};
use sha2::{Digest, Sha256};

/// The signature footer every build reserves, zeroed until signed.
const SIGNATURE_BYTES: usize = 256;

/// DuckDB hashes the bytes before the signature in pieces of this size.
const CHUNK_BYTES: usize = 1024 * 1024;

/// The value DuckDB verifies: the SHA-256 of the concatenated SHA-256 of
/// each 1 MiB chunk of `bytes`.
pub fn extension_hash(bytes: &[u8]) -> [u8; 32] {
    let mut concatenated = Vec::with_capacity(bytes.len().div_ceil(CHUNK_BYTES) * 32);
    for chunk in bytes.chunks(CHUNK_BYTES) {
        concatenated.extend_from_slice(&Sha256::digest(chunk));
    }
    Sha256::digest(&concatenated).into()
}

/// The release asset name a signed build is published under.
pub fn signed_asset_name(asset: &str) -> Option<String> {
    asset
        .strip_suffix(".duckdb_extension")
        .filter(|stem| stem.starts_with("moraine."))
        .map(|_| format!("{asset}.gz"))
}

/// Signs every staged build in `dist` with the PEM private key at `key`
/// and writes `<name>.duckdb_extension.gz` files into `signed`.
pub fn sign_release_assets(arguments: &[String]) -> anyhow::Result<()> {
    let (Some(dist), Some(signed), Some(key)) =
        (arguments.first(), arguments.get(1), arguments.get(2))
    else {
        bail!(
            "usage: cargo xtask sign-release-assets <dist-dir> <signed-dir> <private-key.pem>, \
             e.g. `… sign-release-assets dist signed signing-key.pem`"
        );
    };
    let key = Path::new(key);
    let public_key = public_key_of(key)?;
    fs::create_dir_all(signed).with_context(|| format!("creating {signed}"))?;

    let mut count = 0;
    let mut entries: Vec<_> = fs::read_dir(dist)
        .with_context(|| format!("reading {dist}"))?
        .collect::<Result<_, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(target) = signed_asset_name(&name) else {
            continue;
        };
        sign_build(
            &entry.path(),
            &Path::new(signed).join(target),
            key,
            &public_key,
        )?;
        count += 1;
    }

    ensure!(count > 0, "{dist} holds no moraine extension build to sign");
    println!("ok: signed {count} builds into {signed}");
    Ok(())
}

/// Writes the public half of the private key beside it, for verification.
fn public_key_of(key: &Path) -> anyhow::Result<PathBuf> {
    let public = PathBuf::from(format!("{}.pub", key.display()));
    let output = Command::new("openssl")
        .args(["rsa", "-pubout", "-in"])
        .arg(key)
        .arg("-out")
        .arg(&public)
        .output()
        .context("running openssl; it must be on PATH")?;
    ensure!(
        output.status.success(),
        "openssl could not read the signing key: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(public)
}

/// Signs one build and verifies the signature before writing the gzip, so
/// a bad key fails the release rather than publishing an unloadable file.
fn sign_build(build: &Path, target: &Path, key: &Path, public_key: &Path) -> anyhow::Result<()> {
    let bytes = fs::read(build).with_context(|| format!("reading {}", build.display()))?;
    ensure!(
        bytes.len() > SIGNATURE_BYTES,
        "{} is smaller than the signature footer",
        build.display()
    );
    let (body, footer) = bytes.split_at(bytes.len() - SIGNATURE_BYTES);
    ensure!(
        footer.iter().all(|byte| *byte == 0),
        "{} already carries a signature",
        build.display()
    );

    let work = tempfile_dir()?;
    let hash_path = work.join("hash");
    let signature_path = work.join("signature");
    fs::write(&hash_path, extension_hash(body))?;

    let signed = Command::new("openssl")
        .args(["pkeyutl", "-sign", "-pkeyopt", "digest:sha256", "-in"])
        .arg(&hash_path)
        .arg("-inkey")
        .arg(key)
        .arg("-out")
        .arg(&signature_path)
        .output()
        .context("running openssl")?;
    ensure!(
        signed.status.success(),
        "signing {} failed: {}",
        build.display(),
        String::from_utf8_lossy(&signed.stderr)
    );
    let verified = Command::new("openssl")
        .args([
            "pkeyutl",
            "-verify",
            "-pubin",
            "-pkeyopt",
            "digest:sha256",
            "-in",
        ])
        .arg(&hash_path)
        .arg("-inkey")
        .arg(public_key)
        .arg("-sigfile")
        .arg(&signature_path)
        .output()
        .context("running openssl")?;
    ensure!(
        verified.status.success(),
        "the signature on {} does not verify: {}",
        build.display(),
        String::from_utf8_lossy(&verified.stderr)
    );

    let signature = fs::read(&signature_path)?;
    ensure!(
        signature.len() == SIGNATURE_BYTES,
        "openssl produced a {}-byte signature; the key must be 2048-bit RSA",
        signature.len()
    );
    let file =
        fs::File::create(target).with_context(|| format!("creating {}", target.display()))?;
    let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    encoder.write_all(body)?;
    encoder.write_all(&signature)?;
    encoder.finish()?;
    fs::remove_dir_all(&work)?;
    Ok(())
}

fn tempfile_dir() -> anyhow::Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("moraine-sign-{}", std::process::id()));
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    #[test]
    fn a_small_build_hashes_as_the_digest_of_its_digest() {
        let data = b"moraine".repeat(100);
        let expected: [u8; 32] = Sha256::digest(Sha256::digest(&data)).into();
        assert_eq!(extension_hash(&data), expected);
    }

    #[test]
    fn chunks_are_one_mebibyte() {
        let mut data = vec![7u8; CHUNK_BYTES];
        data.push(9);
        let mut concatenated = Vec::new();
        concatenated.extend_from_slice(&Sha256::digest(&data[..CHUNK_BYTES]));
        concatenated.extend_from_slice(&Sha256::digest(&data[CHUNK_BYTES..]));
        let expected: [u8; 32] = Sha256::digest(&concatenated).into();
        assert_eq!(extension_hash(&data), expected);
    }

    #[test]
    fn only_moraine_builds_are_signed_and_keep_their_name() {
        assert_eq!(
            signed_asset_name("moraine.v2.0.0.osx_arm64.duckdb_extension").as_deref(),
            Some("moraine.v2.0.0.osx_arm64.duckdb_extension.gz")
        );
        assert_eq!(
            signed_asset_name("moraine.v2.0.0.osx_arm64.duckdb_extension.gz"),
            None
        );
        assert_eq!(signed_asset_name("notes.txt"), None);
    }

    #[test]
    fn a_signed_build_verifies_with_the_public_key_and_refuses_a_second_signing() {
        let work = std::env::temp_dir().join(format!("moraine-sign-test-{}", std::process::id()));
        let dist = work.join("dist");
        let signed = work.join("signed");
        fs::create_dir_all(&dist).unwrap();
        let key = work.join("key.pem");
        assert!(
            Command::new("openssl")
                .args(["genrsa", "-out"])
                .arg(&key)
                .arg("2048")
                .output()
                .unwrap()
                .status
                .success()
        );

        let mut build = (0..=255u8)
            .cycle()
            .take(3 * CHUNK_BYTES / 2)
            .collect::<Vec<_>>();
        build.extend(std::iter::repeat_n(0u8, SIGNATURE_BYTES));
        let build_path = dist.join("moraine.v2.0.0.osx_arm64.duckdb_extension");
        fs::write(&build_path, &build).unwrap();
        fs::write(dist.join("README.txt"), b"not a build").unwrap();

        sign_release_assets(&[
            dist.to_string_lossy().into_owned(),
            signed.to_string_lossy().into_owned(),
            key.to_string_lossy().into_owned(),
        ])
        .unwrap();

        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(
            fs::File::open(signed.join("moraine.v2.0.0.osx_arm64.duckdb_extension.gz")).unwrap(),
        )
        .read_to_end(&mut decoded)
        .unwrap();
        assert_eq!(decoded.len(), build.len());
        let (body, signature) = decoded.split_at(decoded.len() - SIGNATURE_BYTES);
        assert_eq!(body, &build[..build.len() - SIGNATURE_BYTES]);
        assert!(signature.iter().any(|byte| *byte != 0));

        let hash_path = work.join("hash");
        let signature_path = work.join("signature");
        fs::write(&hash_path, extension_hash(body)).unwrap();
        fs::write(&signature_path, signature).unwrap();
        let verified = Command::new("openssl")
            .args([
                "pkeyutl",
                "-verify",
                "-pubin",
                "-pkeyopt",
                "digest:sha256",
                "-in",
            ])
            .arg(&hash_path)
            .arg("-inkey")
            .arg(work.join("key.pem.pub"))
            .arg("-sigfile")
            .arg(&signature_path)
            .output()
            .unwrap();
        assert!(
            verified.status.success(),
            "{}",
            String::from_utf8_lossy(&verified.stderr)
        );

        fs::write(&build_path, &decoded).unwrap();
        let again = sign_release_assets(&[
            dist.to_string_lossy().into_owned(),
            signed.to_string_lossy().into_owned(),
            key.to_string_lossy().into_owned(),
        ]);
        assert!(
            again
                .unwrap_err()
                .to_string()
                .contains("already carries a signature")
        );
        fs::remove_dir_all(&work).unwrap();
    }
}
