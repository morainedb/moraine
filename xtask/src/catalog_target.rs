//! Where a CLI-driven benchmark keeps its Moraine catalog: an S3 bucket
//! named by the environment, or a local directory when none is.

use std::{
    env,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;

/// An S3 bucket and prefix, with the endpoint and region the attach uses.
pub struct S3Target {
    bucket: String,
    prefix: String,
    endpoint: Option<String>,
    region: String,
}

/// The store a benchmark attaches: S3 when `MORAINE_S3_BUCKET` is set,
/// else a directory under `root`.
pub enum CatalogTarget {
    S3(S3Target),
    Local(PathBuf),
}

impl CatalogTarget {
    /// Reads `MORAINE_S3_BUCKET`, `MORAINE_S3_PREFIX`, `MORAINE_S3_ENDPOINT`,
    /// and the AWS region from the environment; without a bucket, keeps the
    /// catalog under `local_root`.
    pub fn from_environment(local_root: PathBuf) -> Self {
        match env::var("MORAINE_S3_BUCKET") {
            Ok(bucket) => Self::S3(S3Target {
                bucket,
                prefix: env::var("MORAINE_S3_PREFIX").unwrap_or_default(),
                endpoint: env::var("MORAINE_S3_ENDPOINT").ok(),
                region: env::var("AWS_REGION")
                    .or_else(|_| env::var("AWS_DEFAULT_REGION"))
                    .unwrap_or_else(|_| "us-east-1".to_owned()),
            }),
            Err(_) => Self::Local(local_root),
        }
    }

    /// As [`from_environment`](Self::from_environment), refusing to run
    /// without a bucket.
    pub fn from_environment_s3_only() -> anyhow::Result<Self> {
        match Self::from_environment(PathBuf::new()) {
            Self::S3(target) => Ok(Self::S3(target)),
            Self::Local(_) => anyhow::bail!("MORAINE_S3_BUCKET must be set"),
        }
    }

    pub fn description(&self) -> String {
        match self {
            Self::S3(target) => target
                .endpoint
                .clone()
                .unwrap_or_else(|| format!("AWS S3 in {}", target.region)),
            Self::Local(root) => format!("local directory {}", root.display()),
        }
    }

    /// Whether the attach needs `httpfs` loaded.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::S3(_))
    }

    /// The extensions an attach needs ahead of its secret: `httpfs` for
    /// any S3 store, and `aws` for the credential chain. DuckDB would
    /// autoload `aws` itself, but autoload resolves against the build's
    /// own extension repository, which carries only what this repository
    /// builds.
    pub fn extensions_sql(&self) -> String {
        let Self::S3(target) = self else {
            return String::new();
        };

        let mut sql = "INSTALL httpfs;\nLOAD httpfs;\n".to_owned();
        if target.endpoint.is_none() {
            sql.push_str("INSTALL aws;\nLOAD aws;\n");
        }
        sql
    }

    /// The `CREATE SECRET` the attach needs, if any.
    pub fn secret_sql(&self) -> anyhow::Result<Option<String>> {
        let Self::S3(target) = self else {
            return Ok(None);
        };
        let region = sql_literal(&target.region);
        let Some(endpoint) = &target.endpoint else {
            // The default chain consults the profile file first and fails
            // where there is none, so a machine whose only credentials are
            // an instance profile names `instance` here.
            let chain = env::var("MORAINE_S3_CREDENTIAL_CHAIN")
                .map(|chain| format!(", CHAIN {}", sql_literal(&chain)))
                .unwrap_or_default();
            return Ok(Some(format!(
                "CREATE SECRET moraine_bench (TYPE s3, PROVIDER credential_chain, \
                 REGION {region}{chain});"
            )));
        };

        let key = env::var("AWS_ACCESS_KEY_ID")
            .context("AWS_ACCESS_KEY_ID must be set for an explicit S3 endpoint")?;
        let secret = env::var("AWS_SECRET_ACCESS_KEY")
            .context("AWS_SECRET_ACCESS_KEY must be set for an explicit S3 endpoint")?;
        let use_ssl = endpoint.starts_with("https://");
        let token = env::var("AWS_SESSION_TOKEN")
            .ok()
            .map(|token| format!(", SESSION_TOKEN {}", sql_literal(&token)))
            .unwrap_or_default();
        Ok(Some(format!(
            "CREATE SECRET moraine_bench (TYPE s3, KEY_ID {}, SECRET {}, REGION {region}, \
             ENDPOINT {}, URL_STYLE 'path', USE_SSL {use_ssl}{token});",
            sql_literal(&key),
            sql_literal(&secret),
            sql_literal(endpoint),
        )))
    }

    /// A fresh catalog location for one run, unique by process and clock.
    pub fn catalog_uri(&self, leaf: &str) -> anyhow::Result<String> {
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis();
        let leaf = format!("{leaf}-{}-{epoch}", std::process::id());
        Ok(match self {
            Self::S3(target) => {
                let prefix = target.prefix.trim_matches('/');
                let path = if prefix.is_empty() {
                    leaf
                } else {
                    format!("{prefix}/{leaf}")
                };
                format!("s3://{}/{path}", target.bucket)
            }
            Self::Local(root) => root.join(leaf).display().to_string(),
        })
    }

    /// The `host:port` a round-trip probe should dial for this store, or
    /// `None` for a local directory. A directory bucket carries its zone in
    /// its name, which is what distinguishes the S3 Express host from S3's.
    pub fn probe_target(&self) -> Option<String> {
        let Self::S3(target) = self else {
            return None;
        };

        if let Some(endpoint) = &target.endpoint {
            let (scheme, authority) = endpoint
                .split_once("://")
                .unwrap_or(("https", endpoint.as_str()));
            let authority = authority.trim_end_matches('/');
            if authority.contains(':') {
                return Some(authority.to_owned());
            }
            let port = if scheme == "http" { 80 } else { 443 };
            return Some(format!("{authority}:{port}"));
        }

        let host = match express_zone(&target.bucket) {
            Some(zone) => format!("s3express-{zone}.{}.amazonaws.com", target.region),
            None => format!("s3.{}.amazonaws.com", target.region),
        };
        Some(format!("{host}:443"))
    }
}

/// The zone in a directory bucket's name, as `use1-az4` in
/// `bench--use1-az4--x-s3`.
fn express_zone(bucket: &str) -> Option<&str> {
    bucket
        .strip_suffix("--x-s3")?
        .rsplit_once("--")
        .map(|(_, zone)| zone)
}

/// `value` as a single-quoted SQL string literal.
pub fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_local_target_keeps_the_catalog_under_its_root() {
        let target = CatalogTarget::Local(PathBuf::from("/tmp/bench"));
        let uri = target.catalog_uri("reader").unwrap();
        assert!(uri.starts_with("/tmp/bench/reader-"), "{uri}");
        assert!(!target.is_remote());
        assert!(target.secret_sql().unwrap().is_none());
    }

    #[test]
    fn only_the_credential_chain_pulls_in_the_aws_extension() {
        let chain = CatalogTarget::S3(S3Target {
            bucket: "bench".to_owned(),
            prefix: String::new(),
            endpoint: None,
            region: "us-east-1".to_owned(),
        });
        assert_eq!(
            chain.extensions_sql(),
            "INSTALL httpfs;\nLOAD httpfs;\nINSTALL aws;\nLOAD aws;\n"
        );

        // An explicit endpoint carries its own keys, so nothing resolves
        // a credential chain and `aws` is not needed.
        let explicit = CatalogTarget::S3(S3Target {
            bucket: "bench".to_owned(),
            prefix: String::new(),
            endpoint: Some("http://127.0.0.1:9124".to_owned()),
            region: "us-east-1".to_owned(),
        });
        assert_eq!(explicit.extensions_sql(), "INSTALL httpfs;\nLOAD httpfs;\n");

        assert!(
            CatalogTarget::Local(PathBuf::from("/tmp"))
                .extensions_sql()
                .is_empty()
        );
    }

    #[test]
    fn a_probe_dials_the_express_host_for_a_directory_bucket() {
        let express = CatalogTarget::S3(S3Target {
            bucket: "bench-1a2b--use1-az4--x-s3".to_owned(),
            prefix: String::new(),
            endpoint: None,
            region: "us-east-1".to_owned(),
        });
        assert_eq!(
            express.probe_target(),
            Some("s3express-use1-az4.us-east-1.amazonaws.com:443".to_owned())
        );

        let standard = CatalogTarget::S3(S3Target {
            bucket: "bench-1a2b".to_owned(),
            prefix: String::new(),
            endpoint: None,
            region: "eu-west-2".to_owned(),
        });
        assert_eq!(
            standard.probe_target(),
            Some("s3.eu-west-2.amazonaws.com:443".to_owned())
        );
    }

    #[test]
    fn a_probe_takes_an_explicit_endpoints_own_host_and_port() {
        let with_port = CatalogTarget::S3(S3Target {
            bucket: "bench".to_owned(),
            prefix: String::new(),
            endpoint: Some("http://127.0.0.1:9124".to_owned()),
            region: "us-east-1".to_owned(),
        });
        assert_eq!(with_port.probe_target(), Some("127.0.0.1:9124".to_owned()));

        let without_port = CatalogTarget::S3(S3Target {
            bucket: "bench".to_owned(),
            prefix: String::new(),
            endpoint: Some("http://store.internal".to_owned()),
            region: "us-east-1".to_owned(),
        });
        assert_eq!(
            without_port.probe_target(),
            Some("store.internal:80".to_owned())
        );

        assert_eq!(
            CatalogTarget::Local(PathBuf::from("/tmp")).probe_target(),
            None
        );
    }

    #[test]
    fn an_s3_target_joins_bucket_prefix_and_leaf() {
        let target = CatalogTarget::S3(S3Target {
            bucket: "b".into(),
            prefix: "/p/".into(),
            endpoint: None,
            region: "us-west-2".into(),
        });
        let uri = target.catalog_uri("reader").unwrap();
        assert!(uri.starts_with("s3://b/p/reader-"), "{uri}");
        assert!(target.is_remote());
        assert_eq!(target.description(), "AWS S3 in us-west-2");
    }

    #[test]
    fn literals_escape_quotes() {
        assert_eq!(sql_literal("it's"), "'it''s'");
    }
}
