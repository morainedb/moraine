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

    /// The `CREATE SECRET` the attach needs, if any.
    pub fn secret_sql(&self) -> anyhow::Result<Option<String>> {
        let Self::S3(target) = self else {
            return Ok(None);
        };
        let region = sql_literal(&target.region);
        let Some(endpoint) = &target.endpoint else {
            return Ok(Some(format!(
                "CREATE SECRET moraine_bench (TYPE s3, PROVIDER credential_chain, REGION {region});"
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
