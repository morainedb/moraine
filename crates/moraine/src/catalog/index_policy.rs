//! DuckLake column-type policy for indexes: base-type extraction and the
//! indexability check.

use crate::error::{Error, Result};

/// The base name of a DuckLake column type — `DECIMAL` from `DECIMAL(18,3)` —
/// uppercased.
#[must_use]
pub(crate) fn ducklake_base_type(column_type: &str) -> String {
    column_type
        .split('(')
        .next()
        .unwrap_or(column_type)
        .trim()
        .to_ascii_uppercase()
}

/// Refuses a column equality indexes cannot cover faithfully: DuckDB writes
/// a 128-bit integer to Parquet as a lossy `double`.
pub(crate) fn ensure_indexable(column_name: &str, column_type: &str) -> Result<()> {
    if matches!(
        ducklake_base_type(column_type).as_str(),
        "HUGEINT" | "UHUGEINT" | "INT128" | "UINT128"
    ) {
        return Err(Error::Constraint(format!(
            "column {column_name} is {column_type}; a 128-bit integer is not indexable because \
             DuckDB stores it as a lossy double in Parquet"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_type_strips_parameters_and_uppercases() {
        assert_eq!(ducklake_base_type("DECIMAL(18,3)"), "DECIMAL");
        assert_eq!(ducklake_base_type("bigint"), "BIGINT");
        assert_eq!(ducklake_base_type("  varchar "), "VARCHAR");
    }

    #[test]
    fn hugeint_family_is_refused_everything_else_allowed() {
        for ty in ["HUGEINT", "UHUGEINT", "INT128", "UINT128"] {
            assert!(matches!(
                ensure_indexable("c", ty),
                Err(Error::Constraint(_))
            ));
        }
        for ty in ["BIGINT", "VARCHAR", "UUID", "TIMESTAMP_MS", "DECIMAL(18,3)"] {
            assert!(ensure_indexable("c", ty).is_ok(), "{ty}");
        }
    }
}
