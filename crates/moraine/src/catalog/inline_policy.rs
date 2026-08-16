//! DuckLake column-type policy for inlining.

use crate::{catalog::index_policy::ducklake_base_type, error::Error};

/// Refuses a column moraine cannot inline: inlined rows are Arrow IPC, which
/// has no `VARIANT` representation.
pub(crate) fn ensure_inlinable(column_name: &str, column_type: &str) -> Result<(), Error> {
    if ducklake_base_type(column_type) == "VARIANT" {
        return Err(Error::Unsupported(format!(
            "moraine: column {column_name} is {column_type}, which moraine cannot store — its \
             inline data is serialized through Arrow, and DuckDB's Arrow format has no VARIANT \
             support. Use JSON (or another type) instead."
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_is_refused_as_unsupported_everything_else_allowed() {
        let err = ensure_inlinable("v", "VARIANT").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        // The message names moraine, the type, and Arrow — the three things
        // an operator needs to act on it.
        let text = err.to_string();
        for expected in ["moraine", "VARIANT", "Arrow"] {
            assert!(text.contains(expected), "{text}");
        }
        // Case-insensitive: the type string is DuckLake's, not moraine's.
        assert!(ensure_inlinable("v", "variant").is_err());

        for ty in ["BIGINT", "JSON", "STRUCT(a VARCHAR)", "GEOMETRY", "UUID"] {
            assert!(ensure_inlinable("c", ty).is_ok(), "{ty}");
        }
    }
}
