//! How a lookup value coerces to a column's canonical stored form. The
//! indexability rule and base-type extraction live in the catalog's
//! `index_policy`.

use crate::{
    catalog::index_policy::ducklake_base_type,
    store::index_encoding::{IndexKeyValue, IntWidth},
};

/// A lookup value as the ABI delivers it, owned, before coercion to the
/// indexed column's canonical form.
#[derive(Debug, Clone, PartialEq)]
pub enum LookupInput {
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// A float; carries `FLOAT` lookups too, narrowed at coercion.
    Float(f64),
    /// A boolean.
    Bool(bool),
    /// A string.
    Str(String),
    /// A UUID or blob.
    Bytes(Vec<u8>),
}

/// Coerces a lookup value to the canonical [`IndexKeyValue`] for a column
/// of DuckLake type `ducklake_type`, matching how index maintenance derives
/// the stored key. Errors, as plain message text, on a column type equality
/// indexes do not cover or a value kind that cannot represent it.
// Same-bodied arms stay separate per type name; the `f64 as f32` narrowing
// for a `FLOAT` column is intended.
#[allow(clippy::match_same_arms, clippy::cast_possible_truncation)]
pub fn coerce_lookup_value(
    input: &LookupInput,
    ducklake_type: &str,
) -> std::result::Result<IndexKeyValue, String> {
    let want = |what: &str| -> String {
        format!("index lookup: expected {what} value for a `{ducklake_type}` column")
    };
    let signed_input = || -> std::result::Result<i128, String> {
        match input {
            LookupInput::Int(value) => Ok(i128::from(*value)),
            LookupInput::UInt(value) => Ok(i128::from(*value)),
            _ => Err(want("an integer")),
        }
    };
    let unsigned_input = || -> std::result::Result<u128, String> {
        match input {
            LookupInput::UInt(value) => Ok(u128::from(*value)),
            LookupInput::Int(value) if *value >= 0 => Ok(value.unsigned_abs().into()),
            _ => Err(want("an unsigned integer")),
        }
    };
    let signed = |value, width| IndexKeyValue::Int { value, width };
    let unsigned = |value, width| IndexKeyValue::UInt { value, width };

    // Names as DuckLake records them (see the shim's `MapColumnType`): the
    // bit-width spellings (`INT64`) alongside the SQL names (`BIGINT`).
    let value = match ducklake_base_type(ducklake_type).as_str() {
        "TINYINT" | "INT8" => signed(signed_input()?, IntWidth::I8),
        "SMALLINT" | "INT16" => signed(signed_input()?, IntWidth::I16),
        "INTEGER" | "INT32" => signed(signed_input()?, IntWidth::I32),
        "BIGINT" | "INT64" => signed(signed_input()?, IntWidth::I64),
        "UTINYINT" | "UINT8" => unsigned(unsigned_input()?, IntWidth::I8),
        "USMALLINT" | "UINT16" => unsigned(unsigned_input()?, IntWidth::I16),
        "UINTEGER" | "UINT32" => unsigned(unsigned_input()?, IntWidth::I32),
        "UBIGINT" | "UINT64" => unsigned(unsigned_input()?, IntWidth::I64),
        // 128-bit integers are refused at index creation, so they fall
        // through to the unsupported-type error. Temporal types index by
        // their underlying integer: `DATE` as an `i32` day count, the rest
        // as `i64`.
        "DATE" => signed(signed_input()?, IntWidth::I32),
        "TIME"
        | "TIME_NS"
        | "TIMETZ"
        | "TIME WITH TIME ZONE"
        | "TIMESTAMP"
        | "TIMESTAMP_S"
        | "TIMESTAMP_MS"
        | "TIMESTAMP_NS"
        | "TIMESTAMP_US"
        | "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE" => signed(signed_input()?, IntWidth::I64),
        "BOOLEAN" => {
            let LookupInput::Bool(value) = input else {
                return Err(want("a boolean"));
            };
            IndexKeyValue::Bool(*value)
        }
        "FLOAT" | "FLOAT32" | "REAL" => {
            let LookupInput::Float(value) = input else {
                return Err(want("a float"));
            };
            IndexKeyValue::F32(*value as f32)
        }
        "DOUBLE" | "FLOAT64" => {
            let LookupInput::Float(value) = input else {
                return Err(want("a float"));
            };
            IndexKeyValue::F64(*value)
        }
        "VARCHAR" | "TEXT" | "JSON" => {
            let LookupInput::Str(value) = input else {
                return Err(want("a string"));
            };
            IndexKeyValue::Str(value.clone())
        }
        "UUID" | "BLOB" => {
            let LookupInput::Bytes(value) = input else {
                return Err(want("a UUID or blob"));
            };
            IndexKeyValue::Bytes(value.clone())
        }
        other => {
            return Err(format!(
                "index lookup: column type `{other}` is not supported"
            ));
        }
    };
    Ok(value)
}
