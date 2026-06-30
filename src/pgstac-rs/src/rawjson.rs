//! Reading a `jsonb` column as raw bytes (no parse) for the byte-assembly output path.
//!
//! Parsing `assets`/`bbox` jsonb into a [`serde_json::Value`] is the dominant per-item cost for rich
//! items (the big `classification:bitfields` arrays) and it loses PostgreSQL `numeric` precision on
//! `bbox` (f64 round-trip). [`RawJson`] keeps the original JSON text, so the byte path can splice it in
//! verbatim and merge only where it must (see [`crate::feature`]).

use serde_json::Value;
use serde_json::value::RawValue;
use std::error::Error;
use tokio_postgres::types::{FromSql, Type};

/// The raw JSON text of a `jsonb` value, kept unparsed.
#[derive(Debug, Clone)]
pub struct RawJson(pub Box<RawValue>);

impl RawJson {
    /// The raw JSON text.
    pub fn get(&self) -> &str {
        self.0.get()
    }

    /// Parses the raw text into a [`Value`] (jsonb is always valid JSON; `Null` on the impossible
    /// failure).
    pub fn to_value(&self) -> Value {
        serde_json::from_str(self.0.get()).unwrap_or(Value::Null)
    }

    /// Builds a [`RawJson`] from a [`Value`] — used by tests and the `from_value` round trip.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgstac::rawjson::RawJson;
    /// use serde_json::json;
    ///
    /// let raw = RawJson::from_value(&json!({"a": 1}));
    /// assert_eq!(raw.to_value(), json!({"a": 1}));
    /// ```
    pub fn from_value(value: &Value) -> RawJson {
        RawJson(RawValue::from_string(value.to_string()).expect("a Value serializes to valid JSON"))
    }
}

impl<'a> FromSql<'a> for RawJson {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<RawJson, Box<dyn Error + Sync + Send>> {
        // jsonb wire format: a 1-byte version header (currently `1`) followed by the UTF-8 JSON text.
        // `json` has no header. Strip a leading version byte only for jsonb.
        let text = match raw.first() {
            Some(1) if raw.len() > 1 => std::str::from_utf8(&raw[1..])?,
            _ => std::str::from_utf8(raw)?,
        };
        Ok(RawJson(RawValue::from_string(text.to_string())?))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSONB | Type::JSON)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips_through_value() {
        let raw = RawJson::from_value(&json!({"b": [1, 2, 3], "a": "x"}));
        assert_eq!(raw.to_value(), json!({"b": [1, 2, 3], "a": "x"}));
    }

    #[test]
    fn preserves_high_precision_number_text() {
        // A number beyond f64 precision survives verbatim (the bbox-precision motivation).
        let raw = RawJson(RawValue::from_string("[-22.650799880000001]".to_string()).unwrap());
        assert_eq!(raw.get(), "[-22.650799880000001]");
    }
}
