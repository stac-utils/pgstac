//! Canonical JSON encoding and content hash matching pgstac's SQL `jsonb_canonical` / `jsonb_hash`.
//!
//! pgstac stores an item's `item_hash` as `jsonb_hash(content) = sha256(jsonb_canonical(content))`
//! (see `001a_jsonutils.sql`). The Rust ingest path dehydrates items client-side, so it must compute a
//! **byte-identical** hash — an item ingested via Rust and the same item ingested via the SQL path must
//! get the same `item_hash`. This is parity-gated against the database in `tests/canonical_parity.rs`.
//!
//! The canonical form is deliberately **not** RFC 8785:
//! - object keys are sorted by raw **byte order** (the SQL uses `ORDER BY key COLLATE "C"`),
//! - numbers render as PostgreSQL `float8::text` (every number is cast through `float8`),
//! - strings, `true`/`false`/`null` use their JSON text form.
//!
//! Everything here borrows its input ([`&Value`](serde_json::Value)) — nothing is cloned.

use serde_json::{Number, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// The 32-byte SHA-256 of the canonical encoding of `value` — equal to SQL `jsonb_hash(value)`.
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// let h = pgstac::canonical::jsonb_hash(&json!({"b": 1, "a": 2}));
/// assert_eq!(h.len(), 32);
/// ```
pub fn jsonb_hash(value: &Value) -> [u8; 32] {
    let mut buf = String::new();
    write_canonical(&mut buf, value);
    Sha256::digest(buf.as_bytes()).into()
}

/// The canonical text of `value` — equal to SQL `jsonb_canonical(value)`.
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// assert_eq!(pgstac::canonical::jsonb_canonical(&json!({"b": 1, "a": 2})), r#"{"a":2,"b":1}"#);
/// ```
pub fn jsonb_canonical(value: &Value) -> String {
    let mut buf = String::new();
    write_canonical(&mut buf, value);
    buf
}

/// Appends the canonical encoding of `value` to `out`. Recursive; borrows `value`.
fn write_canonical(out: &mut String, value: &Value) {
    match value {
        Value::Object(map) => {
            out.push('{');
            // Sort keys by raw UTF-8 byte order to match `ORDER BY key COLLATE "C"`.
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(out, key);
                out.push(':');
                write_canonical(out, &map[*key]);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(out, item);
            }
            out.push(']');
        }
        Value::Number(n) => write_pg_float8(out, n),
        Value::String(s) => write_json_string(out, s),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Null => out.push_str("null"),
    }
}

/// Appends a JSON-escaped, double-quoted string **directly into `out`** (no temporary allocation).
/// Matches PostgreSQL's `to_json(text)` / jsonb string `::text` escaping, which is exactly what
/// `serde_json` produces: escape `"`, `\`, the short control escapes (`\b\t\n\f\r`), other control
/// chars (< 0x20) as `\u00xx` (lowercase), and pass everything else (incl. `/` and all non-ASCII)
/// through unchanged. Parity-checked against `serde_json::to_string` in the tests.
fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let escape: &str = match b {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            0x08 => "\\b",
            0x09 => "\\t",
            0x0A => "\\n",
            0x0C => "\\f",
            0x0D => "\\r",
            0x00..=0x1F => {
                // Other control char: \u00xx. The run before it is ASCII-safe to slice.
                out.push_str(&s[start..i]);
                let _ = write!(out, "\\u{b:04x}");
                start = i + 1;
                continue;
            }
            _ => continue, // unescaped byte (ASCII or a UTF-8 continuation/lead byte)
        };
        out.push_str(&s[start..i]);
        out.push_str(escape);
        start = i + 1;
    }
    out.push_str(&s[start..]);
    out.push('"');
}

/// Appends a JSON number as PostgreSQL `float8::text`. SQL `jsonb_canonical` renders every number as
/// `(j #>> '{}')::float8::text`, so the number is cast through `float8` and printed with PostgreSQL's
/// shortest round-trip formatting (fixed-point in the usual range, scientific notation outside it).
fn write_pg_float8(out: &mut String, n: &Number) {
    let Some(f) = n.as_f64() else {
        // A JSON number is always f64-representable via serde_json; defensively emit the raw text.
        out.push_str(&n.to_string());
        return;
    };
    write_float8(out, f);
}

/// [`format_float8`] writing directly into `out` (the hot path; no result-String allocation).
fn write_float8(out: &mut String, f: f64) {
    if f == 0.0 {
        // jsonb numbers pass through `numeric`, which has no signed zero, so -0.0 becomes "0".
        out.push('0');
        return;
    }
    if f.is_nan() {
        out.push_str("NaN");
        return;
    }
    if f.is_infinite() {
        out.push_str(if f < 0.0 { "-Infinity" } else { "Infinity" });
        return;
    }

    let negative = f < 0.0;
    // Rust's `{:e}` yields the shortest round-trip mantissa + base-10 exponent.
    let sci = format!("{:e}", f.abs());
    let (mantissa, exp_str) = sci
        .split_once('e')
        .expect("scientific notation contains 'e'");
    let decexp: i32 = exp_str.parse().expect("valid base-10 exponent");
    let digits: String = mantissa.chars().filter(|&c| c != '.').collect();
    let ndigits = digits.len() as i32;

    if negative {
        out.push('-');
    }

    // Fixed point for -4 <= decexp < 15; scientific outside that range (matches PostgreSQL).
    if !(-4..15).contains(&decexp) {
        out.push_str(&digits[..1]);
        if ndigits > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if decexp >= 0 { '+' } else { '-' });
        let abs_exp = decexp.unsigned_abs();
        if abs_exp < 10 {
            out.push('0');
        }
        let _ = write!(out, "{abs_exp}");
    } else if decexp >= 0 {
        let int_digits = decexp + 1;
        if ndigits <= int_digits {
            out.push_str(&digits);
            for _ in 0..(int_digits - ndigits) {
                out.push('0');
            }
        } else {
            out.push_str(&digits[..int_digits as usize]);
            out.push('.');
            out.push_str(&digits[int_digits as usize..]);
        }
    } else {
        out.push_str("0.");
        for _ in 0..(-decexp - 1) {
            out.push('0');
        }
        out.push_str(&digits);
    }
}

/// Formats `f` exactly as PostgreSQL `float8out` does (shortest round-trip, `%g`-style fixed/scientific
/// selection). Validated against the database in `tests/canonical_parity.rs`. Thin wrapper over
/// [`write_float8`] (the hot path writes into a shared buffer; this is for tests wanting a `String`).
#[cfg(test)]
fn format_float8(f: f64) -> String {
    let mut out = String::new();
    write_float8(&mut out, f);
    out
}

#[cfg(test)]
mod tests {
    use super::{format_float8, jsonb_canonical, write_json_string};
    use serde_json::json;

    #[test]
    fn json_string_escaping_matches_serde() {
        let cases = [
            "",
            "plain",
            "quote\"and\\backslash",
            "tab\tnewline\nreturn\r",
            "controls\u{0}\u{1f}\u{8}\u{c}",
            "slash/stays",
            "unicode é 😀 \u{7f}",
        ];
        for s in cases {
            let mut out = String::new();
            write_json_string(&mut out, s);
            assert_eq!(
                out,
                serde_json::to_string(s).unwrap(),
                "escaping mismatch for {s:?}"
            );
        }
    }

    #[test]
    fn canonical_sorts_object_keys_by_byte_order() {
        assert_eq!(
            jsonb_canonical(&json!({"b": 1, "a": 2, "Z": 3})),
            r#"{"Z":3,"a":2,"b":1}"#
        );
    }

    #[test]
    fn canonical_arrays_keep_order() {
        assert_eq!(jsonb_canonical(&json!([3, 1, 2])), "[3,1,2]");
    }

    #[test]
    fn float8_fixed_and_scientific() {
        assert_eq!(format_float8(0.0), "0");
        assert_eq!(format_float8(-0.0), "0");
        assert_eq!(format_float8(1.0), "1");
        assert_eq!(format_float8(-1.0), "-1");
        assert_eq!(format_float8(100.0), "100");
        assert_eq!(format_float8(0.1), "0.1");
        assert_eq!(format_float8(123.456), "123.456");
        assert_eq!(format_float8(0.0001), "0.0001");
        assert_eq!(format_float8(0.00001), "1e-05");
        assert_eq!(format_float8(1e14), "100000000000000");
        assert_eq!(format_float8(1e15), "1e+15");
        assert_eq!(format_float8(1e16), "1e+16");
        assert_eq!(format_float8(123456789012345.6), "123456789012345.6");
        assert_eq!(format_float8(-105.1019), "-105.1019");
    }
}
