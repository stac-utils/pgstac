//! Deterministic content hash — produces bytes identical to SQL `pgstac.jsonb_canonical_hash`
//! (parity-gated in `tests/canonical_parity.rs`), so an item hashed here and the same item hashed by the
//! SQL ingest path get the same `item_hash`.
//!
//! The document is flattened to leaf `(path, value)` rows, each rendered as `path <FS> value`, sorted by
//! path (byte order), joined by `<RS>`, and SHA-256'd. Numbers are encoded as their `float8` form and
//! RFC3339 timestamps as a canonical UTC instant, so the digest matches a value after the `float8` /
//! `timestamptz` column round-trip pgstac storage performs. Separators are the C0 control chars US/RS/FS
//! (`0x1F`/`0x1E`/`0x1D`), which do not occur in STAC JSON keys or strings.

use serde_json::{Number, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// Path segment separator (Unit Separator).
const US: char = '\u{1F}';
/// Path/value separator (Record Separator is used between leaves; this splits a leaf's path from its value).
const FS: char = '\u{1E}';
/// Between-leaf separator.
const RS: char = '\u{1D}';

/// The 32-byte SHA-256 identity digest of `value` — equal to SQL `pgstac.jsonb_canonical_hash(value)`.
///
/// Errors (loudly, never silently) if `value` contains a string shaped exactly like an RFC3339 timestamp
/// that is not a valid instant — a malformed datetime must surface, not be reinterpreted as a plain string.
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// let h = pgstac::canonical::jsonb_canonical_hash(&json!({"b": 1, "a": 2})).unwrap();
/// assert_eq!(h.len(), 32);
/// // key order is irrelevant; datetime spelling is irrelevant
/// assert_eq!(
///     pgstac::canonical::jsonb_canonical_hash(&json!({"a": 2, "b": 1})).unwrap(),
///     h,
/// );
/// ```
pub fn jsonb_canonical_hash(value: &Value) -> crate::Result<[u8; 32]> {
    // (path, encoded-value) for every leaf; borrow the input (paths are the only allocation).
    let mut leaves: Vec<(String, String)> = Vec::new();
    collect_leaves(&mut leaves, String::new(), value)?;
    // Sort by path in raw byte order (matches SQL `ORDER BY path COLLATE "C"`). Paths are unique per tree.
    leaves.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

    let mut buf = String::new();
    for (i, (path, encoded)) in leaves.iter().enumerate() {
        if i > 0 {
            buf.push(RS);
        }
        buf.push_str(path);
        buf.push(FS);
        buf.push_str(encoded);
    }
    Ok(Sha256::digest(buf.as_bytes()).into())
}

/// Walks `value`, appending a `(path, encoded-value)` row for every leaf (scalar or empty container).
/// A path step is `<US>` then `k<key>` (object member) or `i<index>` (0-based array index); the `k`/`i`
/// tags keep object key `"0"` distinct from array index 0, and `<US>` keeps nesting distinct from a key
/// that itself contains a separator character.
fn collect_leaves(
    out: &mut Vec<(String, String)>,
    path: String,
    value: &Value,
) -> crate::Result<()> {
    match value {
        Value::Object(map) if !map.is_empty() => {
            for (key, child) in map {
                let mut child_path = path.clone();
                child_path.push(US);
                child_path.push('k');
                child_path.push_str(key);
                collect_leaves(out, child_path, child)?;
            }
        }
        Value::Array(items) if !items.is_empty() => {
            for (idx, child) in items.iter().enumerate() {
                let mut child_path = path.clone();
                child_path.push(US);
                child_path.push('i');
                let _ = write!(child_path, "{idx}");
                collect_leaves(out, child_path, child)?;
            }
        }
        // Leaf: scalar, or an empty object/array.
        _ => out.push((path, encode_value(value)?)),
    }
    Ok(())
}

/// Encodes a leaf value with a 1-char type tag (matching the SQL `CASE jsonb_typeof(...)`):
/// `n`<float8> / `b`true|false / `z` (null) / `t`<canonical-instant> / `s`<raw-string> /
/// `e` (empty object) / `a` (empty array).
fn encode_value(value: &Value) -> crate::Result<String> {
    Ok(match value {
        Value::Number(n) => {
            let mut s = String::from("n");
            write_pg_float8(&mut s, n);
            s
        }
        Value::Bool(true) => "btrue".to_string(),
        Value::Bool(false) => "bfalse".to_string(),
        Value::Null => "z".to_string(),
        Value::String(s) => match canonical_stac_datetime(s)? {
            Some(instant) => format!("t{instant}"),
            None => format!("s{s}"),
        },
        // Only empty containers reach here (non-empty ones recurse in `collect_leaves`).
        Value::Object(_) => "e".to_string(),
        Value::Array(_) => "a".to_string(),
    })
}

/// Returns the canonical instant of `s` (UTC, fixed 6-digit microseconds, `Z`) when `s` is datetime-shaped,
/// `Ok(None)` when it is not (a plain string), and an **error** when it is datetime-shaped but not a valid
/// instant. The error case is deliberately loud — a malformed datetime is never silently reinterpreted as a
/// string. Mirrors the SQL string branch: the same shape prefilter and a `::timestamptz` cast pinned to
/// `SET TIMEZONE='UTC'` (matching pgstac's `to_tstz` ingest), which raises on a shaped-but-invalid value.
///
/// Forgiving like `to_tstz`: `YYYY-MM-DD`, an optional `T`/`t`/space time, an optional fractional part, and
/// an optional offset; offset-less values are assumed UTC. For ≤6 fractional digits the output is
/// byte-identical to the SQL side; >6-digit fractions are truncated here but rounded by PostgreSQL (out of
/// STAC contract) — the one known edge where the two could differ.
fn canonical_stac_datetime(s: &str) -> crate::Result<Option<String>> {
    if !is_datetime_shaped(s.as_bytes()) {
        return Ok(None); // not a datetime — a plain string, unchanged
    }
    // Rewrite to a strict RFC3339 string (uppercase `T`/`Z`, `±HH:MM` offset, `Z` when the offset is
    // absent), then parse once — the same instant `to_tstz`'s UTC-pinned `::timestamptz` cast produces.
    let parsed = chrono::DateTime::parse_from_rfc3339(&to_rfc3339_utc_assumed(s)).map_err(|e| {
        crate::Error::Dehydrate(format!(
            "timestamp-shaped item value {s:?} is not a valid instant: {e}"
        ))
    })?;
    Ok(Some(
        parsed
            .with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%S%.6fZ")
            .to_string(),
    ))
}

/// Rewrites a datetime-shaped string (per [`is_datetime_shaped`]) into a strict RFC3339 string chrono can
/// parse: date-only → midnight; a `T`/`t`/space separator → `T`; a missing offset → `Z` (UTC assumed, as
/// pgstac's UTC-pinned ingest does); a `±HHMM` offset → `±HH:MM`; a lowercase `z` → `Z`.
fn to_rfc3339_utc_assumed(s: &str) -> String {
    if s.len() == 10 {
        return format!("{s}T00:00:00Z"); // date-only → midnight UTC
    }
    // Normalize the date/time separator (byte 10) to `T`.
    let mut out = String::with_capacity(s.len() + 1);
    out.push_str(&s[..10]);
    out.push('T');
    out.push_str(&s[11..]);
    // Fix the offset on the tail (everything after the `T`).
    let tail = &out[11..];
    if tail.ends_with(['Z', 'z']) {
        out.truncate(out.len() - 1); // drop the `Z`/`z` (ASCII, 1 byte)
        out.push('Z');
        out
    } else if tail.contains(['+', '-']) {
        insert_offset_colon(&out) // `±HHMM` → `±HH:MM` (no-op if already `±HH:MM`)
    } else {
        out.push('Z'); // no offset → assume UTC
        out
    }
}

/// Cheap byte check for `^\d{4}-\d{2}-\d{2}([Tt ]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:?\d{2})?)?$` — the
/// same shape the SQL side uses to prefilter datetime strings. Date-only and offset-less forms are allowed
/// (offset-less is assumed UTC downstream); the dash anchor excludes bare years, compact dates, and the
/// volatile `now`/`today` specials that a bare `::timestamptz` cast would otherwise accept.
fn is_datetime_shaped(b: &[u8]) -> bool {
    let digit = |i: usize| b.get(i).is_some_and(u8::is_ascii_digit);
    // YYYY-MM-DD
    if b.len() < 10
        || !(digit(0) && digit(1) && digit(2) && digit(3))
        || b[4] != b'-'
        || !(digit(5) && digit(6))
        || b[7] != b'-'
        || !(digit(8) && digit(9))
    {
        return false;
    }
    if b.len() == 10 {
        return true; // date-only
    }
    // separator (T/t/space) + HH:MM:SS
    if !matches!(b[10], b'T' | b't' | b' ')
        || !(digit(11) && digit(12))
        || b.get(13) != Some(&b':')
        || !(digit(14) && digit(15))
        || b.get(16) != Some(&b':')
        || !(digit(17) && digit(18))
    {
        return false;
    }
    let mut i = 19;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return false; // '.' with no digits
        }
    }
    match b.get(i) {
        None => true, // offset-less (assumed UTC)
        Some(&b'Z') | Some(&b'z') => i + 1 == b.len(),
        Some(&b'+') | Some(&b'-') => {
            i += 1;
            if !(digit(i) && digit(i + 1)) {
                return false;
            }
            i += 2;
            if b.get(i) == Some(&b':') {
                i += 1;
            }
            digit(i) && digit(i + 1) && i + 2 == b.len()
        }
        _ => false,
    }
}

/// Inserts a colon into a trailing `±HHMM` numeric offset (`+0500` -> `+05:00`) so chrono's RFC3339 parser
/// accepts the offset-without-colon variant the SQL regex allows. Returns `s` unchanged otherwise.
fn insert_offset_colon(s: &str) -> String {
    let b = s.as_bytes();
    let n = b.len();
    if n >= 5
        && (b[n - 5] == b'+' || b[n - 5] == b'-')
        && b[n - 4].is_ascii_digit()
        && b[n - 3].is_ascii_digit()
        && b[n - 2].is_ascii_digit()
        && b[n - 1].is_ascii_digit()
    {
        format!("{}:{}", &s[..n - 2], &s[n - 2..])
    } else {
        s.to_string()
    }
}

/// Appends a JSON number as PostgreSQL `float8::text`. The canonical value for a number is
/// `(v)::float8::text`, so the number is cast through `float8` and printed with PostgreSQL's shortest
/// round-trip formatting (fixed-point in the usual range, scientific notation outside it). This is required
/// (not merely convenient): promoted numbers are stored in `float8` columns, so this is the value the
/// hydrated/reproducible form carries.
fn write_pg_float8(out: &mut String, n: &Number) {
    let Some(f) = n.as_f64() else {
        // A JSON number is always f64-representable via serde_json; defensively emit the raw text.
        out.push_str(&n.to_string());
        return;
    };
    write_float8(out, f);
}

/// `format_float8` writing directly into `out` (the hot path; no result-String allocation).
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
    use super::{
        canonical_stac_datetime, format_float8, is_datetime_shaped, jsonb_canonical_hash,
        to_rfc3339_utc_assumed,
    };
    use serde_json::json;

    fn hash(v: &serde_json::Value) -> [u8; 32] {
        jsonb_canonical_hash(v).unwrap()
    }

    #[test]
    fn key_order_and_number_scale_are_irrelevant() {
        assert_eq!(
            hash(&json!({"b": 1, "a": {"y": 1, "x": 2}})),
            hash(&json!({"a": {"x": 2, "y": 1}, "b": 1}))
        );
        // 1.0 and 1 both canonicalize to float8 "1".
        assert_eq!(hash(&json!({"n": 1.0})), hash(&json!({"n": 1})));
    }

    #[test]
    fn datetime_forgiving_spellings_are_one_instant() {
        // Every forgiving spelling of 2020-01-01T00:00:00Z hashes identically (mirrors to_tstz + UTC).
        let base = hash(&json!({"d": "2020-01-01T00:00:00Z"}));
        for form in [
            "2020-01-01",                  // date-only -> midnight UTC
            "2020-01-01 00:00:00",         // space separator, offset-less -> UTC
            "2020-01-01T00:00:00",         // T separator, offset-less -> UTC
            "2020-01-01t00:00:00z",        // lowercase t/z
            "2020-01-01T00:00:00.000000Z", // explicit zero micros
            "2020-01-01T05:00:00+05:00",   // offset respected
            "2020-01-01T05:00:00+0500",    // offset without a colon
            "2019-12-31T19:00:00-05:00",   // offset that crosses midnight
        ] {
            assert_eq!(hash(&json!({ "d": form })), base, "{form:?}");
        }
    }

    #[test]
    fn classifier_accepts_forgiving_rejects_specials() {
        for ok in [
            "2020-01-01",
            "2020-01-01 00:00:00",
            "2020-01-01T00:00:00",
            "2020-01-01T00:00:00Z",
            "2020-01-01t00:00:00z",
            "2020-01-01T05:00:00+05:00",
            "2020-01-01T05:00:00+0500",
            "2020-06-15T12:34:56.789Z",
        ] {
            assert!(
                is_datetime_shaped(ok.as_bytes()),
                "{ok:?} should be datetime-shaped"
            );
        }
        for no in [
            "2020",                  // bare year (PostgreSQL rejects it too)
            "2020-01",               // year-month
            "now",                   // volatile special
            "today",                 // volatile special
            "epoch",                 // special
            "20200101",              // compact (no dashes)
            "hello",                 // plain string
            "http://x/2020-01-01",   // not anchored at the start
            "2020-01-01T00:00",      // time without seconds
            "2020-01-01Txx:00:00",   // non-numeric time
            "2020-01-01T00:00:00+5", // truncated offset
        ] {
            assert!(
                !is_datetime_shaped(no.as_bytes()),
                "{no:?} should not be datetime-shaped"
            );
        }
    }

    #[test]
    fn to_rfc3339_normalizes_to_parseable_utc() {
        assert_eq!(to_rfc3339_utc_assumed("2020-01-01"), "2020-01-01T00:00:00Z");
        assert_eq!(
            to_rfc3339_utc_assumed("2020-01-01 12:00:00"),
            "2020-01-01T12:00:00Z"
        );
        assert_eq!(
            to_rfc3339_utc_assumed("2020-01-01T12:00:00"),
            "2020-01-01T12:00:00Z"
        );
        assert_eq!(
            to_rfc3339_utc_assumed("2020-01-01t12:00:00z"),
            "2020-01-01T12:00:00Z"
        );
        assert_eq!(
            to_rfc3339_utc_assumed("2020-01-01T12:00:00Z"),
            "2020-01-01T12:00:00Z"
        );
        assert_eq!(
            to_rfc3339_utc_assumed("2020-01-01T12:00:00+0500"),
            "2020-01-01T12:00:00+05:00"
        );
        assert_eq!(
            to_rfc3339_utc_assumed("2020-01-01T12:00:00+05:00"),
            "2020-01-01T12:00:00+05:00"
        );
        assert_eq!(
            to_rfc3339_utc_assumed("2020-01-01T12:00:00.5-05:00"),
            "2020-01-01T12:00:00.5-05:00"
        );
    }

    #[test]
    fn rejected_datetime_shaped_strings_stay_raw() {
        // "now"/"today" must NOT normalize to the current time (non-deterministic); they stay raw strings.
        assert_ne!(hash(&json!({"d": "now"})), hash(&json!({"d": "today"})));
        // a bare year stays raw, distinct from the normalized full date
        assert_ne!(
            hash(&json!({"d": "2020"})),
            hash(&json!({"d": "2020-01-01"}))
        );
    }

    #[test]
    fn canonical_stac_datetime_classifies() {
        assert_eq!(canonical_stac_datetime("hello").unwrap(), None); // not a datetime
        assert_eq!(
            canonical_stac_datetime("2020-01-01").unwrap(),
            Some("2020-01-01T00:00:00.000000Z".to_string())
        );
        assert!(canonical_stac_datetime("2020-13-45").is_err()); // shaped but invalid -> loud
    }

    #[test]
    fn paths_do_not_collide() {
        // object nesting vs a key containing '/'
        assert_ne!(hash(&json!({"a/b": 1})), hash(&json!({"a": {"b": 1}})));
        // object key "0" vs array index 0
        assert_ne!(hash(&json!({"0": 1})), hash(&json!([1])));
        // empty object vs empty array vs absent
        assert_ne!(hash(&json!({"a": {}})), hash(&json!({"a": []})));
    }

    #[test]
    fn shaped_but_invalid_timestamp_errors_loudly() {
        for bad in [
            "2020-13-45",           // month 13, day 45 (date-only)
            "2020-02-30",           // Feb 30
            "2020-13-45T99:99:99Z", // every field out of range
            "2020-01-01T25:00:00Z", // hour 25
        ] {
            assert!(
                jsonb_canonical_hash(&json!({ "d": bad })).is_err(),
                "{bad:?} should error"
            );
        }
        // a non-datetime string is fine (classification, not an error)
        assert!(jsonb_canonical_hash(&json!({"id": "item-2020"})).is_ok());
    }

    #[test]
    fn float8_fixed_and_scientific() {
        assert_eq!(format_float8(0.0), "0");
        assert_eq!(format_float8(-0.0), "0");
        assert_eq!(format_float8(1.0), "1");
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
