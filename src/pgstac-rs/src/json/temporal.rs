//! Rendering the raw `timestamptz` columns to STAC datetime text, client-side.
//!
//! pgstac's `tstz_to_stac_text` formats `YYYY-MM-DDTHH:MM:SS[.ffffff]Z` in UTC, trimming trailing
//! fractional zeros (and the dot when the fraction is all zeros). `search_plan`'s projection now hands
//! Rust the raw `timestamptz`, so we reproduce that rendering here instead of asking the server.

use chrono::{DateTime, Utc};

/// Formats a `timestamptz` exactly like pgstac's `tstz_to_stac_text`.
///
/// # Examples
///
/// ```
/// use chrono::{TimeZone, Utc};
/// use pgstac::temporal::tstz_to_stac_text;
///
/// let dt = Utc.with_ymd_and_hms(2013, 4, 19, 12, 34, 56).unwrap();
/// assert_eq!(tstz_to_stac_text(dt), "2013-04-19T12:34:56Z");
/// ```
pub fn tstz_to_stac_text(value: DateTime<Utc>) -> String {
    // `%.6f` always emits 6 fractional digits; trim trailing zeros and a bare dot to match
    // `trim(trailing '.' from trim(trailing '0' from to_char(..., 'US')))`.
    let base = value.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
    let trimmed = base.trim_end_matches('0').trim_end_matches('.');
    format!("{trimmed}Z")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn whole_second_drops_fraction() {
        let dt = Utc.with_ymd_and_hms(2013, 4, 19, 0, 0, 0).unwrap();
        assert_eq!(tstz_to_stac_text(dt), "2013-04-19T00:00:00Z");
    }

    #[test]
    fn trailing_zeros_trimmed() {
        // .120000 -> .12
        let dt = Utc
            .with_ymd_and_hms(2013, 4, 19, 12, 34, 56)
            .unwrap()
            .with_timezone(&Utc)
            + chrono::Duration::microseconds(120_000);
        assert_eq!(tstz_to_stac_text(dt), "2013-04-19T12:34:56.12Z");
    }

    #[test]
    fn full_microseconds_kept() {
        let dt = Utc.with_ymd_and_hms(2013, 4, 19, 12, 34, 56).unwrap()
            + chrono::Duration::microseconds(123_456);
        assert_eq!(tstz_to_stac_text(dt), "2013-04-19T12:34:56.123456Z");
    }
}
