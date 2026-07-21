//! Reading the raw `items.geometry` column (EWKB on the wire) and converting it client-side.
//!
//! pgstac stores geometry as a PostGIS `geometry`; `search_plan`'s projection selects the column
//! unwrapped (`i.geometry`), so the binary protocol hands Rust the raw EWKB bytes. We convert to
//! GeoJSON **here, only for JSON output** — Arrow/Parquet output keeps the WKB and never touches this.

use crate::Result;
use bytes::BytesMut;
use geozero::geojson::GeoJson;
use geozero::wkb::Ewkb as GeozeroEwkb;
use geozero::{CoordDimensions, ToJson, ToWkb};
use serde_json::Value;
use std::error::Error;
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};

/// Encodes a GeoJSON geometry [`Value`] to EWKB bytes with SRID 4326 — the form the `items.geometry`
/// column expects on a binary COPY. Borrows `geometry` and serializes it once (no deep clone).
///
/// # Examples
///
/// ```
/// use pgstac::geom::{geojson_to_ewkb, Ewkb};
/// use serde_json::json;
/// let ewkb = geojson_to_ewkb(&json!({"type": "Point", "coordinates": [1.0, 2.0]})).unwrap();
/// let round_trip = Ewkb(ewkb).to_geojson().unwrap();
/// assert_eq!(round_trip["type"], "Point");
/// assert_eq!(round_trip["coordinates"], json!([1, 2]));
/// ```
pub fn geojson_to_ewkb(geometry: &Value) -> Result<Vec<u8>> {
    let geojson = serde_json::to_string(geometry)?;
    Ok(GeoJson(&geojson).to_ewkb(CoordDimensions::xy(), Some(4326))?)
}

/// The EWKB bytes of an `items.geometry` value.
///
/// The PostGIS `geometry` binary wire format *is* EWKB, so one type serves both directions: it reads
/// the raw bytes off the binary protocol ([`FromSql`]) — keeping them lets the dump path forward WKB to
/// Arrow/Parquet untouched while the JSON path converts to GeoJSON via [`Ewkb::to_geojson`] — and writes
/// them back verbatim on a binary COPY ([`ToSql`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ewkb(pub Vec<u8>);

impl Ewkb {
    /// Converts the EWKB to a GeoJSON geometry [`Value`].
    ///
    /// # Examples
    ///
    /// ```
    /// use pgstac::geom::Ewkb;
    /// // EWKB for POINT(1 2) (little-endian, no SRID flag).
    /// let ewkb = Ewkb(hex_to_bytes("0101000000000000000000f03f0000000000000040"));
    /// let gj = ewkb.to_geojson().unwrap();
    /// assert_eq!(gj["type"], "Point");
    /// assert_eq!(gj["coordinates"], serde_json::json!([1, 2]));
    /// # fn hex_to_bytes(s: &str) -> Vec<u8> {
    /// #     (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    /// # }
    /// ```
    pub fn to_geojson(&self) -> Result<Value> {
        let json = GeozeroEwkb(&self.0).to_json()?;
        Ok(serde_json::from_str(&json)?)
    }
}

impl<'a> FromSql<'a> for Ewkb {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Ewkb, Box<dyn Error + Sync + Send>> {
        Ok(Ewkb(raw.to_vec()))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "geometry"
    }
}

impl ToSql for Ewkb {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "geometry"
    }

    to_sql_checked!();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn point_ewkb_to_geojson() {
        // POINT(1 2), little-endian WKB.
        let geom = Ewkb(hex("0101000000000000000000f03f0000000000000040"));
        let gj = geom.to_geojson().unwrap();
        assert_eq!(gj["type"], "Point");
        assert_eq!(gj["coordinates"], serde_json::json!([1, 2]));
    }

    #[test]
    fn ewkb_with_srid_to_geojson() {
        // SRID=4326;POINT(19 10) — EWKB with the SRID flag (0x20000000) set.
        let geom = Ewkb(hex("0101000020e610000000000000000033400000000000002440"));
        let gj = geom.to_geojson().unwrap();
        assert_eq!(gj["type"], "Point");
        assert_eq!(gj["coordinates"], serde_json::json!([19, 10]));
    }

    #[test]
    fn geojson_to_ewkb_round_trips_with_srid() {
        let geom = serde_json::json!({"type": "Point", "coordinates": [19.0, 10.0]});
        let ewkb = geojson_to_ewkb(&geom).unwrap();
        // SRID=4326 -> the EWKB encodes the SRID and round-trips to the same geometry.
        assert_eq!(
            ewkb,
            hex("0101000020e610000000000000000033400000000000002440")
        );
        let gj = Ewkb(ewkb).to_geojson().unwrap();
        assert_eq!(gj["type"], "Point");
        assert_eq!(gj["coordinates"], serde_json::json!([19, 10]));
    }
}
