#![deny(
    elided_lifetimes_in_paths,
    explicit_outlives_requirements,
    keyword_idents,
    macro_use_extern_crate,
    meta_variable_misuse,
    missing_abi,
    missing_debug_implementations,
    non_ascii_idents,
    noop_method_call,
    pointer_structural_match,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications,
    unused_results
)]

use pyo3::prelude::*;

mod hydrate;
mod schema;

#[pymodule]
fn pgstacrs(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(crate::hydrate::hydrate, m)?)?;

    m.add_function(wrap_pyfunction!(crate::schema::arrow_schema_to_json, m)?)?;
    m.add_function(wrap_pyfunction!(crate::schema::json_to_arrow_schema, m)?)?;

    Ok(())
}
