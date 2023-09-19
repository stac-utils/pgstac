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

use anyhow::anyhow;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
};

const MAGIC_MARKER: &str = "𒍟※";

#[pyfunction]
fn hydrate<'a>(base: &PyAny, item: &'a PyAny) -> PyResult<&'a PyAny> {
    hydrate_any(base, item)
}

fn hydrate_any<'a>(base: &PyAny, item: &'a PyAny) -> PyResult<&'a PyAny> {
    if let Ok(item) = item.downcast::<PyDict>() {
        if let Ok(base) = base.downcast::<PyDict>() {
            hydrate_dict(base, item).map(|item| item.into())
        } else {
            Err(anyhow!("type mismatch").into())
        }
    } else if let Ok(item) = item.downcast::<PyList>() {
        if let Ok(base) = base.downcast::<PyList>() {
            hydrate_list(base, item).map(|item| item.into())
        } else {
            Err(anyhow!("type mismatch").into())
        }
    } else {
        Ok(item)
    }
}

fn hydrate_list<'a>(base: &PyList, item: &'a PyList) -> PyResult<&'a PyList> {
    for i in 0..item.len() {
        if i >= base.len() {
            return Ok(item);
        } else {
            item.set_item(i, hydrate(&base[i], &item[i])?)?;
        }
    }
    Ok(item)
}

fn hydrate_dict<'a>(base: &PyDict, item: &'a PyDict) -> PyResult<&'a PyDict> {
    for (key, base_value) in base {
        if let Some(item_value) = item.get_item(key) {
            if item_value
                .downcast::<PyString>()
                .ok()
                .and_then(|value| value.to_str().ok())
                .map(|s| s == MAGIC_MARKER)
                .unwrap_or(false)
            {
                item.del_item(&key)?;
            } else {
                item.set_item(key, hydrate(base_value, item_value)?)?;
            }
        } else {
            item.set_item(key, base_value)?;
        }
    }
    Ok(item)
}

#[pymodule]
fn pgstacrs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(crate::hydrate, m)?)?;
    Ok(())
}
