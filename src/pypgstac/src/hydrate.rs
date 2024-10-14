use anyhow::anyhow;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyList};

const MAGIC_MARKER: &str = "𒍟※";

#[pyfunction]
pub(crate) fn hydrate<'py>(
    base: &'py Bound<'py, PyAny>,
    item: &'py Bound<'py, PyAny>,
) -> PyResult<&'py Bound<'py, PyAny>> {
    hydrate_any(base, item)
}

fn hydrate_any<'py>(
    base: &'py Bound<'py, PyAny>,
    item: &'py Bound<'py, PyAny>,
) -> PyResult<&'py Bound<'py, PyAny>> {
    if let Ok(item) = item.downcast::<PyDict>() {
        if let Ok(base) = base.downcast::<PyDict>() {
            Ok(hydrate_dict(base, item)?.as_any())
        } else if base.is_none() {
            Ok(item)
        } else {
            Err(anyhow!("type mismatch").into())
        }
    } else if let Ok(item) = item.downcast::<PyList>() {
        if let Ok(base) = base.downcast::<PyList>() {
            Ok(hydrate_list(base, item)?.as_any())
        } else if base.is_none() {
            Ok(item)
        } else {
            Err(anyhow!("type mismatch").into())
        }
    } else {
        Ok(item)
    }
}

fn hydrate_list<'py>(
    base: &'py Bound<'py, PyList>,
    item: &'py Bound<'py, PyList>,
) -> PyResult<&'py Bound<'py, PyList>> {
    for i in 0..item.len() {
        if i >= base.len() {
            return Ok(item);
        } else {
            item.set_item(i, hydrate(&base.get_item(i)?, &item.get_item(i)?)?)?;
        }
    }
    Ok(item)
}

fn hydrate_dict<'py>(
    base: &'py Bound<'py, PyDict>,
    item: &'py Bound<'py, PyDict>,
) -> PyResult<&'py Bound<'py, PyDict>> {
    for (key, base_value) in base {
        if let Some(item_value) = item.get_item(&key)? {
            if item_value
                .extract::<PyBackedStr>()
                .ok()
                .map(|s| <PyBackedStr as AsRef<str>>::as_ref(&s) == MAGIC_MARKER)
                .unwrap_or(false)
            {
                item.del_item(key)?;
            } else {
                item.set_item(&key, hydrate(&base_value, &item_value)?)?;
            }
        } else {
            item.set_item(key, base_value)?;
        }
    }
    Ok(item)
}
