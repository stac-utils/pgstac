use ::pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use anyhow::{anyhow, Error};

const MAGIC_MARKER: &str = "𒍟※";

#[pyfunction]
fn hydrate<'a>(base: &PyAny, item: &'a PyAny) -> PyResult<&'a PyAny> {
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

    hydrate_any(base, item)
}

#[pymodule]
fn pgstacrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(crate::hydrate, m)?)?;
    Ok(())
}
