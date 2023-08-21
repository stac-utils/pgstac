use serde_json::{Map, Value};
use thiserror::Error;
use pyo3::{create_exception, exceptions::PyException, prelude::*};

const MAGIC_MARKER: &str = "𒍟※";

create_exception!(pgstacrs, HydrationError, PyException);

pub trait Hydrate {
    fn hydrate(&mut self, base: Self) -> Result<(), Error>;
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("type mismatch")]
    TypeMismatch(Value, Value),
}

impl Hydrate for Value {
    fn hydrate(&mut self, base: Self) -> Result<(), Error> {
        match self {
            Value::Object(item) => match base {
                Value::Object(base) => item.hydrate(base),
                _ => Err(Error::TypeMismatch(self.clone(), base)),
            },
            Value::Array(item) => match base {
                Value::Array(base) => item.hydrate(base),
                _ => Err(Error::TypeMismatch(self.clone(), base)),
            },
            _ => Ok(()),
        }
    }
}

impl Hydrate for Vec<Value> {
    fn hydrate(&mut self, base: Self) -> Result<(), Error> {
        for (item, base) in self.iter_mut().zip(base.into_iter()) {
            item.hydrate(base)?;
        }
        Ok(())
    }
}

impl Hydrate for Map<String, Value> {
    fn hydrate(&mut self, base: Self) -> Result<(), Error> {
        for (key, base_value) in base {
            if self
                .get(&key)
                .and_then(|value| value.as_str())
                .map(|s| s == MAGIC_MARKER)
                .unwrap_or(false)
            {
                self.remove(&key);
            } else if let Some(self_value) = self.get_mut(&key) {
                self_value.hydrate(base_value)?;
            } else {
                self.insert(key, base_value);
            }
        }
        Ok(())
    }
}

struct HydrateError(Error);

impl From<HydrateError> for PyErr {
    fn from(value: HydrateError) -> Self {
        HydrationError::new_err(value.0.to_string())
    }
}
#[pyfunction]
fn hydrate(base: &PyAny, item: &PyAny) -> PyResult<Py<PyAny>> {
    let mut serde_item: Value = pythonize::depythonize(item)?;
    let serde_base: Value = pythonize::depythonize(base)?;
    serde_item.hydrate(serde_base).map_err(HydrateError)?;
    pythonize::pythonize(item.py(), &serde_item).map_err(PyErr::from)
}

#[pymodule]
fn pgstacrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hydrate, m)?)?;
    Ok(())
}
