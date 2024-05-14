use std::ffi::CString;
use std::sync::Arc;

use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::{Schema, SchemaRef};
use pyo3::exceptions::PyValueError;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyCapsule;
use pyo3::{PyAny, PyResult};

#[pyfunction]
pub(crate) fn arrow_schema_to_json(schema: PySchema) -> PyResult<String> {
    serde_json::to_string(&schema.0).map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

#[pyfunction]
pub(crate) fn json_to_arrow_schema(json_string: PyBackedStr) -> PyResult<PySchema> {
    let schema = serde_json::from_str(&json_string)
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    Ok(PySchema(Arc::new(schema)))
}

/// A wrapper around an arrow schema
#[pyclass(name = "Schema")]
pub(crate) struct PySchema(SchemaRef);

#[pymethods]
impl PySchema {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this array
    /// into a pyarrow schema, without copying memory.
    fn __arrow_c_schema__(&self) -> PyResult<PyObject> {
        let ffi_schema = FFI_ArrowSchema::try_from(self.0.as_ref())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let schema_capsule_name = CString::new("arrow_schema").unwrap();

        Python::with_gil(|py| {
            let schema_capsule = PyCapsule::new_bound(py, ffi_schema, Some(schema_capsule_name))?;
            Ok(schema_capsule.to_object(py))
        })
    }
}

impl<'a> FromPyObject<'a> for PySchema {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let schema_ptr = import_arrow_c_schema(ob)?;
        let schema =
            Schema::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self(Arc::new(schema)))
    }
}

/// Validate PyCapsule has provided name
fn validate_pycapsule_name(capsule: &PyCapsule, expected_name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if let Some(capsule_name) = capsule_name {
        let capsule_name = capsule_name.to_str()?;
        if capsule_name != expected_name {
            return Err(PyValueError::new_err(format!(
                "Expected name '{}' in PyCapsule, instead got '{}'",
                expected_name, capsule_name
            )));
        }
    } else {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    Ok(())
}

/// Import `__arrow_c_schema__` across Python boundary
pub(crate) fn import_arrow_c_schema(ob: &PyAny) -> PyResult<&FFI_ArrowSchema> {
    if !ob.hasattr("__arrow_c_schema__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_schema__",
        ));
    }

    let capsule: &PyCapsule = ob.getattr("__arrow_c_schema__")?.call0()?.downcast()?;
    validate_pycapsule_name(capsule, "arrow_schema")?;

    let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
    Ok(schema_ptr)
}
