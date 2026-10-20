//! Explicit bounded local Proto persistence. Decode establishes syntax only;
//! consumers must still validate identity, provenance, durability and semantics.
use prost::Message;
use pyo3::{prelude::*, types::PyBytes};
use std::sync::Arc;

pub(crate) fn export<'py, T: Message + Send + Sync + 'static>(
    py: Python<'py>,
    value: Arc<T>,
) -> PyResult<Bound<'py, PyBytes>> {
    let data = py
        .detach(move || marketcow_sdk::persistence::export(value.as_ref()).map_err(super::error))?;
    Ok(PyBytes::new(py, &data))
}
pub(crate) fn decode<T: Message + Default + Send + 'static>(
    py: Python<'_>,
    data: &Bound<'_, PyBytes>,
) -> PyResult<T> {
    let data = data.as_bytes();
    // PyBytes is immutable and kept alive by the bound argument during detach.
    py.detach(move || marketcow_sdk::persistence::restore(data).map_err(super::error))
}
