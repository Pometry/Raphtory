use crate::core::Prop;
use crate::db::api::properties::internal::{
    TemporalProperties, TemporalPropertiesOps, TemporalPropertyView, TemporalPropertyViewOps,
};
use crate::db::api::view::internal::{DynamicGraph, IntoDynamic};
use crate::db::graph::vertex::VertexView;
use crate::prelude::GraphViewOps;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use std::ops::Deref;
use std::sync::Arc;

impl<T: Deref> TemporalPropertyViewOps<String> for T
where
    T::Target: TemporalPropertyViewOps<String>,
{
    fn temporal_value(&self, id: &String) -> Option<Prop> {
        self.deref().temporal_value(id)
    }

    fn temporal_history(&self, id: &String) -> Vec<i64> {
        self.deref().temporal_history(id)
    }

    fn temporal_values(&self, id: &String) -> Vec<Prop> {
        self.deref().temporal_values(id)
    }

    fn temporal_value_at(&self, id: &String, t: i64) -> Option<Prop> {
        self.deref().temporal_value_at(id, t)
    }
}

impl<T: Deref> TemporalPropertiesOps for T
where
    T::Target: TemporalPropertiesOps<String>,
{
    fn temporal_property_keys(&self) -> Vec<String> {
        self.deref().temporal_property_keys()
    }

    fn temporal_properties(&self) -> Box<dyn Iterator<Item = String> + '_> {
        self.deref().temporal_properties()
    }

    fn temporal_property(&self, key: &str) -> Option<String> {
        self.deref().temporal_property(key)
    }
}

#[pyclass(name = "Properties")]
pub struct PyProperties {
    props: TemporalProperties<Arc<dyn TemporalPropertiesOps + Send + Sync>>,
}

#[pymethods]
impl PyProperties {
    fn keys(&self) -> Vec<String> {
        self.props.keys()
    }
    fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        let v = self
            .props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))?;
        Ok(v.value().unwrap())
    }
}

impl<P: TemporalPropertiesOps + Clone + Send + Sync + 'static> IntoPy<PyObject>
    for TemporalProperties<P>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        let v = self.props;
        PyProperties {
            props: TemporalProperties::new(Arc::new(v)),
        }
        .into_py(py)
    }
}
