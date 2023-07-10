use crate::core::Prop;
use crate::db::api::properties::internal::{
    StaticProperties, StaticPropertiesOps, TemporalProperties, TemporalPropertiesOps,
    TemporalPropertyView, TemporalPropertyViewOps,
};
use crate::db::api::view::internal::{DynamicGraph, IntoDynamic, Static};
use crate::db::graph::vertex::VertexView;
use crate::prelude::GraphViewOps;
use crate::python::types::repr::{iterator_dict_repr, iterator_repr, Repr};
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use std::borrow::Borrow;
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

    fn temporal_property_values(&self) -> Box<dyn Iterator<Item = String> + '_> {
        self.deref().temporal_property_values()
    }

    fn get_temporal_property(&self, key: &str) -> Option<String> {
        self.deref().get_temporal_property(key)
    }
}

impl<T: Deref> StaticPropertiesOps for T
where
    T::Target: StaticPropertiesOps,
{
    fn static_property_keys(&self) -> Vec<String> {
        self.deref().static_property_keys()
    }

    fn static_property_values(&self) -> Vec<Prop> {
        self.deref().static_property_values()
    }

    fn get_static_property(&self, key: &str) -> Option<Prop> {
        self.deref().get_static_property(key)
    }
}

pub type DynTemporalProperties = TemporalProperties<Arc<dyn TemporalPropertiesOps + Send + Sync>>;

impl<P: TemporalPropertiesOps + Clone + Send + Sync + Static + 'static> From<TemporalProperties<P>>
    for DynTemporalProperties
{
    fn from(value: TemporalProperties<P>) -> Self {
        TemporalProperties::new(Arc::new(value.props))
    }
}

impl<P: Into<DynTemporalProperties>> From<P> for PyTemporalProperties {
    fn from(value: P) -> Self {
        Self {
            props: value.into(),
        }
    }
}

#[pyclass(name = "Properties")]
pub struct PyTemporalProperties {
    props: DynTemporalProperties,
}

#[pymethods]
impl PyTemporalProperties {
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

#[pyclass(name = "Property")]
pub struct PyTemporalPropertyView {
    prop: TemporalPropertyView<Arc<dyn TemporalPropertyViewOps + Send + Sync>>,
}

pub type DynStaticProperties = StaticProperties<Arc<dyn StaticPropertiesOps + Send + Sync>>;

impl<P: StaticPropertiesOps + Send + Sync + Static + 'static> From<StaticProperties<P>>
    for DynStaticProperties
{
    fn from(value: StaticProperties<P>) -> Self {
        StaticProperties {
            props: Arc::new(value.props),
        }
    }
}

#[pyclass(name = "MetaData")]
pub struct PyStaticProperties {
    props: DynStaticProperties,
}

impl<P: TemporalPropertiesOps + Clone + Send + Sync + 'static + Static> IntoPy<PyObject>
    for TemporalProperties<P>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProperties::from(self).into_py(py)
    }
}

impl IntoPy<PyObject> for DynTemporalProperties {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProperties::from(self).into_py(py)
    }
}

impl<P: TemporalPropertiesOps + Clone> Repr for TemporalProperties<P> {
    fn repr(&self) -> String {
        format!("Properties({{{}}})", iterator_dict_repr(self.iter()))
    }
}

impl<P: TemporalPropertyViewOps> Repr for TemporalPropertyView<P> {
    fn repr(&self) -> String {
        format!("Property({})", iterator_repr(self.iter()))
    }
}

impl Repr for PyTemporalPropertyView {
    fn repr(&self) -> String {
        self.prop.repr()
    }
}

impl Repr for PyTemporalProperties {
    fn repr(&self) -> String {
        self.props.repr()
    }
}

impl<P: TemporalPropertyViewOps + Send + Sync + 'static> IntoPy<PyObject>
    for TemporalPropertyView<P>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalPropertyView {
            prop: TemporalPropertyView::new(Arc::new(self.props), self.id),
        }
        .into_py(py)
    }
}

impl<P: StaticPropertiesOps + Send + Sync + 'static> From<StaticProperties<P>>
    for PyStaticProperties
{
    fn from(value: StaticProperties<P>) -> Self {
        PyStaticProperties {
            props: StaticProperties::new(Arc::new(value.props)),
        }
    }
}

impl<P: StaticPropertiesOps + Send + Sync + 'static> IntoPy<PyObject> for StaticProperties<P> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyStaticProperties::from(self).into_py(py)
    }
}

impl<P: StaticPropertiesOps> Repr for StaticProperties<P> {
    fn repr(&self) -> String {
        format!("StaticProperties({{{}}})", iterator_dict_repr(self.iter()))
    }
}

impl Repr for PyStaticProperties {
    fn repr(&self) -> String {
        self.props.repr()
    }
}
