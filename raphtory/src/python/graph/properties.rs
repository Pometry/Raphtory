use crate::core::Prop;
use crate::db::api::properties::internal::{
    BoxableTemporalProperties, InheritStaticPropertiesOps, InheritTemporalPropertiesOps,
    InheritTempralPropertyViewOps, StaticProperties, StaticPropertiesOps, TemporalProperties,
    TemporalPropertyView, TemporalPropertyViewOps,
};
use crate::db::api::view::internal::{DynamicGraph, Static};
use crate::python::types::repr::{iterator_dict_repr, iterator_repr, Repr};
use crate::python::utils::PyGenericIterator;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use std::sync::Arc;

pub type DynTemporalProperties =
    TemporalProperties<Arc<dyn BoxableTemporalProperties + Send + Sync>>;
pub type DynTemporalProperty =
    TemporalPropertyView<Arc<dyn BoxableTemporalProperties + Send + Sync>>;

impl InheritTemporalPropertiesOps for Arc<dyn BoxableTemporalProperties + Send + Sync> {}

impl InheritTempralPropertyViewOps for Arc<dyn TemporalPropertyViewOps + Send + Sync> {}

impl<P: BoxableTemporalProperties + Clone + Send + Sync + Static + 'static>
    From<TemporalProperties<P>> for DynTemporalProperties
{
    fn from(value: TemporalProperties<P>) -> Self {
        TemporalProperties::new(Arc::new(value.props))
    }
}

impl From<TemporalProperties<DynamicGraph>> for DynTemporalProperties {
    fn from(value: TemporalProperties<DynamicGraph>) -> Self {
        let props: Arc<dyn BoxableTemporalProperties + Send + Sync> = Arc::new(value.props);
        TemporalProperties::new(props)
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
    fn values(&self) -> Vec<DynTemporalProperty> {
        self.props.values()
    }
    fn items(&self) -> Vec<(String, DynTemporalProperty)> {
        self.props.iter().collect()
    }

    fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        let v = self
            .props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))?;
        Ok(v.value().unwrap())
    }

    fn get(&self, key: &str) -> Option<DynTemporalProperty> {
        /// Fixme: Add option to specify default?
        self.props.get(key)
    }

    fn __iter__(&self) -> PyGenericIterator {
        self.keys().into_iter().into()
    }

    fn __contains__(&self, key: &str) -> bool {
        self.props.contains(key)
    }

    fn __len__(&self) -> usize {
        self.keys().len()
    }
}

#[pyclass(name = "Property")]
pub struct PyTemporalPropertyView {
    prop: DynTemporalProperty,
}

#[pymethods]
impl PyTemporalPropertyView {
    pub fn history(&self) -> Vec<i64> {
        self.prop.history()
    }
    pub fn values(&self) -> Vec<Prop> {
        self.prop.values()
    }
    pub fn __iter__(&self) -> PyGenericIterator {
        self.prop.iter().into()
    }
    pub fn at(&self, t: i64) -> Option<Prop> {
        self.prop.at(t)
    }
    pub fn value(&self) -> Option<Prop> {
        self.prop.value()
    }
}

impl<P: BoxableTemporalProperties + Send + Sync + 'static> From<TemporalPropertyView<P>>
    for PyTemporalPropertyView
{
    fn from(value: TemporalPropertyView<P>) -> Self {
        Self {
            prop: TemporalPropertyView {
                id: value.id,
                props: Arc::new(value.props),
            },
        }
    }
}

pub type DynStaticProperties = StaticProperties<Arc<dyn StaticPropertiesOps + Send + Sync>>;

impl InheritStaticPropertiesOps for Arc<dyn StaticPropertiesOps + Send + Sync> {}

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

impl<P: BoxableTemporalProperties + Clone + Send + Sync + 'static + Static> IntoPy<PyObject>
    for TemporalProperties<P>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProperties::from(self).into_py(py)
    }
}

impl IntoPy<PyObject> for TemporalProperties<DynamicGraph> {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProperties::from(self).into_py(py)
    }
}

impl IntoPy<PyObject> for DynTemporalProperties {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalProperties::from(self).into_py(py)
    }
}

impl<P: BoxableTemporalProperties + Clone> Repr for TemporalProperties<P> {
    fn repr(&self) -> String {
        format!("Properties({{{}}})", iterator_dict_repr(self.iter()))
    }
}

impl<P: BoxableTemporalProperties> Repr for TemporalPropertyView<P> {
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

impl<P: BoxableTemporalProperties + Send + Sync + 'static> IntoPy<PyObject>
    for TemporalPropertyView<P>
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyTemporalPropertyView::from(self).into_py(py)
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
