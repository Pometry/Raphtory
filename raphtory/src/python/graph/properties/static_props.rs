use crate::core::Prop;
use crate::db::api::properties::internal::PropertiesOps;
use crate::db::api::properties::StaticProperties;
use crate::db::api::view::internal::Static;
use crate::python::graph::properties::DynProps;
use crate::python::types::repr::Repr;
use crate::python::types::wrappers::iterators::{NestedOptionPropIterable, OptionPropIterable};
use crate::python::utils::{PyGenericIterator, PyNestedGenericIterator};
use itertools::Itertools;
use pyo3::exceptions::{PyKeyError, PyNotImplementedError, PyTypeError};
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

pub type DynStaticProperties = StaticProperties<DynProps>;

impl<P: PropertiesOps + Send + Sync + Static + 'static> From<StaticProperties<P>>
    for DynStaticProperties
{
    fn from(value: StaticProperties<P>) -> Self {
        StaticProperties {
            props: Arc::new(value.props),
        }
    }
}

#[derive(PartialEq)]
pub struct StaticPropsComparable(HashMap<String, Prop>);

impl<'source> FromPyObject<'source> for StaticPropsComparable {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(sp) = ob.extract::<PyRef<PyStaticProperties>>() {
            Ok(sp.deref().into())
        } else if let Ok(m) = ob.extract::<HashMap<String, Prop>>() {
            Ok(StaticPropsComparable(m))
        } else {
            Err(PyTypeError::new_err("not comparable with properties"))
        }
    }
}

impl From<&PyStaticProperties> for StaticPropsComparable {
    fn from(value: &PyStaticProperties) -> Self {
        Self(value.props.as_map())
    }
}

impl From<DynStaticProperties> for StaticPropsComparable {
    fn from(value: DynStaticProperties) -> Self {
        Self(value.as_map())
    }
}

#[pyclass(name = "MetaData")]
pub struct PyStaticProperties {
    props: DynStaticProperties,
}

#[pymethods]
impl PyStaticProperties {
    fn keys(&self) -> Vec<String> {
        self.props.keys()
    }
    fn values(&self) -> Vec<Prop> {
        self.props.values()
    }
    fn items(&self) -> Vec<(String, Prop)> {
        self.props.iter().collect()
    }

    fn __getitem__(&self, key: &str) -> PyResult<Prop> {
        self.props
            .get(key)
            .ok_or(PyKeyError::new_err("No such property"))
    }

    fn get(&self, key: &str) -> Option<Prop> {
        // Fixme: Add option to specify default?
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

impl<P: PropertiesOps + Send + Sync + 'static> From<StaticProperties<P>> for PyStaticProperties {
    fn from(value: StaticProperties<P>) -> Self {
        PyStaticProperties {
            props: StaticProperties::new(Arc::new(value.props)),
        }
    }
}

impl Repr for PyStaticProperties {
    fn repr(&self) -> String {
        self.props.repr()
    }
}

py_iterable!(
    StaticPropsIterable,
    DynStaticProperties,
    PyStaticProperties,
    PyGenericIterator
);

pub enum StaticPropsIterComparable {
    Vec(Vec<StaticPropsComparable>),
    This(Py<StaticPropsIterable>),
}

impl<'source> FromPyObject<'source> for StaticPropsIterComparable {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if let Ok(s) = ob.extract::<Py<StaticPropsIterable>>() {
            Ok(StaticPropsIterComparable::This(s))
        } else if let Ok(v) = ob.extract::<Vec<StaticPropsComparable>>() {
            Ok(StaticPropsIterComparable::Vec(v))
        } else {
            Err(PyTypeError::new_err("cannot compare"))
        }
    }
}

#[pymethods]
impl StaticPropsIterable {
    fn keys(&self) -> Vec<String> {
        self.iter().map(|p| p.keys()).kmerge().dedup().collect()
    }

    fn values(&self) -> Vec<OptionPropIterable> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    fn items(&self) -> Vec<(String, OptionPropIterable)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    fn __getitem__(&self, key: String) -> PyResult<OptionPropIterable> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    fn get(&self, key: String) -> Option<OptionPropIterable> {
        self.__contains__(&key).then(|| {
            let builder = self.builder.clone();
            let key = Arc::new(key);
            (move || {
                let key = key.clone();
                builder().map(move |p| p.get(key.as_ref()))
            })
            .into()
        })
    }

    fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|p| p.contains(key))
    }

    pub fn __richcmp__(
        &self,
        other: StaticPropsIterComparable,
        op: CompareOp,
        py: Python<'_>,
    ) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Err(PyNotImplementedError::new_err("not ordered")),
            CompareOp::Le => Err(PyNotImplementedError::new_err("cannot compare")),
            CompareOp::Eq => match other {
                StaticPropsIterComparable::Vec(v) => Ok(self
                    .iter()
                    .zip(v)
                    .all(|(t, o)| StaticPropsComparable::from(t) == o)),
                StaticPropsIterComparable::This(o) => {
                    Ok(self.iter().zip(o.borrow(py).iter()).all(|(t, o)| {
                        StaticPropsComparable::from(t) == StaticPropsComparable::from(o)
                    }))
                }
            },
            CompareOp::Ne => Ok(!self.__richcmp__(other, CompareOp::Eq, py)?),
            CompareOp::Gt => Err(PyNotImplementedError::new_err("cannot compare")),
            CompareOp::Ge => Err(PyNotImplementedError::new_err("cannot compare")),
        }
    }
}

py_nested_iterable!(
    NestedStaticPropsIterable,
    DynStaticProperties,
    PyStaticProperties,
    PyNestedGenericIterator
);

#[pymethods]
impl NestedStaticPropsIterable {
    fn keys(&self) -> Vec<String> {
        self.iter()
            .flat_map(|it| it.map(|p| p.keys()))
            .kmerge()
            .dedup()
            .collect()
    }

    fn values(&self) -> Vec<NestedOptionPropIterable> {
        self.keys()
            .into_iter()
            .map(|k| self.get(k).expect("key exists"))
            .collect()
    }
    fn items(&self) -> Vec<(String, NestedOptionPropIterable)> {
        self.keys().into_iter().zip(self.values()).collect()
    }

    fn __getitem__(&self, key: String) -> PyResult<NestedOptionPropIterable> {
        self.get(key).ok_or(PyKeyError::new_err("No such property"))
    }

    fn get(&self, key: String) -> Option<NestedOptionPropIterable> {
        self.__contains__(&key).then(|| {
            let builder = self.builder.clone();
            let key = Arc::new(key);
            (move || {
                let key = key.clone();
                builder().map(move |it| {
                    let key = key.clone();
                    it.map(move |p| p.get(key.as_ref()))
                })
            })
            .into()
        })
    }

    fn __contains__(&self, key: &str) -> bool {
        self.iter().any(|mut it| it.any(|p| p.contains(key)))
    }
}
