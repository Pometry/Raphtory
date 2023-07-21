// internal macro for sum and mean methods
macro_rules! _py_numeric_methods {
    ($name:ident, $item:ty, $pyitem:ty) => {
        #[pymethods]
        impl $name {
            pub fn sum(&self) -> $pyitem {
                let v: $item = self.iter().sum();
                v.into()
            }

            pub fn mean(&self) -> f64 {
                use $crate::python::types::wrappers::iterators::MeanExt;
                self.iter().mean()
            }
        }
    };
}

// Internal macro defining max and min on ordered iterables
macro_rules! _py_ord_max_min_methods {
    ($name:ident, $pyitem:ty) => {
        #[pymethods]
        impl $name {
            pub fn max(&self) -> Option<$pyitem> {
                self.iter().max().map(|v| v.into())
            }

            pub fn min(&self) -> Option<$pyitem> {
                self.iter().min().map(|v| v.into())
            }
        }
    };
}

// Internal macro defining max and min on float iterables
macro_rules! _py_float_max_min_methods {
    ($name:ident, $pyitem:ty) => {
        #[pymethods]
        impl $name {
            pub fn max(&self) -> Option<$pyitem> {
                self.iter().max_by(|a, b| a.total_cmp(b)).map(|v| v.into())
            }
            pub fn min(&self) -> Option<$pyitem> {
                self.iter().min_by(|a, b| a.total_cmp(b)).map(|v| v.into())
            }
        }
    };
}

// Internal macro for methods supported by all iterables (also used by nested iterables)
macro_rules! _py_iterable_base_methods {
    ($name:ident, $iter:ty) => {
        #[pymethods]
        impl $name {
            pub fn __iter__(&self) -> $iter {
                self.iter().into()
            }

            pub fn __len__(&self) -> usize {
                self.iter().count()
            }

            pub fn __repr__(&self) -> String {
                self.repr()
            }
        }
    };
}

// internal macro for the collect method (as it is different for nested iterables)
macro_rules! _py_iterable_collect_method {
    ($name:ident, $pyitem:ty) => {
        #[pymethods]
        impl $name {
            pub fn collect(&self) -> Vec<$pyitem> {
                self.iter().map(|v| v.into()).collect()
            }
        }
    };
}

/// Construct a python Iterable struct which wraps a closure that returns an iterator
///
/// Has methods `__iter__`, `__len__`, `__repr__`, `collect`
///
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator builder
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`, need Into<`pyitem`> to be implemented for `item`)
/// * `pyiter` - The python iterator wrapper that should be returned when calling `__iter__` (needs to have the same `item` and `pyitem`)
macro_rules! py_iterable {
    ($name:ident, $item:ty) => {
        py_iterable!($name, $item, $item);
    };
    ($name:ident, $item:ty, $pyitem:ty) => {
        #[pyclass]
        pub struct $name($crate::python::types::iterable::Iterable<$item, $pyitem>);

        impl Repr for $name {
            fn repr(&self) -> String {
                self.0.repr()
            }
        }

        impl std::ops::Deref for $name {
            type Target = $crate::python::types::iterable::Iterable<$item, $pyitem>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<F: Fn() -> It + Send + Sync + 'static, It: Iterator + Send + 'static> From<F> for $name
        where
            It::Item: Into<$item>,
        {
            fn from(value: F) -> Self {
                Self($crate::python::types::iterable::Iterable::new(
                    stringify!($name),
                    value,
                ))
            }
        }
        _py_iterable_base_methods!($name, $crate::python::utils::PyGenericIterator);
        _py_iterable_collect_method!($name, $pyitem);
    };
}

/// Construct a python Iterable struct which wraps a closure that returns an iterator of ordered values
///
/// additionally adds the `min` and `max` methods to those created by `py_iterable`
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator builder
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`, need Into<`pyitem`> to be implemented for `item`)
/// * `pyiter` - The python iterator wrapper that should be returned when calling `__iter__` (needs to have the same `item` and `pyitem`)
macro_rules! py_ordered_iterable {
    ($name:ident, $item:ty) => {
        py_ordered_iterable!($name, $item, $item);
    };
    ($name:ident, $item:ty, $pyitem:ty) => {
        py_iterable!($name, $item, $pyitem);
        _py_ord_max_min_methods!($name, $pyitem);
    };
}

/// Construct a python Iterable struct which wraps a closure that returns an iterator of ordered and summable values
///
/// additionally adds the `mean` and `sum` methods to those created by `py_ordered_iterable`
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator builder
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`, need Into<`pyitem`> to be implemented for `item`)
/// * `pyiter` - The python iterator wrapper that should be returned when calling `__iter__` (needs to have the same `item` and `pyitem`)
macro_rules! py_numeric_iterable {
    ($name:ident, $item:ty) => {
        py_numeric_iterable!($name, $item, $item);
    };
    ($name:ident, $item:ty, $pyitem:ty) => {
        py_ordered_iterable!($name, $item, $pyitem);
        _py_numeric_methods!($name, $item, $pyitem);
    };
}

/// Construct a python Iterable struct which wraps a closure that returns an iterator of float values
///
/// This acts the same as `py_numeric_iterable` but with special implementations of `max` and `min` for floats.
///
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator builder
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`, need Into<`pyitem`> to be implemented for `item`)
/// * `pyiter` - The python iterator wrapper that should be returned when calling `__iter__` (needs to have the same `item` and `pyitem`)
macro_rules! py_float_iterable {
    ($name:ident, $item:ty) => {
        py_float_iterable!($name, $item, $item);
    };
    ($name:ident, $item:ty, $pyitem:ty) => {
        py_iterable!($name, $item, $pyitem);
        _py_numeric_methods!($name, $item, $pyitem);
        _py_float_max_min_methods!($name, $pyitem);
    };
}

/// Add equality support to iterable
///
///
/// # Arguments
///
/// * `name` - The identifier for the iterable struct
/// * `cmp_item` - Struct to use for comparisons, needs to support `cmp_item: From<item>`
///                and `cmp_item: PartialEq` and FromPyObject for all the python types we
///                want to compare with
/// * `cmp_internal` - Name for the internal Enum that is created by the macro to implement
///                    the conversion from python (only needed because we can't create our own
///                    unique identifier without a proc macro)
macro_rules! py_iterable_comp {
    ($name:ty, $cmp_item:ty, $cmp_internal:ident) => {
        #[derive(Clone)]
        enum $cmp_internal {
            Vec(Vec<$cmp_item>),
            This(Py<$name>),
        }

        impl<'source> FromPyObject<'source> for $cmp_internal {
            fn extract(ob: &'source PyAny) -> PyResult<Self> {
                if let Ok(s) = ob.extract::<Py<$name>>() {
                    Ok($cmp_internal::This(s))
                } else if let Ok(v) = ob.extract::<Vec<$cmp_item>>() {
                    Ok($cmp_internal::Vec(v))
                } else {
                    Err(pyo3::exceptions::PyTypeError::new_err("cannot compare"))
                }
            }
        }

        impl $cmp_internal {
            fn iter_py<'py>(
                &'py self,
                py: Python<'py>,
            ) -> Box<dyn Iterator<Item = $cmp_item> + 'py> {
                match self {
                    Self::Vec(v) => Box::new(v.iter().cloned()),
                    Self::This(t) => Box::new(t.borrow(py).iter().map_into()),
                }
            }
        }

        impl PartialEq for $cmp_internal {
            fn eq(&self, other: &Self) -> bool {
                Python::with_gil(|py| self.iter_py(py).eq(other.iter_py(py)))
            }
        }

        impl<I: Iterator<Item = J>, J: Into<$cmp_item>> From<I> for $cmp_internal {
            fn from(value: I) -> Self {
                Self::Vec(value.map_into().collect())
            }
        }

        #[pymethods]
        impl $name {
            fn __richcmp__(
                &self,
                other: $cmp_internal,
                op: pyo3::basic::CompareOp,
                py: Python<'_>,
            ) -> PyResult<bool> {
                match op {
                    pyo3::basic::CompareOp::Lt => {
                        Err(pyo3::exceptions::PyTypeError::new_err("not ordered"))
                    }
                    pyo3::basic::CompareOp::Le => {
                        Err(pyo3::exceptions::PyTypeError::new_err("not ordered"))
                    }
                    pyo3::basic::CompareOp::Eq => Ok(self
                        .iter()
                        .map(|t| <$cmp_item>::from(t))
                        .eq(other.iter_py(py))),
                    pyo3::basic::CompareOp::Ne => {
                        Ok(!self.__richcmp__(other, pyo3::basic::CompareOp::Eq, py)?)
                    }
                    pyo3::basic::CompareOp::Gt => {
                        Err(pyo3::exceptions::PyTypeError::new_err("not ordered"))
                    }
                    pyo3::basic::CompareOp::Ge => {
                        Err(pyo3::exceptions::PyTypeError::new_err("not ordered"))
                    }
                }
            }
        }
    };
}
