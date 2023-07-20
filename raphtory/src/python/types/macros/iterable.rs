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
    ($name:ident, $item:ty, $pyiter:ty) => {
        py_iterable!($name, $item, $item, $pyiter);
    };
    ($name:ident, $item:ty, $pyitem:ty, $pyiter:ty) => {
        #[pyclass]
        pub struct $name($crate::python::types::iterable::Iterable<$item, $pyitem>);

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
                    stringify!($name).to_string(),
                    value,
                ))
            }
        }
        _py_iterable_base_methods!($name, $pyiter);
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
    ($name:ident, $item:ty, $iter:ty) => {
        py_ordered_iterable!($name, $item, $item, $iter);
    };
    ($name:ident, $item:ty, $pyitem:ty, $iter:ty) => {
        py_iterable!($name, $item, $pyitem, $iter);
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
    ($name:ident, $item:ty, $iter:ty) => {
        py_numeric_iterable!($name, $item, $item, $iter);
    };
    ($name:ident, $item:ty, $pyitem:ty, $iter:ty) => {
        py_ordered_iterable!($name, $item, $pyitem, $iter);
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
    ($name:ident, $item:ty, $iter:ty) => {
        py_float_iterable!($name, $item, $item, $iter);
    };
    ($name:ident, $item:ty, $pyitem:ty, $iter:ty) => {
        py_iterable!($name, $item, $pyitem, $iter);
        _py_numeric_methods!($name, $item, $pyitem);
        _py_float_max_min_methods!($name, $pyitem);
    };
}

macro_rules! py_iterable_comp {
    ($name:ident, $item:ty, $cmp_item:ty, $cmp_internal:ident) => {
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
                    Err(PyTypeError::new_err("cannot compare"))
                }
            }
        }

        #[pymethods]
        impl $name {
            fn __richcmp__(
                &self,
                other: $cmp_internal,
                op: CompareOp,
                py: Python<'_>,
            ) -> PyResult<bool> {
                match op {
                    CompareOp::Lt => Err(PyNotImplementedError::new_err("not ordered")),
                    CompareOp::Le => Err(PyNotImplementedError::new_err("cannot compare")),
                    CompareOp::Eq => match other {
                        $cmp_internal::Vec(v) => {
                            Ok(self.iter().zip(v).all(|(t, o)| <$cmp_item>::from(t) == o))
                        }
                        $cmp_internal::This(o) => Ok(self
                            .iter()
                            .zip(o.borrow(py).iter())
                            .all(|(t, o)| <$cmp_item>::from(t) == <$cmp_item>::from(o))),
                    },
                    CompareOp::Ne => Ok(!self.__richcmp__(other, CompareOp::Eq, py)?),
                    CompareOp::Gt => Err(PyNotImplementedError::new_err("cannot compare")),
                    CompareOp::Ge => Err(PyNotImplementedError::new_err("cannot compare")),
                }
            }
        }
    };
}
