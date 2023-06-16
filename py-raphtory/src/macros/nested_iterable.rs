// Internal macro to create the struct for a nested iterable
macro_rules! _py_nested_iterable_base {
    ($name:ident, $item:ty, $pyitem:ty) => {
        #[pyclass]
        pub struct $name($crate::types::iterable::NestedIterable<$item, $pyitem>);

        impl Deref for $name {
            type Target = $crate::types::iterable::NestedIterable<$item, $pyitem>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<F: Fn() -> BoxedIter<BoxedIter<$item>> + Sync + Send + 'static> From<F> for $name {
            fn from(value: F) -> Self {
                Self($crate::types::iterable::NestedIterable::new(
                    stringify!($name).to_string(),
                    value,
                ))
            }
        }
    };
}

// Internal macro to create basic methods for a nested iterable
macro_rules! _py_nested_iterable_methods {
    ($name:ident, $pyitem:ty, $iter:ty) => {
        _py_iterable_base_methods!($name, $iter);

        #[pymethods]
        impl $name {
            pub fn collect(&self) -> Vec<Vec<$pyitem>> {
                self.iter()
                    .map(|it| it.map(|v| v.into()).collect())
                    .collect()
            }
        }
    };
}

// Internal macro to add mean and sum methods to a nested iterable
macro_rules! _py_nested_numeric_methods {
    ($name:ident, $item:ty, $value_iterable:ty) => {
        #[pymethods]
        impl $name {
            pub fn sum(&self) -> $value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<$item> = Box::new(builder().map(|it| it.sum()));
                    iter
                })
                .into()
            }

            pub fn mean(&self) -> Float64Iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<f64> = Box::new(builder().map(|it| it.mean()));
                    iter
                })
                .into()
            }
        }
    };
}

// Internal macro implementing max and min for a nested iterable of ordered values
macro_rules! _py_nested_ord_max_min_methods {
    ($name:ident, $item:ty, $option_value_iterable:ty) => {
        #[pymethods]
        impl $name {
            fn max(&self) -> $option_value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<Option<$item>> = Box::new(builder().map(|it| it.max()));
                    iter
                })
                .into()
            }

            fn min(&self) -> $option_value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<Option<$item>> = Box::new(builder().map(|it| it.min()));
                    iter
                })
                .into()
            }
        }
    };
}

// Internal macro implementing max and min for a nested iterable of float values
macro_rules! _py_nested_float_max_min_methods {
    ($name:ident, $item:ty, $option_value_iterable:ty) => {
        #[pymethods]
        impl $name {
            fn max(&self) -> $option_value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<Option<$item>> =
                        Box::new(builder().map(|it| it.max_by(|a, b| a.total_cmp(b))));
                    iter
                })
                .into()
            }

            fn min(&self) -> $option_value_iterable {
                let builder = self.builder.clone();
                (move || {
                    let iter: BoxedIter<Option<$item>> =
                        Box::new(builder().map(|it| it.min_by(|a, b| a.total_cmp(b))));
                    iter
                })
                .into()
            }
        }
    };
}

/// Create a nested Iterable
///
/// has `__iter__`, `__len__`, `__repr__` and `collect` methods
///
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator builder
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`, need Into<`pyitem`> to be implemented for `item`)
/// * `iter` - The python iterator wrapper that should be returned when calling `__iter__`
macro_rules! py_nested_iterable {
    ($name:ident, $item:ty, $iter:ty) => {
        py_nested_iterable!($name, $item, $item, $iter);
    };
    ($name:ident, $item:ty, $pyitem:ty, $iter:ty) => {
        _py_nested_iterable_base!($name, $item, $pyitem);
        _py_nested_iterable_methods!($name, $pyitem, $iter);
    };
}

/// Create a nested Iterable for ordered types
///
/// Additionally has `min` and `max` methods compared to `py_nested_iterable`
///
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator builder
/// * `iter` - The python iterator wrapper that should be returned when calling `__iter__`
/// * `option_value_iterable` - The iterable to return for `max` and `min` (should have item type `Option<Item>`)
macro_rules! py_nested_ordered_iterable {
    ($name:ident, $item:ty, $iter:ty, $option_value_iterable:ty) => {
        py_nested_iterable!($name, $item, $iter);
        _py_nested_ord_max_min_methods!($name, $item, $option_value_iterable);
    };
}

/// Create a nested Iterable for numeric ordered types
///
/// Additionally has `mean` and `sum` methods compared to `py_nested_ordered_iterable`
///
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator builder
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`, need Into<`pyitem`> to be implemented for `item`)
/// * `iter` - The python iterator wrapper that should be returned when calling `__iter__`
/// * `value_iterable` - The iterable to return for `sum` and `mean`
/// * `option_value_iterable` - The iterable to return for `max` and `min` (should have item type `Option<Item>`)
macro_rules! py_nested_numeric_iterable {
    ($name:ident, $item:ty, $iter:ty, $value_iterable:ty, $option_value_iterable:ty) => {
        py_nested_ordered_iterable!($name, $item, $iter, $option_value_iterable);
        _py_nested_numeric_methods!($name, $item, $value_iterable);
    };
}

/// Create a nested Iterable for float types
///
/// Same as `py_nested_numeric_iterable` but with special implementation of `max` and `min` for floats
///
/// # Arguments
///
/// * `name` - The identifier for the new struct
/// * `item` - The type of `Item` for the wrapped iterator builder
/// * `pyitem` - The type of the python wrapper for `Item` (optional if `item` implements `IntoPy`, need Into<`pyitem`> to be implemented for `item`)
/// * `iter` - The python iterator wrapper that should be returned when calling `__iter__`
/// * `value_iterable` - The iterable to return for `sum` and `mean`
/// * `option_value_iterable` - The iterable to return for `max` and `min` (should have item type `Option<Item>`)
#[allow(dead_code)]
macro_rules! py_nested_float_iterable {
    ($name:ident, $item:ty, $iter:ty, $value_iterable:ty, $option_value_iterable:ty) => {
        py_nested_iterable!($name, $item);
        _py_nested_numeric_methods!($name, $item, $value_iterable);
        _py_nested_float_max_min_methods!($name, $item, $option_value_iterable);
    };
}
