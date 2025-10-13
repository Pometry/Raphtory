#[macro_export]
macro_rules! py_borrowing_iter {
    ($inner:expr, $inner_t:ty, $closure:expr) => {{
        struct Iterator($inner_t);

        impl $crate::python::types::wrappers::iterators::PyIter for Iterator {
            fn iter(&self) -> $crate::db::api::view::BoxedLIter<'_, PyResult<PyObject>> {
                // forces the type inference to return the correct lifetimes,
                // calling the closure directly does not work
                fn apply<'a, O: $crate::python::types::wrappers::iterators::IntoPyIter<'a>>(
                    arg: &'a $inner_t,
                    f: impl FnOnce(&'a $inner_t) -> O,
                ) -> $crate::db::api::view::BoxedLIter<'a, PyResult<pyo3::PyObject>>
                {
                    $crate::python::types::wrappers::iterators::IntoPyIter::into_py_iter(f(arg))
                }
                apply(&self.0, $closure)
            }
        }

        $crate::python::types::wrappers::iterators::PyIter::into_py_iter(Iterator($inner))
    }};
}

#[macro_export]
macro_rules! py_borrowing_iter_result {
    ($inner:expr, $inner_t:ty, $closure:expr) => {{
        struct Iterator($inner_t);

        impl $crate::python::types::wrappers::iterators::PyIter for Iterator {
            fn iter(&self) -> $crate::db::api::view::BoxedLIter<PyResult<PyObject>> {
                // forces the type inference to return the correct lifetimes,
                // calling the closure directly does not work
                fn apply<'a, O: $crate::python::types::wrappers::iterators::IntoPyIterResult<'a>>(
                    arg: &'a $inner_t,
                    f: impl FnOnce(&'a $inner_t) -> O,
                ) -> $crate::db::api::view::BoxedLIter<'a, PyResult<pyo3::PyObject>>
                {
                    $crate::python::types::wrappers::iterators::IntoPyIterResult::into_py_iter_result(f(arg))
                }
                apply(&self.0, $closure)
            }
        }

        $crate::python::types::wrappers::iterators::PyIter::into_py_iter(Iterator($inner))
    }};
}

#[macro_export]
macro_rules! py_borrowing_iter_tuple_result {
    ($inner:expr, $inner_t:ty, $closure:expr) => {{
        struct Iterator($inner_t);

        impl $crate::python::types::wrappers::iterators::PyIter for Iterator {
            fn iter(&self) -> $crate::db::api::view::BoxedLIter<PyResult<PyObject>> {
                // forces the type inference to return the correct lifetimes,
                // calling the closure directly does not work
                fn apply<'a, O: $crate::python::types::wrappers::iterators::IntoPyIterTupleResult<'a>>(
                    arg: &'a $inner_t,
                    f: impl FnOnce(&'a $inner_t) -> O,
                ) -> $crate::db::api::view::BoxedLIter<'a, PyResult<pyo3::PyObject>>
                {
                    $crate::python::types::wrappers::iterators::IntoPyIterTupleResult::into_py_iter_tuple_result(f(arg))
                }
                apply(&self.0, $closure)
            }
        }

        $crate::python::types::wrappers::iterators::PyIter::into_py_iter(Iterator($inner))
    }};
}
