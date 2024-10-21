#[macro_export]
macro_rules! py_borrowing_iter {
    ($inner:expr, $inner_t:ty, $closure:expr) => {{
        struct Iterator($inner_t);

        impl $crate::python::types::wrappers::iterators::PyIter for Iterator {
            fn iter(&self) -> raphtory_api::BoxedLIter<PyObject> {
                // forces the type inference to return the correct lifetimes,
                // calling the closure directly does not work
                fn apply<'a, O: $crate::python::types::wrappers::iterators::IntoPyIter<'a>>(
                    arg: &'a $inner_t,
                    f: impl FnOnce(&'a $inner_t) -> O,
                ) -> raphtory_api::BoxedLIter<'a, pyo3::PyObject> {
                    $crate::python::types::wrappers::iterators::IntoPyIter::into_py_iter(f(arg))
                }
                apply(&self.0, $closure)
            }
        }

        $crate::python::types::wrappers::iterators::PyIter::into_py_iter(Iterator($inner))
    }};
}
