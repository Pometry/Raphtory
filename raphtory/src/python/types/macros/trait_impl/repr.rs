/// Macro for implementing all the repr method on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `Repr`
macro_rules! impl_repr {
    ($obj:ty, $field:ident) => {
        #[pyo3::pymethods]
        impl $obj {
            fn __repr__(&self) -> String {
                self.$field.repr()
            }
        }
    };
}
