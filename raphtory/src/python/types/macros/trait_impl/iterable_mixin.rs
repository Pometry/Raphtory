/// Macro for implementing iterable methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing rust methods `iter`, `len`, `is_empty`, `collect`
/// * collect_return_type: The return type of the `collect` method (needs to implement `IntoPyObject`)
/// * collect_py_return_type: The python return type (as a string) used for documentation
/// * element_name: The python element name (as a string) used for documentation
macro_rules! impl_iterable_mixin {
    ($obj:ty, $field:ident, $collect_return_type:ty, $collect_py_return_type:literal, $element_name:literal, $iter:expr) => {
        #[pymethods]
        impl $obj {
            fn __len__(&self) -> usize {
                self.$field.len()
            }

            fn __bool__(&self) -> bool {
                !self.$field.is_empty()
            }

            fn __iter__(&self) -> $crate::python::utils::PyGenericIterator {
                ($iter)(&self.$field).into()
            }

            #[doc = concat!(" Collect all ", $element_name, "s into a list")]
            ///
            /// Returns:
            #[doc = concat!("     ", $collect_py_return_type, ": the list of ", $element_name, "s")]
            fn collect(&self) -> $collect_return_type {
                self.$field.collect()
            }
        }
    };

    ($obj:ty, $field:ident, $collect_return_type:ty, $collect_py_return_type:literal, $element_name:literal) => {
        #[pymethods]
        impl $obj {
            fn __len__(&self) -> usize {
                self.$field.len()
            }

            fn __bool__(&self) -> bool {
                !self.$field.is_empty()
            }

            fn __iter__(&self) -> $crate::python::utils::PyGenericIterator {
                self.$field.iter().into()
            }

            #[doc = concat!(" Collect all ", $element_name, "s into a list")]
            ///
            /// Returns:
            #[doc = concat!("     ", $collect_py_return_type, ": the list of ", $element_name, "s")]
            fn collect(&self) -> $collect_return_type {
                self.$field.collect()
            }
        }
    };
}
