/// Add equality support to pyclass
///
/// # Arguments
///
/// * `name` - The identifier for the struct
/// * `cmp_item` - Struct to use for comparisons, needs to support `cmp_item: From<&name>`
///                and `cmp_item: PartialEq` and `cmp_item: FromPyObject` with conversion for all
///                the python types we want to compare with
macro_rules! py_eq {
    ($name:ty, $cmp_name:ty) => {
        #[pyo3::pymethods]
        impl $name {
            pub fn __richcmp__(
                &self,
                other: $cmp_name,
                op: pyo3::basic::CompareOp,
            ) -> pyo3::PyResult<bool> {
                match op {
                    pyo3::basic::CompareOp::Lt => Err(PyTypeError::new_err("not ordered")),
                    pyo3::basic::CompareOp::Le => Err(PyTypeError::new_err("not ordered")),
                    pyo3::basic::CompareOp::Eq => Ok(<$cmp_name>::from(self) == other),
                    pyo3::basic::CompareOp::Ne => Ok(<$cmp_name>::from(self) != other),
                    pyo3::basic::CompareOp::Gt => Err(PyTypeError::new_err("not ordered")),
                    pyo3::basic::CompareOp::Ge => Err(PyTypeError::new_err("not ordered")),
                }
            }
        }
    };
}
