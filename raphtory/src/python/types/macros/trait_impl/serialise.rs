/// Macro for implementing all the Cache methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `Cache`
/// * base_type: The rust type of `field`
/// * name: The name of the object that appears in the docstring
macro_rules! impl_serialise {
    ($obj:ty, $field:ident: $base_type:ty, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            #[doc = concat!(" Load ", $name, " from a parquet file.")]
            ///
            /// Arguments:
            ///   path (str): The path to the file.
            ///
            /// Returns:
            #[doc = concat!("   ", $name, ":")]
            #[staticmethod]
            fn load_from_file(path: PathBuf) -> Result<$base_type, GraphError> {
                <$base_type as $crate::serialise::StableDecode>::decode(path)
            }

            #[doc = concat!(" Saves the ", $name, " to the given path in parquet format.")]
            ///
            /// Arguments:
            ///     path (str): The path to the file.
            ///
            /// Returns:
            ///     None:
            fn save_to_file(&self, path: PathBuf) -> Result<(), GraphError> {
                $crate::serialise::StableEncode::encode(&self.$field, path)
            }

            #[doc = concat!(" Saves the ", $name, " to the given path.")]
            ///
            /// Arguments:
            ///     path (str): The path to the file.
            /// Returns:
            ///     None:
            fn save_to_zip(&self, path: PathBuf) -> Result<(), GraphError> {
                let folder = $crate::serialise::GraphFolder::new_as_zip(path);
                $crate::serialise::StableEncode::encode(&self.$field, folder)
            }

            #[doc = concat!(" Load ", $name, " from serialised bytes.")]
            ///
            /// Arguments:
            ///     bytes (bytes): The serialised bytes to decode
            ///
            /// Returns:
            #[doc = concat!("   ", $name, ":")]
            #[staticmethod]
            fn deserialise(bytes: &[u8]) -> Result<$base_type, GraphError> {
                <$base_type as $crate::serialise::StableDecode>::decode_from_bytes(bytes)
            }

            #[doc = concat!(" Serialise ", $name, " to bytes.")]
            ///
            /// Returns:
            ///   bytes:
            fn serialise<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
                let bytes = $crate::serialise::StableEncode::encode_to_bytes(&self.$field);
                pyo3::types::PyBytes::new(py, &bytes)
            }
        }
    };
}
