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
            #[doc = concat!(" Write ", $name, " to cache file and initialise the cache.")]
            ///
            /// Future updates are tracked. Use `write_updates` to persist them to the
            /// cache file. If the file already exists its contents are overwritten.
            ///
            /// Arguments:
            ///     path (str): The path to the cache file
            fn cache(&self, path: &str) -> Result<(), GraphError> {
                $crate::serialise::CacheOps::cache(&self.$field, path)
            }

            /// Persist the new updates by appending them to the cache file.
            fn write_updates(&self) -> Result<(), GraphError> {
                $crate::serialise::CacheOps::write_updates(&self.$field)
            }

            #[doc = concat!(" Load ", $name, " from a file and initialise it as a cache file.")]
            ///
            /// Future updates are tracked. Use `write_updates` to persist them to the
            /// cache file.
            ///
            /// Arguments:
            ///   path (str): The path to the cache file
            ///
            /// Returns:
            #[doc = concat!("   ", $name)]
            #[staticmethod]
            fn load_cached(path: &str) -> Result<$base_type, GraphError> {
                <$base_type as $crate::serialise::CacheOps>::load_cached(path)
            }

            #[doc = concat!(" Load ", $name, " from a file.")]
            ///
            /// Arguments:
            ///   path (str): The path to the file.
            ///
            /// Returns:
            #[doc = concat!("   ", $name)]
            #[staticmethod]
            fn load_from_file(path: &str) -> Result<$base_type, GraphError> {
                <$base_type as $crate::serialise::StableDecode>::decode(path)
            }

            #[doc = concat!(" Saves the ", $name, " to the given path.")]
            ///
            /// Arguments:
            ///  path (str): The path to the file.
            fn save_to_file(&self, path: &str) -> Result<(), GraphError> {
                $crate::serialise::StableEncode::encode(&self.$field, path)
            }

            #[doc = concat!(" Load ", $name, " from serialised bytes.")]
            ///
            /// Arguments:
            ///   bytes (Bytes): The serialised bytes to decode
            ///
            /// Returns:
            #[doc = concat!("   ", $name)]
            #[staticmethod]
            fn deserialise(bytes: &[u8]) -> Result<$base_type, GraphError> {
                <$base_type as $crate::serialise::StableDecode>::decode_from_bytes(bytes)
            }

            #[doc = concat!(" Serialise ", $name, " to bytes.")]
            ///
            /// Returns:
            ///   Bytes
            fn serialise<'py>(&self, py: Python<'py>) -> &'py pyo3::types::PyBytes {
                let bytes = $crate::serialise::StableEncode::encode_to_vec(&self.$field);
                pyo3::types::PyBytes::new(py, &bytes)
            }
        }
    };
}
