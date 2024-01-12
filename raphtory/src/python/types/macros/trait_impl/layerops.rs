/// Macro for implementing all the LayerOps methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `LayerOps`
/// * base_type: The rust type of `field` (note that `<$base_type as LayerOps<'static>>::LayeredViewType`
///              should have an `IntoPy<PyObject>` implementation)
/// * name: The name of the object that appears in the docstring
macro_rules! impl_layerops {
    ($obj:ty, $field:ident, $base_type:ty, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            #[doc = concat!(r" Return a view of ", $name, " containing only the default edge layer")]
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn default_layer(&self) -> <$base_type as LayerOps<'static>>::LayeredViewType {
                self.$field.default_layer()
            }

            #[doc = concat!(" Return a view of ", $name, r#" containing the layer `"name"`"#)]
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn layer(
                &self,
                name: &str,
            ) -> Result<<$base_type as LayerOps<'static>>::LayeredViewType, $crate::core::utils::errors::GraphError> {
                self.$field.layer(name)
            }

            #[doc = concat!(" Return a view of ", $name, " containing all layers `names`")]
            /// Arguments:
            ///     names (list[str]): list of layer names for the new view
            ///
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn layers(
                &self,
                names: Vec<String>,
            ) -> Result<<$base_type as LayerOps<'static>>::LayeredViewType, $crate::core::utils::errors::GraphError> {
                self.$field.layer(names)
            }
        }
    };
}
