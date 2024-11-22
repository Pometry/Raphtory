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
            /// Errors if the layer does not exist
            ///
            /// Arguments:
            ///     name (str): then name of the layer.
            ///
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn layer(
                &self,
                name: &str,
            ) -> Result<<$base_type as LayerOps<'static>>::LayeredViewType, $crate::core::utils::errors::GraphError> {
                self.$field.layers(name)
            }

            #[doc = concat!(" Return a view of ", $name, " containing all layers except the excluded `name`")]
            /// Errors if any of the layers do not exist.
            ///
            /// Arguments:
            ///     name (str): layer name that is excluded for the new view
            ///
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn exclude_layer(
                &self,
                name: &str,
            ) -> Result<<$base_type as LayerOps<'static>>::LayeredViewType, $crate::core::utils::errors::GraphError> {
                self.$field.exclude_layers(name)
            }

            #[doc = concat!(" Return a view of ", $name, " containing all layers except the excluded `name`")]
            /// Arguments:
            ///     name (str): layer name that is excluded for the new view
            ///
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn exclude_valid_layer(
                &self,
                name: &str,
            ) -> <$base_type as LayerOps<'static>>::LayeredViewType {
                self.$field.exclude_valid_layers(name)
            }

            #[doc = concat!(" Check if ", $name, r#" has the layer `"name"`"#)]
            ///
            /// Arguments:
            ///     name (str): the name of the layer to check
            ///
            /// Returns:
            ///     bool
            fn has_layer(
                &self,
                name: &str,
            ) -> bool {
                self.$field.has_layer(name)
            }

            #[doc = concat!(" Return a view of ", $name, " containing all layers `names`")]
            /// Errors if any of the layers do not exist.
            ///
            /// Arguments:
            ///     names (list[str]): list of layer names for the new view
            ///
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn layers(
                &self,
                names: Vec<String>,
            ) -> Result<<$base_type as LayerOps<'static>>::LayeredViewType, $crate::core::utils::errors::GraphError> {
                self.$field.layers(names)
            }

            #[doc = concat!(" Return a view of ", $name, " containing all layers except the excluded `names`")]
            /// Errors if any of the layers do not exist.
            ///
            /// Arguments:
            ///     names (list[str]): list of layer names that are excluded for the new view
            ///
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn exclude_layers(
                &self,
                names: Vec<String>,
            ) -> Result<<$base_type as LayerOps<'static>>::LayeredViewType, $crate::core::utils::errors::GraphError> {
                self.$field.exclude_layers(names)
            }

            #[doc = concat!(" Return a view of ", $name, " containing all layers except the excluded `names`")]
            /// Arguments:
            ///     names (list[str]): list of layer names that are excluded for the new view
            ///
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn exclude_valid_layers(
                &self,
                names: Vec<String>,
            ) -> <$base_type as LayerOps<'static>>::LayeredViewType {
                self.$field.exclude_valid_layers(names)
            }

            #[doc = concat!(" Return a view of ", $name, " containing all layers `names`")]
            /// Any layers that do not exist are ignored
            ///
            /// Arguments:
            ///     names (list[str]): list of layer names for the new view
            ///
            /// Returns:
            #[doc = concat!("     ", $name, ": The layered view")]
            fn valid_layers(
                &self,
                names: Vec<String>,
            ) -> <$base_type as LayerOps<'static>>::LayeredViewType {
                self.$field.valid_layers(names)
            }
        }
    };
}
