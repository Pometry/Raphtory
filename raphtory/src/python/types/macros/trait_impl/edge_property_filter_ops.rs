/// Macro for implementing all the LayerOps methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `LayerOps`
/// * base_type: The rust type of `field` (note that `<$base_type as LayerOps<'static>>::LayeredViewType`
///              should have an `IntoPy<PyObject>` implementation)
/// * name: The name of the object that appears in the docstring
macro_rules! impl_edge_property_filter_ops {
    ($obj:ident<$base_type:ty>, $field:ident, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            /// Return a filtered view that only includes edges with a given property value
            ///
            /// Arguments:
            ///     property (str): The name of the property to use for filtering
            ///                     (looked up in temporal properties first and falls
            ///                     back to constant properties
            ///     value (Any): The property value to compare with
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_edges_eq(
                &self,
                property: &str,
                value: Prop,
            ) -> Result<
                <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType,
                GraphError,
            > {
                crate::prelude::EdgePropertyFilterOps::filter_edges_eq(
                    &self.$field,
                    property,
                    value,
                )
            }

            /// Return a filtered view that only includes edges with a property value not equal to a given value
            ///
            /// Arguments:
            ///     property (str): The name of the property to use for filtering
            ///                     (looked up in temporal properties first and falls
            ///                     back to constant properties
            ///     value (Any): The property value to compare with
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_edges_ne(
                &self,
                property: &str,
                value: Prop,
            ) -> Result<
                <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType,
                GraphError,
            > {
                crate::prelude::EdgePropertyFilterOps::filter_edges_ne(
                    &self.$field,
                    property,
                    value,
                )
            }

            /// Return a filtered view that only includes edges with a property value less than a
            /// given value
            ///
            /// Arguments:
            ///     property (str): The name of the property to use for filtering
            ///                     (looked up in temporal properties first and falls
            ///                     back to constant properties
            ///     value (Any): The property value to compare with
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_edges_lt(
                &self,
                property: &str,
                value: Prop,
            ) -> Result<
                <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType,
                GraphError,
            > {
                crate::prelude::EdgePropertyFilterOps::filter_edges_lt(
                    &self.$field,
                    property,
                    value,
                )
            }

            /// Return a filtered view that only includes edges with a property value less than or equal to a
            /// given value
            ///
            /// Arguments:
            ///     property (str): The name of the property to use for filtering
            ///                     (looked up in temporal properties first and falls
            ///                     back to constant properties
            ///     value (Any): The property value to compare with
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_edges_le(
                &self,
                property: &str,
                value: Prop,
            ) -> Result<
                <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType,
                GraphError,
            > {
                crate::prelude::EdgePropertyFilterOps::filter_edges_le(
                    &self.$field,
                    property,
                    value,
                )
            }

            /// Return a filtered view that only includes edges with a property value greater than a
            /// given value
            ///
            /// Arguments:
            ///     property (str): The name of the property to use for filtering
            ///                     (looked up in temporal properties first and falls
            ///                     back to constant properties
            ///     value (Any): The property value to compare with
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_edges_gt(
                &self,
                property: &str,
                value: Prop,
            ) -> Result<
                <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType,
                GraphError,
            > {
                crate::prelude::EdgePropertyFilterOps::filter_edges_gt(
                    &self.$field,
                    property,
                    value,
                )
            }

            /// Return a filtered view that only includes edges with a property value greater than or equal to a
            /// given value
            ///
            /// Arguments:
            ///     property (str): The name of the property to use for filtering
            ///                     (looked up in temporal properties first and falls
            ///                     back to constant properties
            ///     value (Any): The property value to compare with
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_edges_ge(
                &self,
                property: &str,
                value: Prop,
            ) -> Result<
                <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType,
                GraphError,
            > {
                crate::prelude::EdgePropertyFilterOps::filter_edges_ge(
                    &self.$field,
                    property,
                    value,
                )
            }
        }
    };
}
