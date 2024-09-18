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
            fn filter_edges_eq(
                &self,
                property: &str,
                value: Prop,
            ) -> <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType
            {
                crate::prelude::EdgePropertyFilterOps::filter_edges_eq(
                    &self.$field,
                    property,
                    value,
                )
            }

            fn filter_edges_lt(
                &self,
                property: &str,
                value: Prop,
            ) -> <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType
            {
                crate::prelude::EdgePropertyFilterOps::filter_edges_lt(
                    &self.$field,
                    property,
                    value,
                )
            }

            fn filter_edges_gt(
                &self,
                property: &str,
                value: Prop,
            ) -> <$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType
            {
                crate::prelude::EdgePropertyFilterOps::filter_edges_gt(
                    &self.$field,
                    property,
                    value,
                )
            }
        }
    };
}
