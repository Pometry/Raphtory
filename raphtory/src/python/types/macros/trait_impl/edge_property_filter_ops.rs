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
            /// Return a filtered view that only includes edges that satisfy the filter
            ///
            /// Arguments
            ///     filter (PropertyFilter): The filter to apply to the edge properties. Construct a
            ///                              filter using `Prop`.
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_edges(&self, filter: &$crate::python::types::wrappers::prop::PyPropertyFilter) -> Result<<$base_type as crate::prelude::EdgePropertyFilterOps<'static>>::FilteredViewType,
                GraphError,> {
                let $crate::python::types::wrappers::prop::PyPropertyFilter {name, filter} = filter;
                $crate::prelude::EdgePropertyFilterOps::filter_edges(&self.$field, name, filter.clone())
            }


            /// Return a filtered view that only includes exploded edges that satisfy the filter
            ///
            /// Arguments:
            ///     filter (PropertyFilter): The filter to apply to the exploded edge properties. Construct a
            ///                              filter using `Prop`.
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_exploded_edges(
                &self,
                filter: &$crate::python::types::wrappers::prop::PyPropertyFilter,
            ) -> Result<
                <$base_type as crate::prelude::ExplodedEdgePropertyFilterOps<'static>>::FilteredViewType,
                GraphError,
            > {
                let $crate::python::types::wrappers::prop::PyPropertyFilter {name, filter} = filter;
                crate::prelude::ExplodedEdgePropertyFilterOps::filter_exploded_edges(
                    &self.$field,
                    name,
                    filter.clone(),
                )
            }
        }
    };
}
