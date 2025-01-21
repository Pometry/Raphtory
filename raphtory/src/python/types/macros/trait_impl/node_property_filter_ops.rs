/// Macro for implementing all the NodePropertyFilterOps methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `NodePropertyFilterOps`
/// * base_type: The rust type of `field`
/// * name: The name of the object that appears in the docstring
macro_rules! impl_node_property_filter_ops {
    ($obj:ident<$base_type:ty>, $field:ident, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            /// Return a filtered view that only includes nodes that satisfy the filter
            ///
            /// Arguments:
            ///     filter (PropertyFilter): The filter to apply to the node properties. Construct a
            ///                              filter using `Prop`.
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_nodes(
                &self,
                filter: $crate::python::types::wrappers::prop::PyPropertyFilter,
            ) -> Result<
                <$base_type as OneHopFilter<'static>>::Filtered<
                    <PyPropertyFilter as InternalNodePropertyFilterOps>::NodePropertyFiltered<
                        'static,
                        <$base_type as OneHopFilter<'static>>::FilteredGraph,
                    >,
                >,
                GraphError,
            > {
                self.$field.clone().filter_nodes(filter)
            }
        }
    };
}
