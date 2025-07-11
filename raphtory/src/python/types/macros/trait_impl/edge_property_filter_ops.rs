/// Macro for implementing all the EdgePropertyFilterOps methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `EdgePropertyFilterOps`
/// * base_type: The rust type of `field`
/// * name: The name of the object that appears in the docstring
macro_rules! impl_edge_property_filter_ops {
    ($obj:ident<$base_type:ty>, $field:ident, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            /// Return a filtered view that only includes edges that satisfy the filter
            ///
            /// Arguments:
            ///     filter (FilterExpr): The filter to apply to the edges.
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_edges(
                &self,
                filter: PyFilterExpr,
            ) -> Result<<$base_type as OneHopFilter<'static>>::Filtered<DynamicGraph>, GraphError>
            {
                Ok(self.$field.clone().filter_edges(filter)?.into_dyn_hop())
            }

            /// Return a filtered view that only includes exploded edges that satisfy the filter
            ///
            /// Arguments:
            ///     filter (FilterExpr): The filter to apply to the exploded edge properties.
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter_exploded_edges(
                &self,
                filter: PyFilterExpr,
            ) -> Result<<$base_type as OneHopFilter<'static>>::Filtered<DynamicGraph>, GraphError>
            {
                Ok(self.$field.filter_exploded_edges(filter)?.into_dyn_hop())
            }
        }
    };
}
