/// Macro for implementing all the FilterOps methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `FilterOps`
/// * base_type: The rust type of `field`
/// * name: The name of the object that appears in the docstring
macro_rules! impl_filter_ops {
    ($obj:ident<$base_type:ty>, $field:ident, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            /// Return a filtered view that only includes edges that satisfy the filter
            ///
            /// Arguments:
            ///     filter (FilterExpr): The filter to apply to the nodes and edges.
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter(
                &self,
                filter: PyFilterExpr,
            ) -> Result<<$base_type as BaseFilter<'static>>::Filtered<DynamicGraph>, GraphError>
            {
                Ok(self.$field.clone().filter(filter)?.into_dyn_hop())
            }
        }
    };
}
