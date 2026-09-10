/// Macro for implementing all the FilterOps methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `FilterOps`
/// * base_type: The rust type of `field`
/// * name: The name of the object that appears in the docstring
///
/// Paths are `$crate`-qualified so the macro expands correctly regardless of
/// what the call site happens to import.

macro_rules! impl_filter_ops {
    ($obj:ident<$base_type:ty>, $field:ident, $name:literal) => {
        #[pyo3::pymethods]
        impl $obj {
            /// Return a filtered view that only includes nodes and edges that satisfy the filter
            ///
            /// Arguments:
            ///     filter (filter.FilterExpr): The filter to apply to the nodes and edges.
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter(
                &self,
                filter: $crate::python::filter::filter_expr::PyFilterExpr,
            ) -> pyo3::PyResult<
                <$base_type as $crate::db::api::view::internal::InternalFilter<'static>>::Filtered<
                    $crate::db::api::view::DynamicGraph,
                >,
            > {
                use $crate::db::api::view::{internal::IntoDynHop, Filter};
                Ok(self.$field.clone().filter(filter)?.into_dyn_hop())
            }
        }
    };
}
