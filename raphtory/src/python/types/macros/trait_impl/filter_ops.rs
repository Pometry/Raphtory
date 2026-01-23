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
            /// Return a filtered view that only includes nodes and edges that satisfy the filter
            ///
            /// Arguments:
            ///     filter (FilterExpr): The filter to apply to the nodes and edges.
            ///
            /// Returns:
            #[doc=concat!("    ", $name, ": The filtered view")]
            fn filter(
                &self,
                filter: &pyo3::Bound<'_, pyo3::types::PyAny>,
            ) -> PyResult<<$base_type as InternalFilter<'static>>::Filtered<DynamicGraph>> {
                if let Ok(expr) = filter.extract::<PyFilterExpr>() {
                    return Ok(self.$field.clone().filter(expr)?.into_dyn_hop());
                }

                if let Ok(builder) = filter.extract::<pyo3::PyRef<
                    '_,
                    crate::python::filter::property_filter_builders::PyViewFilterBuilder,
                >>() {
                    let expr = PyFilterExpr(builder.0.clone());
                    return Ok(self.$field.clone().filter(expr)?.into_dyn_hop());
                }

                Err(pyo3::exceptions::PyTypeError::new_err(
                    "argument 'filter' must be FilterExpr or a ViewFilterBuilder",
                ))
            }
        }
    };
}
