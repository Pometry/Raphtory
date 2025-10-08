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
                filter: AcceptFilter,
            ) -> Result<<$base_type as BaseFilter<'static>>::Filtered<DynamicGraph>, GraphError>
            {
                match filter {
                    AcceptFilter::Expr(expr) => {
                        Ok(self.$field.clone().filter(expr)?.into_dyn_hop())
                    }
                    AcceptFilter::Chain(chain) => {
                        // Force Rust-side validation by turning the chain into a temporary FilterExpr.
                        pyo3::Python::with_gil(|py| -> Result<(), GraphError> {
                            // Call .is_some() to produce a FilterExpr (any operator is fine).
                            let obj = chain
                                .bind(py)
                                .call_method0("is_some")
                                .map_err(|e| GraphError::InvalidFilter(e.to_string()))?;
                            let probe: PyFilterExpr = obj
                                .extract()
                                .map_err(|e| GraphError::InvalidFilter(e.to_string()))?;
                            // This triggers resolve/validate in Rust:
                            let _ = self.$field.clone().filter(probe)?;
                            Ok(())
                        })?;

                        // If we get here, the chain was syntactically valid but not a full FilterExpr.
                        Err(GraphError::InvalidFilter(
                            "Expected FilterExpr; got a property chain. Add a comparison (e.g., ... > 2).".into(),
                        ))
                    }
                }
            }
        }
    };
}
