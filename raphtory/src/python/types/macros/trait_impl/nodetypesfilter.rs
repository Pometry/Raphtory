macro_rules! impl_nodetypesfilter {
    ($obj:ty, $field:ident, $base_type:ty, $name:literal) => {

        #[pyo3::pymethods]
        impl $obj {

            /// Filter nodes by node types
            ///
            /// Arguments:
            ///     node_types (list[str]): list of node types
            ///
            fn type_filter(&self, node_types: Vec<&str>) -> <$base_type as $crate::db::api::view::internal::OneHopFilter<'static>>::Filtered<$crate::db::graph::views::node_type_filtered_subgraph::TypeFilteredSubgraph<<$base_type as $crate::db::api::view::internal::OneHopFilter<'static>>::FilteredGraph>> {
                self.$field.type_filter(node_types)
            }
        }
    };
}
