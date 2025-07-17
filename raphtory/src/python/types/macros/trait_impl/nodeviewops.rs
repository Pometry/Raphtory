macro_rules! impl_nodeviewops {
    ($obj:ident, $field:ident, $base_type:ty, $name:literal, $edge_type:literal, $path_type:literal) => {
        impl_timeops!($obj, $field, $base_type, $name);
        impl_filter_ops!($obj<$base_type>, $field, $name);
        impl_layerops!($obj, $field, $base_type, $name);
        impl_repr!($obj, $field);

        #[pyo3::pymethods]
        impl $obj {
            /// Get the edges that are incident to this node.
            ///
            /// Returns:
            ///
            #[doc = concat!("     ", $edge_type, ": The incident edges.")]
            #[getter]
            fn edges(&self) -> <$base_type as NodeViewOps<'static>>::Edges {
                self.$field.edges()
            }

            /// Get the edges that point into this node.
            ///
            /// Returns:
            ///
            #[doc = concat!("     ", $edge_type, ": The inbound edges.")]
            #[getter]
            fn in_edges(&self) -> <$base_type as NodeViewOps<'static>>::Edges {
                self.$field.in_edges()
            }

            /// Get the edges that point out of this node.
            ///
            /// Returns:
            ///
            #[doc = concat!("     ", $edge_type, ": The outbound edges.")]
            #[getter]
            fn out_edges(&self) -> <$base_type as NodeViewOps<'static>>::Edges {
                self.$field.out_edges()
            }

            /// Get the neighbours of this node.
            ///
            /// Returns:
            ///
            #[doc = concat!("     ", $path_type, ": The neighbours (both inbound and outbound).")]
            #[getter]
            fn neighbours(&self) -> <$base_type as NodeViewOps<'static>>::PathType {
                self.$field.neighbours()
            }

            /// Get the neighbours of this node that point into this node.
            ///
            /// Returns:
            ///
            #[doc = concat!("     ", $path_type, ": The in-neighbours.")]
            #[getter]
            fn in_neighbours(&self) -> <$base_type as NodeViewOps<'static>>::PathType {
                self.$field.in_neighbours()
            }

            /// Get the neighbours of this node that point out of this node.
            ///
            /// Returns:
            ///
            #[doc = concat!("     ", $path_type, ": The out-neighbours.")]
            #[getter]
            fn out_neighbours(&self) -> <$base_type as NodeViewOps<'static>>::PathType {
                self.$field.out_neighbours()
            }
        }
    };
}
