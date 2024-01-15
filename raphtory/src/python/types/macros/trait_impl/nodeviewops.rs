macro_rules! impl_nodeviewops {
    ($obj:ty, $field:ident, $base_type:ty, $name:literal) => {
        impl_timeops!($obj, $field, $base_type, $name);
        impl_layerops!($obj, $field, $base_type, $name);
        impl_repr!($obj, $field);

        #[pyo3::pymethods]
        impl $obj {
            /// Get the edges that are incident to this node.
            ///
            /// Returns:
            ///
            /// An iterator over the edges that are incident to this node.
            #[getter]
            fn edges(&self) -> <$base_type as NodeViewOps<'static>>::Edges {
                self.$field.edges()
            }

            /// Get the edges that point into this node.
            ///
            /// Returns:
            ///
            /// An iterator over the edges that point into this node.
            #[getter]
            fn in_edges(&self) -> <$base_type as NodeViewOps<'static>>::Edges {
                self.$field.in_edges()
            }

            /// Get the edges that point out of this node.
            ///
            /// Returns:
            ///
            /// An iterator over the edges that point out of this node.
            #[getter]
            fn out_edges(&self) -> <$base_type as NodeViewOps<'static>>::Edges {
                self.$field.out_edges()
            }

            /// Get the neighbours of this node.
            ///
            /// Returns:
            ///
            /// An iterator over the neighbours of this node.
            #[getter]
            fn neighbours(&self) -> <$base_type as NodeViewOps<'static>>::PathType {
                self.$field.neighbours()
            }

            /// Get the neighbours of this node that point into this node.
            ///
            /// Returns:
            ///
            /// An iterator over the neighbours of this node that point into this node.
            #[getter]
            fn in_neighbours(&self) -> <$base_type as NodeViewOps<'static>>::PathType {
                self.$field.in_neighbours()
            }

            /// Get the neighbours of this node that point out of this node.
            ///
            /// Returns:
            ///
            /// An iterator over the neighbours of this node that point out of this node.
            #[getter]
            fn out_neighbours(&self) -> <$base_type as NodeViewOps<'static>>::PathType {
                self.$field.out_neighbours()
            }
        }
    };
}
