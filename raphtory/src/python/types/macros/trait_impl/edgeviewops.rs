/// Macro for implementing all the LayerOps methods on a python wrapper
///
/// # Arguments
/// * obj: The struct the methods should be implemented for
/// * field: The name of the struct field holding the rust struct implementing `LayerOps`
/// * base_type: The rust type of `field` (note that `<$base_type as LayerOps<'static>>::LayeredViewType`
///              should have an `IntoPy<PyObject>` implementation)
/// * name: The name of the object that appears in the docstring
macro_rules! impl_edgeviewops {
    ($obj:ty, $field:ident, $base_type:ty, $name:literal) => {
        impl_timeops!($obj, $field, $base_type, $name);
        impl_layerops!($obj, $field, $base_type, $name);

        #[pymethods]
        impl $obj {
            /// Returns the source node of the edge.
            #[getter]
            fn src(&self) -> <$base_type as $crate::db::api::view::EdgeViewOps<'static>>::Nodes {
                self.$field.src()
            }

            /// Returns the destination node of the edge.
            #[getter]
            fn dst(&self) -> <$base_type as $crate::db::api::view::EdgeViewOps<'static>>::Nodes {
                self.$field.dst()
            }

            /// Returns the node at the other end of the edge (same as `dst()` for out-edges and `src()` for in-edges)
            #[getter]
            fn nbr(&self) -> <$base_type as $crate::db::api::view::EdgeViewOps<'static>>::Nodes {
                self.$field.nbr()
            }

            /// Explodes an edge and returns all instances it had been updated as seperate edges
            fn explode(
                &self,
            ) -> <$base_type as $crate::db::api::view::EdgeViewOps<'static>>::Exploded {
                self.$field.explode()
            }

            fn explode_layers(
                &self,
            ) -> <$base_type as $crate::db::api::view::EdgeViewOps<'static>>::Exploded {
                self.$field.explode_layers()
            }
        }
    };
}
