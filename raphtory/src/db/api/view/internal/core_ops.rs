use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            properties::{
                graph_props::GraphProps,
                props::{ArcReadLockedVec, Meta},
                tprop::{LockedLayeredTProp, TProp},
            },
            vertices::{vertex_ref::VertexRef, vertex_store::VertexStore},
            LayerIds, EID, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{LockedLayeredIndex, TimeIndex, TimeIndexEntry},
            ArcEntry,
        },
        ArcStr, Prop,
    },
    db::api::view::{internal::Base, BoxedIter},
};
use enum_dispatch::enum_dispatch;

/// Core functions that should (almost-)always be implemented by pointing at the underlying graph.
#[enum_dispatch]
pub trait CoreGraphOps {
    /// get the number of vertices in the main graph
    fn unfiltered_num_vertices(&self) -> usize;

    fn vertex_meta(&self) -> &Meta;

    fn edge_meta(&self) -> &Meta;

    fn graph_meta(&self) -> &GraphProps;

    fn get_layer_name(&self, layer_id: usize) -> ArcStr;

    fn get_layer_id(&self, name: &str) -> Option<usize>;

    /// Get the layer name for a given id
    fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> BoxedIter<ArcStr>;

    /// Returns the external ID for a vertex
    fn vertex_id(&self, v: VID) -> u64;

    /// Returns the string name for a vertex
    fn vertex_name(&self, v: VID) -> String;

    /// Get all the addition timestamps for an edge
    /// (this should always be global and not affected by windowing as deletion semantics may need information outside the current view!)
    fn edge_additions(
        &self,
        eref: EdgeRef,
        layer_ids: LayerIds,
    ) -> LockedLayeredIndex<'_, TimeIndexEntry>;

    /// Get all the addition timestamps for a vertex
    /// (this should always be global and not affected by windowing as deletion semantics may need information outside the current view!)
    fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex<i64>>;

    /// Gets the internal reference for an external vertex reference and keeps internal references unchanged.
    fn internalise_vertex(&self, v: VertexRef) -> Option<VID>;

    /// Gets the internal reference for an external vertex reference and keeps internal references unchanged. Assumes vertex exists!
    fn internalise_vertex_unchecked(&self, v: VertexRef) -> VID;

    /// Gets a static graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// Option<Prop> - The property value if it exists.
    fn constant_prop(&self, id: usize) -> Option<Prop>;

    /// Gets a temporal graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// Option<LockedView<TProp>> - The history of property values if it exists.
    fn temporal_prop(&self, id: usize) -> Option<LockedView<TProp>>;

    /// Gets a static property of a given vertex given the name and vertex reference.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the property is being queried.
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// Option<Prop> - The property value if it exists.
    fn constant_vertex_prop(&self, v: VID, id: usize) -> Option<Prop>;

    /// Gets the keys of constant properties of a given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the property is being queried.
    ///
    /// # Returns
    ///
    /// The keys of the constant properties.
    fn constant_vertex_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_>;

    /// Gets a temporal property of a given vertex given the name and vertex reference.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the property is being queried.
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// Option<LockedView<TProp>> - The history of property values if it exists.
    fn temporal_vertex_prop(&self, v: VID, id: usize) -> Option<LockedView<TProp>>;

    /// Returns a vector of all ids of temporal properties within the given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the names.
    ///
    /// # Returns
    ///
    /// the ids of the temporal properties
    fn temporal_vertex_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_>;

    /// Returns the static edge property with the given name for the
    /// given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    ///
    /// # Returns
    ///
    /// A property if it exists
    fn get_const_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: LayerIds) -> Option<Prop>;

    /// Returns a vector of keys for the static properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// # Returns
    ///
    /// the keys for the constant properties of the given edge.
    fn const_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_>;

    /// Returns a vector of all temporal values of the edge property with the given name for the
    /// given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    ///
    /// # Returns
    ///
    /// A property if it exists
    fn temporal_edge_prop(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: LayerIds,
    ) -> Option<LockedLayeredTProp>;

    /// Returns a vector of keys for the temporal properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// # Returns
    ///
    /// * keys for the temporal properties of the given edge.
    fn temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_>;

    fn core_edges(&self) -> Box<dyn Iterator<Item = ArcEntry<EdgeStore>>>;

    fn core_edge(&self, eid: EID) -> ArcEntry<EdgeStore>;
    fn core_vertices(&self) -> Box<dyn Iterator<Item = ArcEntry<VertexStore>>>;

    fn core_vertex(&self, vid: VID) -> ArcEntry<VertexStore>;
}

pub trait InheritCoreOps: Base {}

impl<G: InheritCoreOps> DelegateCoreOps for G
where
    G::Base: CoreGraphOps,
{
    type Internal = G::Base;

    #[inline]
    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateCoreOps {
    type Internal: CoreGraphOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateCoreOps + ?Sized> CoreGraphOps for G {
    #[inline]
    fn unfiltered_num_vertices(&self) -> usize {
        self.graph().unfiltered_num_vertices()
    }

    #[inline]
    fn vertex_meta(&self) -> &Meta {
        self.graph().vertex_meta()
    }

    #[inline]
    fn edge_meta(&self) -> &Meta {
        self.graph().edge_meta()
    }

    #[inline]
    fn graph_meta(&self) -> &GraphProps {
        self.graph().graph_meta()
    }

    #[inline]
    fn get_layer_name(&self, layer_id: usize) -> ArcStr {
        self.graph().get_layer_name(layer_id)
    }

    #[inline]
    fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.graph().get_layer_id(name)
    }

    #[inline]
    fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> BoxedIter<ArcStr> {
        self.graph().get_layer_names_from_ids(layer_ids)
    }

    #[inline]
    fn vertex_id(&self, v: VID) -> u64 {
        self.graph().vertex_id(v)
    }

    #[inline]
    fn vertex_name(&self, v: VID) -> String {
        self.graph().vertex_name(v)
    }

    #[inline]
    fn edge_additions(
        &self,
        eref: EdgeRef,
        layer_ids: LayerIds,
    ) -> LockedLayeredIndex<'_, TimeIndexEntry> {
        self.graph().edge_additions(eref, layer_ids)
    }

    #[inline]
    fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex<i64>> {
        self.graph().vertex_additions(v)
    }

    #[inline]
    fn internalise_vertex(&self, v: VertexRef) -> Option<VID> {
        self.graph().internalise_vertex(v)
    }

    #[inline]
    fn internalise_vertex_unchecked(&self, v: VertexRef) -> VID {
        self.graph().internalise_vertex_unchecked(v)
    }

    #[inline]
    fn constant_prop(&self, id: usize) -> Option<Prop> {
        self.graph().constant_prop(id)
    }

    #[inline]
    fn temporal_prop(&self, id: usize) -> Option<LockedView<TProp>> {
        self.graph().temporal_prop(id)
    }

    #[inline]
    fn constant_vertex_prop(&self, v: VID, id: usize) -> Option<Prop> {
        self.graph().constant_vertex_prop(v, id)
    }

    #[inline]
    fn constant_vertex_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph().constant_vertex_prop_ids(v)
    }

    #[inline]
    fn temporal_vertex_prop(&self, v: VID, id: usize) -> Option<LockedView<TProp>> {
        self.graph().temporal_vertex_prop(v, id)
    }

    #[inline]
    fn temporal_vertex_prop_ids(&self, v: VID) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph().temporal_vertex_prop_ids(v)
    }

    #[inline]
    fn get_const_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: LayerIds) -> Option<Prop> {
        self.graph().get_const_edge_prop(e, id, layer_ids)
    }

    #[inline]
    fn const_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph().const_edge_prop_ids(e, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: LayerIds,
    ) -> Option<LockedLayeredTProp> {
        self.graph().temporal_edge_prop(e, id, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_ids(
        &self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        self.graph().temporal_edge_prop_ids(e, layer_ids)
    }

    #[inline]
    fn core_edges(&self) -> Box<dyn Iterator<Item = ArcEntry<EdgeStore>>> {
        self.graph().core_edges()
    }

    #[inline]
    fn core_edge(&self, eid: EID) -> ArcEntry<EdgeStore> {
        self.graph().core_edge(eid)
    }

    #[inline]
    fn core_vertices(&self) -> Box<dyn Iterator<Item = ArcEntry<VertexStore>>> {
        self.graph().core_vertices()
    }

    #[inline]
    fn core_vertex(&self, vid: VID) -> ArcEntry<VertexStore> {
        self.graph().core_vertex(vid)
    }
}
