use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            properties::tprop::{LockedLayeredTProp, TProp},
            vertices::{vertex_ref::VertexRef, vertex_store::VertexStore},
            LayerIds, EID, VID,
        },
        storage::{
            locked_view::LockedView,
            timeindex::{LockedLayeredIndex, TimeIndex, TimeIndexEntry},
            ArcEntry,
        },
        Prop,
    },
    db::api::view::internal::Base,
};
use enum_dispatch::enum_dispatch;

/// Core functions that should (almost-)always be implemented by pointing at the underlying graph.
#[enum_dispatch]
pub trait CoreGraphOps {
    /// get the number of vertices in the main graph
    fn unfiltered_num_vertices(&self) -> usize;

    fn get_layer_name(&self, layer_id: usize) -> Option<LockedView<String>>;

    fn get_layer_id(&self, name: &str) -> Option<usize>;

    /// Get the layer name for a given id
    fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> Vec<String>;

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

    /// Lists the keys of all static properties of the graph
    ///
    /// # Returns
    ///
    /// Vec<String> - The keys of the static properties.
    fn static_prop_names(&self) -> Vec<String>;

    /// Gets a static graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// Option<Prop> - The property value if it exists.
    fn static_prop(&self, name: &str) -> Option<Prop>;

    /// Lists the keys of all temporal properties of the graph
    ///
    /// # Returns
    ///
    /// Vec<String> - The keys of the static properties.
    fn temporal_prop_names(&self) -> Vec<String>;

    /// Gets a temporal graph property.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property.
    ///
    /// # Returns
    ///
    /// Option<LockedView<TProp>> - The history of property values if it exists.
    fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>>;

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
    fn static_vertex_prop(&self, v: VID, name: &str) -> Option<Prop>;

    /// Gets the keys of static properties of a given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the property is being queried.
    ///
    /// # Returns
    ///
    /// Vec<String> - The keys of the static properties.
    fn static_vertex_prop_names<'a>(
        &'a self,
        v: VID,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a>;

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
    fn temporal_vertex_prop(&self, v: VID, name: &str) -> Option<LockedView<TProp>>;

    /// Returns a vector of all names of temporal properties within the given vertex
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which to retrieve the names.
    ///
    /// # Returns
    ///
    /// A vector of strings representing the names of the temporal properties
    fn temporal_vertex_prop_names<'a>(
        &'a self,
        v: VID,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a>;

    /// Returns a vector of all names of temporal properties that exist on at least one vertex
    ///
    /// # Returns
    ///
    /// A vector of strings representing the names of the temporal properties
    fn all_vertex_prop_names(&self, is_static: bool) -> Vec<String>;

    /// Returns a vector of all names of temporal properties that exist on at least one vertex
    ///
    /// # Returns
    ///
    /// A vector of strings representing the names of the temporal properties
    fn all_edge_prop_names(&self, is_static: bool) -> Vec<String>;
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
    fn static_edge_prop(&self, e: EdgeRef, name: &str, layer_ids: LayerIds) -> Option<Prop>;

    /// Returns a vector of keys for the static properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// # Returns
    ///
    /// * A `Vec` of `String` containing the keys for the static properties of the given edge.
    fn static_edge_prop_names<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a>;

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
        name: &str,
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
    /// * A `Vec` of `String` containing the keys for the temporal properties of the given edge.
    fn temporal_edge_prop_names<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a>;

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
    fn get_layer_name(&self, layer_id: usize) -> Option<LockedView<String>> {
        self.graph().get_layer_name(layer_id)
    }

    #[inline]
    fn get_layer_id(&self, name: &str) -> Option<usize> {
        self.graph().get_layer_id(name)
    }

    #[inline]
    fn get_layer_names_from_ids(&self, layer_ids: LayerIds) -> Vec<String> {
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
    fn static_prop_names(&self) -> Vec<String> {
        self.graph().static_prop_names()
    }

    #[inline]
    fn static_prop(&self, name: &str) -> Option<Prop> {
        self.graph().static_prop(name)
    }

    #[inline]
    fn temporal_prop_names(&self) -> Vec<String> {
        self.graph().temporal_prop_names()
    }

    #[inline]
    fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        self.graph().temporal_prop(name)
    }

    #[inline]
    fn static_vertex_prop(&self, v: VID, name: &str) -> Option<Prop> {
        self.graph().static_vertex_prop(v, name)
    }

    #[inline]
    fn static_vertex_prop_names<'a>(
        &'a self,
        v: VID,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        self.graph().static_vertex_prop_names(v)
    }

    #[inline]
    fn temporal_vertex_prop(&self, v: VID, name: &str) -> Option<LockedView<TProp>> {
        self.graph().temporal_vertex_prop(v, name)
    }

    #[inline]
    fn temporal_vertex_prop_names<'a>(
        &'a self,
        v: VID,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        self.graph().temporal_vertex_prop_names(v)
    }

    #[inline]
    fn all_vertex_prop_names(&self, is_static: bool) -> Vec<String> {
        self.graph().all_vertex_prop_names(is_static)
    }

    #[inline]
    fn all_edge_prop_names(&self, is_static: bool) -> Vec<String> {
        self.graph().all_edge_prop_names(is_static)
    }

    #[inline]
    fn static_edge_prop(&self, e: EdgeRef, name: &str, layer_ids: LayerIds) -> Option<Prop> {
        self.graph().static_edge_prop(e, name, layer_ids)
    }

    #[inline]
    fn static_edge_prop_names<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        self.graph().static_edge_prop_names(e, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop(
        &self,
        e: EdgeRef,
        name: &str,
        layer_ids: LayerIds,
    ) -> Option<LockedLayeredTProp> {
        self.graph().temporal_edge_prop(e, name, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_names<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: LayerIds,
    ) -> Box<dyn Iterator<Item = LockedView<'a, String>> + 'a> {
        self.graph().temporal_edge_prop_names(e, layer_ids)
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
