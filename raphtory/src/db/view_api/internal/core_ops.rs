use crate::core::edge_ref::EdgeRef;
use crate::core::tgraph2::VID;
use crate::core::locked_view::LockedView;
use crate::core::timeindex::TimeIndex;
use crate::core::tprop::TProp;
use crate::core::vertex_ref::VertexRef;
use crate::core::Prop;
use crate::db::view_api::internal::Base;

/// Core functions that should (almost-)always be implemented by pointing at the underlying graph.
pub trait CoreGraphOps {
    /// Get the layer name for a given id
    fn get_layer_name_by_id(&self, layer_id: usize) -> String;

    /// Returns the global ID for a vertex
    fn vertex_id(&self, v: VID) -> u64;

    /// Returns the string name for a vertex
    fn vertex_name(&self, v: VID) -> String;

    /// Get all the addition timestamps for an edge
    /// (this should always be global and not affected by windowing as deletion semantics may need information outside the current view!)
    fn edge_additions(&self, eref: EdgeRef) -> LockedView<TimeIndex>;

    /// Get all the addition timestamps for a vertex
    /// (this should always be global and not affected by windowing as deletion semantics may need information outside the current view!)
    fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex>;

    /// Gets the local reference for a remote vertex and keeps local references unchanged. Assumes vertex exists!
    fn localise_vertex_unchecked(&self, v: VertexRef) -> VID;

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
    fn static_vertex_prop_names(&self, v: VID) -> Vec<String>;

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
    fn temporal_vertex_prop_names(&self, v: VID) -> Vec<String>;

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
    fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop>;

    /// Returns a vector of keys for the static properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// # Returns
    ///
    /// * A `Vec` of `String` containing the keys for the static properties of the given edge.
    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String>;

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
    fn temporal_edge_prop(&self, e: EdgeRef, name: &str) -> Option<LockedView<TProp>>;

    /// Returns a vector of keys for the temporal properties of the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    ///
    /// # Returns
    ///
    /// * A `Vec` of `String` containing the keys for the temporal properties of the given edge.
    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String>;

}

pub trait InheritCoreOps: Base {}

impl<G: InheritCoreOps> DelegateCoreOps for G
where
    G::Base: CoreGraphOps,
{
    type Internal = G::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateCoreOps {
    type Internal: CoreGraphOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateCoreOps + ?Sized> CoreGraphOps for G {
    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        self.graph().get_layer_name_by_id(layer_id)
    }

    fn vertex_id(&self, v: VID) -> u64 {
        self.graph().vertex_id(v)
    }

    fn vertex_name(&self, v: VID) -> String {
        self.graph().vertex_name(v)
    }

    fn edge_additions(&self, eref: EdgeRef) -> LockedView<TimeIndex>{
        self.graph().edge_additions(eref)
    }

    fn vertex_additions(&self, v: VID) -> LockedView<TimeIndex> {
        self.graph().vertex_additions(v)
    }

    fn localise_vertex_unchecked(&self, v: VertexRef) -> VID {
        self.graph().localise_vertex_unchecked(v)
    }

    fn static_prop_names(&self) -> Vec<String> {
        self.graph().static_prop_names()
    }

    fn static_prop(&self, name: &str) -> Option<Prop> {
        self.graph().static_prop(name)
    }

    fn temporal_prop_names(&self) -> Vec<String> {
        self.graph().temporal_prop_names()
    }

    fn temporal_prop(&self, name: &str) -> Option<LockedView<TProp>> {
        self.graph().temporal_prop(name)
    }

    fn static_vertex_prop(&self, v: VID, name: &str) -> Option<Prop> {
        self.graph().static_vertex_prop(v, name)
    }

    fn static_vertex_prop_names(&self, v: VID) -> Vec<String> {
        self.graph().static_vertex_prop_names(v)
    }

    fn temporal_vertex_prop(&self, v: VID, name: &str) -> Option<LockedView<TProp>> {
        self.graph().temporal_vertex_prop(v, name)
    }

    fn temporal_vertex_prop_names(&self, v: VID) -> Vec<String> {
        self.graph().temporal_vertex_prop_names(v)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: &str) -> Option<Prop> {
        self.graph().static_edge_prop(e, name)
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph().static_edge_prop_names(e)
    }

    fn temporal_edge_prop(&self, e: EdgeRef, name: &str) -> Option<LockedView<TProp>> {
        self.graph().temporal_edge_prop(e, name)
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        self.graph().temporal_edge_prop_names(e)
    }

}
