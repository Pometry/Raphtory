use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, VID, EID, LayerIds},
        Direction,
    },
    db::api::view::{internal::Base, Layer},
};

/// The GraphViewInternalOps trait provides a set of methods to query a directed graph
/// represented by the raphtory_core::tgraph::TGraph struct.
pub trait GraphOps: Send + Sync {
    /// Check if a vertex exists locally and returns local reference.
    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID>;

    fn find_edge_id(&self, e_id: EID) -> Option<EdgeRef>;

    /// Get all layer ids
    fn get_unique_layers_internal(&self) -> Vec<usize>;

    /// Get the layer id for the given layer name
    fn get_layer_id(&self, key: Layer) -> Option<LayerIds>;

    /// get the layer ids for the given edge id
    fn get_layer_ids(&self, e_id: EID) -> Option<LayerIds>;

    /// Returns the total number of vertices in the graph.
    fn vertices_len(&self) -> usize;

    /// Returns the total number of edges in the graph.
    fn edges_len(&self, layers: LayerIds) -> usize;

    /// Returns true if the graph contains an edge between the source vertex
    /// (src) and the destination vertex (dst).
    /// # Arguments
    ///
    /// * `src` - The source vertex of the edge.
    /// * `dst` - The destination vertex of the edge.
    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layers: LayerIds) -> bool {
        self.edge_ref(src, dst, layers).is_some()
    }

    /// Returns true if the graph contains the specified vertex (v).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.local_vertex_ref(v).is_some()
    }

    /// Returns the number of edges that point towards or from the specified vertex
    /// (v) based on the direction (d).
    /// # Arguments
    ///
    /// * `v` - VID of the vertex to check.
    /// * `d` - Direction of the edges to count.
    fn degree(&self, v: VID, d: Direction, layers: LayerIds) -> usize;

    /// Returns the VID that corresponds to the specified vertex ID (v).
    /// Returns None if the vertex ID is not present in the graph.
    /// # Arguments
    ///
    /// * `v` - The vertex ID to lookup.
    fn vertex_ref(&self, v: u64) -> Option<VID> {
        self.local_vertex_ref(v.into())
    }

    /// Returns all the vertex references in the graph.
    /// # Returns
    /// * `Box<dyn Iterator<Item = VID> + Send>` - An iterator over all the vertex
    /// references in the graph.
    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send>;

    /// Returns the edge reference that corresponds to the specified src and dst vertex
    /// # Arguments
    ///
    /// * `src` - The source vertex.
    /// * `dst` - The destination vertex.
    ///
    /// # Returns
    ///
    /// * `Option<EdgeRef>` - The edge reference if it exists.
    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: LayerIds) -> Option<EdgeRef>;

    /// Returns all the edge references in the graph.
    ///
    /// # Returns
    ///
    /// * `Box<dyn Iterator<Item = EdgeRef> + Send>` - An iterator over all the edge references.
    fn edge_refs(&self, layers: LayerIds) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns an iterator over the edges connected to a given vertex in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the edges are being queried.
    /// * `d` - The direction in which to search for edges.
    /// * `layer` - The optional layer to consider
    ///
    /// # Returns
    ///
    /// Box<dyn Iterator<Item = EdgeRef> + Send> -  A boxed iterator that yields references to
    /// the edges connected to the vertex.
    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    /// Returns an iterator over the neighbors of a given vertex in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the neighbors are being queried.
    /// * `d` - The direction in which to search for neighbors.
    ///
    /// # Returns
    ///
    /// A boxed iterator that yields references to the neighboring vertices.
    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;
}

pub trait InheritGraphOps: Base {}

impl<G: InheritGraphOps> DelegateGraphOps for G
where
    G::Base: GraphOps,
{
    type Internal = G::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateGraphOps {
    type Internal: GraphOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateGraphOps + Send + Sync + ?Sized> GraphOps for G {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<VID> {
        self.graph().local_vertex_ref(v)
    }

    fn find_edge_id(&self, e_id: EID) -> Option<EdgeRef> {
        self.graph().find_edge_id(e_id)
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        self.graph().get_unique_layers_internal()
    }

    fn get_layer_id(&self, key: Layer) -> Option<LayerIds> {
        self.graph().get_layer_id(key)
    }

    fn vertices_len(&self) -> usize {
        self.graph().vertices_len()
    }

    fn edges_len(&self, layers: LayerIds) -> usize {
        self.graph().edges_len(layers)
    }

    fn degree(&self, v: VID, d: Direction, layers: LayerIds) -> usize {
        self.graph().degree(v, d, layers)
    }
    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VID> + Send> {
        self.graph().vertex_refs()
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layers: LayerIds) -> Option<EdgeRef> {
        self.graph().edge_ref(src, dst, layers)
    }

    fn edge_refs(&self, layers: LayerIds) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph().edge_refs(layers)
    }

    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        self.graph().vertex_edges(v, d, layers)
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph().neighbours(v, d, layer)
    }

    fn get_layer_ids(&self, e_id: EID) -> Option<LayerIds> {
        self.graph().get_layer_ids(e_id)
    }
}
