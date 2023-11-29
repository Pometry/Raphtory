use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, vertices::vertex_ref::VertexRef, LayerIds, EID, VID},
        Direction,
    },
    db::api::view::{
        internal::{Base, EdgeFilter},
        BoxedLIter,
    },
};

/// The GraphViewInternalOps trait provides a set of methods to query a directed graph
/// represented by the raphtory_core::tgraph::TGraph struct.
pub trait GraphOps<'graph>: Send + Sync {
    /// Check if a vertex exists and returns internal reference.
    fn internal_vertex_ref(
        &self,
        v: VertexRef,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<VID>;

    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef>;

    /// Returns the total number of vertices in the graph.
    fn vertices_len(&self, layer_ids: LayerIds, filter: Option<&EdgeFilter>) -> usize;

    /// Returns the total number of edges in the graph.
    fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize;

    fn temporal_edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize;

    /// Returns true if the graph contains an edge between the source vertex
    /// (src) and the destination vertex (dst).
    /// # Arguments
    ///
    /// * `src` - The source vertex of the edge.
    /// * `dst` - The destination vertex of the edge.
    fn has_edge_ref(
        &self,
        src: VID,
        dst: VID,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> bool {
        self.edge_ref(src, dst, layers, filter).is_some()
    }

    /// Returns true if the graph contains the specified vertex (v).
    /// # Arguments
    ///
    /// * `v` - VertexRef of the vertex to check.
    #[inline]
    fn has_vertex_ref(&self, v: VertexRef, layers: &LayerIds, filter: Option<&EdgeFilter>) -> bool {
        self.internal_vertex_ref(v, layers, filter).is_some()
    }

    /// Returns the number of edges that point towards or from the specified vertex
    /// (v) based on the direction (d).
    /// # Arguments
    ///
    /// * `v` - VID of the vertex to check.
    /// * `d` - Direction of the edges to count.
    fn degree(&self, v: VID, d: Direction, layers: &LayerIds, filter: Option<&EdgeFilter>)
        -> usize;

    /// Returns the VID that corresponds to the specified vertex ID (v).
    /// Returns None if the vertex ID is not present in the graph.
    /// # Arguments
    ///
    /// * `v` - The vertex ID to lookup.
    #[inline]
    fn vertex_ref(&self, v: u64, layers: &LayerIds, filter: Option<&EdgeFilter>) -> Option<VID> {
        self.internal_vertex_ref(v.into(), layers, filter)
    }

    /// Returns the edge reference that corresponds to the specified src and dst vertex
    /// # Arguments
    ///
    /// * `src` - The source vertex.
    /// * `dst` - The destination vertex.
    ///
    /// Returns:
    ///
    /// * `Option<EdgeRef>` - The edge reference if it exists.
    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef>;

    /// Returns all the vertex references in the graph.
    /// Returns:
    /// * `Box<dyn Iterator<Item = VID> + Send>` - An iterator over all the vertex
    /// references in the graph.
    fn vertex_refs(&self, layers: LayerIds, filter: Option<&EdgeFilter>)
        -> BoxedLIter<'graph, VID>;

    /// Returns all the edge references in the graph.
    ///
    /// Returns:
    ///
    /// * `Box<dyn Iterator<Item = EdgeRef> + Send>` - An iterator over all the edge references.
    fn edge_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef>;

    /// Returns an iterator over the edges connected to a given vertex in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the edges are being queried.
    /// * `d` - The direction in which to search for edges.
    /// * `layer` - The optional layer to consider
    ///
    /// Returns:
    ///
    /// Box<dyn Iterator<Item = EdgeRef> + Send> -  A boxed iterator that yields references to
    /// the edges connected to the vertex.
    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef>;

    /// Returns an iterator over the neighbors of a given vertex in a given direction.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the vertex for which the neighbors are being queried.
    /// * `d` - The direction in which to search for neighbors.
    ///
    /// Returns:
    ///
    /// A boxed iterator that yields references to the neighboring vertices.
    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, VID>;
}

pub trait InheritGraphOps: Base + Send + Sync {}

impl<'base: 'graph, 'graph, G: InheritGraphOps + Send + Sync + ?Sized + 'graph> GraphOps<'graph>
    for G
where
    G::Base: GraphOps<'base>,
{
    #[inline]
    fn internal_vertex_ref(
        &self,
        v: VertexRef,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<VID> {
        self.base().internal_vertex_ref(v, layer_ids, filter)
    }

    #[inline]
    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.base().find_edge_id(e_id, layer_ids, filter)
    }

    #[inline]
    fn vertices_len(&self, layer_ids: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.base().vertices_len(layer_ids, filter)
    }

    #[inline]
    fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.base().edges_len(layers, filter)
    }

    #[inline]
    fn temporal_edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.base().temporal_edges_len(layers, filter)
    }

    #[inline]
    fn has_edge_ref(
        &self,
        src: VID,
        dst: VID,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> bool {
        self.base().has_edge_ref(src, dst, layers, filter)
    }

    #[inline]
    fn has_vertex_ref(&self, v: VertexRef, layers: &LayerIds, filter: Option<&EdgeFilter>) -> bool {
        self.base().has_vertex_ref(v, layers, filter)
    }

    #[inline]
    fn degree(
        &self,
        v: VID,
        d: Direction,
        layers: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> usize {
        self.base().degree(v, d, layers, filter)
    }

    #[inline]
    fn vertex_ref(&self, v: u64, layers: &LayerIds, filter: Option<&EdgeFilter>) -> Option<VID> {
        self.base().vertex_ref(v, layers, filter)
    }

    #[inline]
    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.base().edge_ref(src, dst, layer, filter)
    }

    #[inline]
    fn vertex_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, VID> {
        self.base().vertex_refs(layers, filter)
    }

    #[inline]
    fn edge_refs(
        &self,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        self.base().edge_refs(layers, filter)
    }

    #[inline]
    fn vertex_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        self.base().vertex_edges(v, d, layer, filter)
    }

    #[inline]
    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, VID> {
        self.base().neighbours(v, d, layers, filter)
    }
}
