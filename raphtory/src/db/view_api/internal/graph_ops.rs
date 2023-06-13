use crate::core::edge_ref::EdgeRef;
use crate::core::timeindex::{TimeIndex, TimeIndexOps, TimeIndexWindow};
use crate::core::tprop::TProp;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use crate::db::view_api::internal::time_semantics::TimeSemantics;
use crate::db::view_api::BoxedIter;
use std::collections::HashMap;
use std::ops::Range;

/// The GraphViewInternalOps trait provides a set of methods to query a directed graph
/// represented by the raphtory_core::tgraph::TGraph struct.
pub trait GraphViewInternalOps: Send + Sync {
    /// Check if a vertex exists locally and returns local reference.
    fn local_vertex_ref(&self, v: VertexRef) -> Option<LocalVertexRef>;

    /// Get all layer ids
    fn get_unique_layers_internal(&self) -> Vec<usize>;

    /// Get the layer name for a given id
    fn get_layer_name_by_id(&self, layer_id: usize) -> String;

    /// Get the layer id for the given layer name
    fn get_layer_id(&self, key: Option<&str>) -> Option<usize>;

    /// Returns the total number of vertices in the graph.
    fn vertices_len(&self) -> usize;

    /// Returns the total number of edges in the graph.
    fn edges_len(&self, layer: Option<usize>) -> usize;

    /// Returns true if the graph contains an edge between the source vertex
    /// (src) and the destination vertex (dst).
    /// # Arguments
    ///
    /// * `src` - The source vertex of the edge.
    /// * `dst` - The destination vertex of the edge.
    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> bool {
        self.edge_ref(src, dst, layer).is_some()
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
    /// * `v` - LocalVertexRef of the vertex to check.
    /// * `d` - Direction of the edges to count.
    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize;

    /// Returns the LocalVertexRef that corresponds to the specified vertex ID (v).
    /// Returns None if the vertex ID is not present in the graph.
    /// # Arguments
    ///
    /// * `v` - The vertex ID to lookup.
    fn vertex_ref(&self, v: u64) -> Option<LocalVertexRef> {
        self.local_vertex_ref(v.into())
    }

    /// Returns the global ID for a vertex
    fn vertex_id(&self, v: LocalVertexRef) -> u64;

    /// Returns the string name for a vertex
    fn vertex_name(&self, v: LocalVertexRef) -> String {
        match self.static_vertex_prop(v, "_id".to_string()) {
            None => self.vertex_id(v).to_string(),
            Some(prop) => prop.to_string(),
        }
    }

    /// Returns all the vertex references in the graph.
    /// # Returns
    /// * `Box<dyn Iterator<Item = LocalVertexRef> + Send>` - An iterator over all the vertex
    /// references in the graph.
    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send>;

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send>;

    /// Returns the edge reference that corresponds to the specified src and dst vertex
    /// # Arguments
    ///
    /// * `src` - The source vertex.
    /// * `dst` - The destination vertex.
    ///
    /// # Returns
    ///
    /// * `Option<EdgeRef>` - The edge reference if it exists.
    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef>;

    /// Returns all the edge references in the graph.
    ///
    /// # Returns
    ///
    /// * `Box<dyn Iterator<Item = EdgeRef> + Send>` - An iterator over all the edge references.
    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

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
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
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
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;
}

pub trait InheritInternalViewOps {
    type Internal: GraphViewInternalOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: InheritInternalViewOps + Send + Sync> GraphViewInternalOps for G {
    fn local_vertex_ref(&self, v: VertexRef) -> Option<LocalVertexRef> {
        todo!()
    }

    fn get_unique_layers_internal(&self) -> Vec<usize> {
        todo!()
    }

    fn get_layer_name_by_id(&self, layer_id: usize) -> String {
        todo!()
    }

    fn get_layer_id(&self, key: Option<&str>) -> Option<usize> {
        todo!()
    }

    fn vertices_len(&self) -> usize {
        todo!()
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        todo!()
    }

    fn degree(&self, v: LocalVertexRef, d: Direction, layer: Option<usize>) -> usize {
        todo!()
    }

    fn vertex_id(&self, v: LocalVertexRef) -> u64 {
        todo!()
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        todo!()
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = LocalVertexRef> + Send> {
        todo!()
    }

    fn edge_ref(&self, src: VertexRef, dst: VertexRef, layer: usize) -> Option<EdgeRef> {
        todo!()
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        todo!()
    }

    fn vertex_edges(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        todo!()
    }

    fn neighbours(
        &self,
        v: LocalVertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        todo!()
    }

    fn static_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<Prop> {
        todo!()
    }

    fn static_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        todo!()
    }

    fn temporal_vertex_prop(&self, v: LocalVertexRef, name: String) -> Option<&TProp> {
        todo!()
    }

    fn temporal_vertex_prop_names(&self, v: LocalVertexRef) -> Vec<String> {
        todo!()
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop> {
        todo!()
    }

    fn static_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        todo!()
    }

    fn temporal_edge_prop(&self, e: EdgeRef, name: String) -> Option<&TProp> {
        todo!()
    }

    fn temporal_edge_prop_names(&self, e: EdgeRef) -> Vec<String> {
        todo!()
    }

    fn num_shards(&self) -> usize {
        todo!()
    }
}
