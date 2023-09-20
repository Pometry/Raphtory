use raphtory::core::Direction;
use raphtory::core::entities::edges::edge_ref::EdgeRef;
use raphtory::core::entities::{LayerIds, VID, EID};
use raphtory::core::entities::vertices::vertex_ref::VertexRef;
use raphtory::db::api::view::internal::{GraphOps, EdgeFilter};

use crate::TemporalColumnarGraph;

// impl GraphOps for TemporalColumnarGraph {
//     #[doc = " Check if a vertex exists and returns internal reference."]
//     fn internal_vertex_ref(
//         &self,
//         v: VertexRef,
//         layer_ids: &LayerIds,
//         filter: Option<&EdgeFilter>,
//     ) -> Option<VID> {
//         todo!()
//     }

//     fn find_edge_id(
//         &self,
//         e_id: EID,
//         layer_ids: &LayerIds,
//         filter: Option<&EdgeFilter>,
//     ) -> Option<EdgeRef> {
//         todo!()
//     }

//     #[doc = " Returns the total number of vertices in the graph."]
//     fn vertices_len(&self, layer_ids: LayerIds, filter: Option<&EdgeFilter>) -> usize {
//         todo!()
//     }

//     #[doc = " Returns the total number of edges in the graph."]
//     fn edges_len(&self, layers: LayerIds, filter: Option<&EdgeFilter>) -> usize {
//         todo!()
//     }

//     #[doc = " Returns the number of edges that point towards or from the specified vertex"]
//     #[doc = " (v) based on the direction (d)."]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - VID of the vertex to check."]
//     #[doc = " * `d` - Direction of the edges to count."]
//     fn degree(
//         &self,
//         v: VID,
//         d: Direction,
//         layers: &LayerIds,
//         filter: Option<&EdgeFilter>,
//     ) -> usize {
//         todo!()
//     }

//     #[doc = " Returns all the vertex references in the graph."]
//     #[doc = " # Returns"]
//     #[doc = " * `Box<dyn Iterator<Item = VID> + Send>` - An iterator over all the vertex"]
//     #[doc = " references in the graph."]
//     fn vertex_refs(
//         &self,
//         layers: LayerIds,
//         filter: Option<&EdgeFilter>,
//     ) -> Box<dyn Iterator<Item = VID> + Send> {
//         todo!()
//     }

//     #[doc = " Returns the edge reference that corresponds to the specified src and dst vertex"]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `src` - The source vertex."]
//     #[doc = " * `dst` - The destination vertex."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " * `Option<EdgeRef>` - The edge reference if it exists."]
//     fn edge_ref(
//         &self,
//         src: VID,
//         dst: VID,
//         layer: &LayerIds,
//         filter: Option<&EdgeFilter>,
//     ) -> Option<EdgeRef> {
//         todo!()
//     }

//     #[doc = " Returns all the edge references in the graph."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " * `Box<dyn Iterator<Item = EdgeRef> + Send>` - An iterator over all the edge references."]
//     fn edge_refs(
//         &self,
//         layers: LayerIds,
//         filter: Option<&EdgeFilter>,
//     ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     #[doc = " Returns an iterator over the edges connected to a given vertex in a given direction."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - A reference to the vertex for which the edges are being queried."]
//     #[doc = " * `d` - The direction in which to search for edges."]
//     #[doc = " * `layer` - The optional layer to consider"]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " Box<dyn Iterator<Item = EdgeRef> + Send> -  A boxed iterator that yields references to"]
//     #[doc = " the edges connected to the vertex."]
//     fn vertex_edges(
//         &self,
//         v: VID,
//         d: Direction,
//         layer: LayerIds,
//         filter: Option<&EdgeFilter>,
//     ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
//         todo!()
//     }

//     #[doc = " Returns an iterator over the neighbors of a given vertex in a given direction."]
//     #[doc = ""]
//     #[doc = " # Arguments"]
//     #[doc = ""]
//     #[doc = " * `v` - A reference to the vertex for which the neighbors are being queried."]
//     #[doc = " * `d` - The direction in which to search for neighbors."]
//     #[doc = ""]
//     #[doc = " # Returns"]
//     #[doc = ""]
//     #[doc = " A boxed iterator that yields references to the neighboring vertices."]
//     fn neighbours(
//         &self,
//         v: VID,
//         d: Direction,
//         layers: LayerIds,
//         filter: Option<&EdgeFilter>,
//     ) -> Box<dyn Iterator<Item = VID> + Send> {
//         todo!()
//     }
// }
