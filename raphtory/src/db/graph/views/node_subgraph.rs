use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            nodes::{node_ref::NodeRef, node_store::NodeStore},
            LayerIds, EID, VID,
        },
        Direction,
    },
    db::api::{
        properties::internal::InheritPropertiesOps,
        view::{
            internal::{
                Base, EdgeFilter, EdgeFilterOps, GraphOps, Immutable, InheritCoreOps,
                InheritEdgeFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritTimeSemantics, NodeFilterOps, Static, TimeSemantics,
            },
            BoxedLIter,
        },
    },
    prelude::GraphViewOps,
};
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Clone)]
pub struct NodeSubgraph<G> {
    pub(crate) graph: G,
    pub(crate) nodes: Arc<FxHashSet<VID>>,
}

impl<G> Static for NodeSubgraph<G> {}

impl<'graph, G: Debug + 'graph> Debug for NodeSubgraph<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSubgraph")
            .field("graph", &self.graph)
            .field("nodes", &self.nodes)
            .finish()
    }
}

impl<'graph, G: GraphViewOps<'graph>> Base for NodeSubgraph<G> {
    type Base = G;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> Immutable for NodeSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for NodeSubgraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeSubgraph<G> {
    pub fn new(graph: G, nodes: FxHashSet<VID>) -> Self {
        let nodes = Arc::new(nodes);
        Self { graph, nodes }
    }
}

// FIXME: this should use the list version ideally
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for NodeSubgraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for NodeSubgraph<G> {
    fn edges_filtered(&self) -> bool {
        true
    }

    fn edge_list_trusted(&self) -> bool {
        false
    }

    fn filter_edge(&self, edge: &EdgeStore, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge(edge, layer_ids)
            && self.nodes.contains(&edge.src)
            && self.nodes.contains(&edge.dst)
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for NodeSubgraph<G> {
    // FIXME: should use list version and make this true
    fn node_list_trusted(&self) -> bool {
        false
    }
    fn nodes_filtered(&self) -> bool {
        true
    }

    fn filter_node(&self, node: &NodeStore, layer_ids: &LayerIds) -> bool {
        self.graph.filter_node(node, layer_ids) && self.nodes.contains(&node.vid)
    }
}

impl<'graph, G: GraphViewOps<'graph> + 'graph> GraphOps<'graph> for NodeSubgraph<G> {
    fn internal_node_ref(&self, v: NodeRef, layer_ids: &LayerIds) -> Option<VID> {
        self.graph
            .internal_node_ref(v, layer_ids)
            .filter(|v| self.nodes.contains(v))
    }

    fn find_edge_id(
        &self,
        e_id: EID,
        layer_ids: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.graph
            .find_edge_id(e_id, layer_ids, filter)
            .filter(|e| self.nodes.contains(&e.src()) && self.nodes.contains(&e.dst()))
    }

    fn has_edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> bool {
        self.graph.has_edge_ref(src, dst, layer, filter)
    }

    fn has_node_ref(
        &self,
        v: NodeRef,
        layer_ids: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        self.internal_node_ref(v, layer_ids).is_some()
    }

    fn degree(&self, v: VID, d: Direction, layer: &LayerIds, filter: Option<&EdgeFilter>) -> usize {
        self.graph.degree(v, d, layer, filter)
    }

    fn node_ref(&self, v: u64, layers: &LayerIds, filter: Option<&EdgeFilter>) -> Option<VID> {
        self.internal_node_ref(v.into(), layers)
    }

    fn edge_ref(
        &self,
        src: VID,
        dst: VID,
        layer: &LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> Option<EdgeRef> {
        self.graph.edge_ref(src, dst, layer, filter)
    }

    fn node_refs(
        &self,
        _layers: LayerIds,
        _filter: Option<&EdgeFilter>,
    ) -> Box<dyn Iterator<Item = VID> + Send> {
        // this sucks but seems to be the only way currently (see also http://smallcultfollowing.com/babysteps/blog/2018/09/02/rust-pattern-iterating-an-over-a-rc-vec-t/)
        let verts = Vec::from_iter(self.nodes.iter().copied());
        Box::new(verts.into_iter())
    }

    fn edge_refs(
        &self,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        let g1 = self.graph.clone();
        let nodes = self.nodes.clone().iter().copied().collect_vec();
        let filter = filter.cloned();
        Box::new(
            nodes.into_iter().flat_map(move |v| {
                g1.node_edges(v, Direction::OUT, layer.clone(), filter.as_ref())
            }),
        )
    }

    fn node_edges(
        &self,
        v: VID,
        d: Direction,
        layer: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, EdgeRef> {
        self.graph.node_edges(v, d, layer, filter)
    }

    fn neighbours(
        &self,
        v: VID,
        d: Direction,
        layers: LayerIds,
        filter: Option<&EdgeFilter>,
    ) -> BoxedLIter<'graph, VID> {
        self.graph.neighbours(v, d, layers, filter)
    }
}

#[cfg(test)]
mod subgraph_tests {
    use crate::{algorithms::motifs::triangle_count::triangle_count, prelude::*};
    use itertools::Itertools;

    #[test]
    fn test_materialize_no_edges() {
        let g = Graph::new();

        g.add_node(1, 1, NO_PROPS, None).unwrap();
        g.add_node(2, 2, NO_PROPS, None).unwrap();
        let sg = g.subgraph([1, 2]);

        let actual = sg.materialize().unwrap().into_events().unwrap();
        assert_eq!(actual, sg);
    }

    #[test]
    fn test_remove_degree1_triangle_count() {
        let graph = Graph::new();
        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];
        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }
        let subgraph = graph.subgraph(graph.nodes().into_iter().filter(|v| v.degree() > 1));
        let ts = triangle_count(&subgraph, None);
        let tg = triangle_count(&graph, None);
        assert_eq!(ts, tg)
    }

    #[test]
    fn layer_materialize() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
        g.add_edge(0, 3, 4, NO_PROPS, Some("2")).unwrap();

        let sg = g.subgraph([1, 2]);
        let sgm = sg.materialize().unwrap();
        assert_eq!(
            sg.unique_layers().collect_vec(),
            sgm.unique_layers().collect_vec()
        );
    }
}
