use crate::{
    core::entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
    db::{
        api::{
            properties::Properties,
            view::{
                internal::{InternalLayerOps, OneHopFilter},
                BaseEdgeViewOps, BoxedLIter,
            },
        },
        graph::edge::EdgeView,
    },
    prelude::{EdgeViewOps, GraphViewOps, Layer},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Edges<'graph, G, GH = G> {
    base_graph: G,
    graph: GH,
    edges: Arc<dyn Fn(&GH) -> BoxedLIter<'graph, EdgeRef> + Send + Sync + 'graph>,
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> OneHopFilter<'graph>
    for Edges<'graph, G, GH>
{
    type Graph = G;
    type Filtered<GHH: GraphViewOps<'graph> + 'graph> = Edges<'graph, G, GHH>;

    fn current_filter(&self) -> &Self::Graph {
        &self.graph
    }

    fn one_hop_filtered<GHH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GHH,
    ) -> Self::Filtered<GHH> {
        let base_graph = self.base_graph.clone();
        let edges = self.edges.clone();
        Edges {
            base_graph,
            graph: filtered_graph,
            edges,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> InternalLayerOps
    for Edges<'graph, G, GH>
{
    fn layer_ids(&self) -> LayerIds {
        self.graph.layer_ids()
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        self.graph.layer_ids_from_names(key)
    }
}

impl<'graph, G: GraphViewOps<'graph>, GH: GraphViewOps<'graph>> BaseEdgeViewOps<'graph>
    for Edges<'graph, G, GH>
{
    type BaseGraph = G;
    type Graph = GH;
    type ValueType<T> = BoxedLIter<'graph, T> where T: 'graph;
    type PropType = EdgeView<GH>;
    type Nodes = ();
    type Exploded = ();

    fn map<O: 'graph, F: Fn(&Self::Graph, EdgeRef) -> O + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::ValueType<O> {
        todo!()
    }

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>> {
        todo!()
    }

    fn map_nodes<F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> VID + Send + Sync + Clone + 'graph>(
        &self,
        op: F,
    ) -> Self::Nodes {
        todo!()
    }

    fn map_exploded<
        I: Iterator<Item = EdgeRef> + Send + 'graph,
        F: for<'a> Fn(&'a Self::Graph, EdgeRef) -> I + Send + Sync + Clone + 'graph,
    >(
        &self,
        op: F,
    ) -> Self::Exploded {
        todo!()
    }
}

#[derive(Clone)]
pub struct NestedEdges<'graph, G, GH = G> {
    base_graph: G,
    graph: GH,
    edges:
        Arc<dyn Fn(&GH) -> BoxedLIter<'graph, BoxedLIter<'graph, EdgeRef>> + Send + Sync + 'graph>,
}
