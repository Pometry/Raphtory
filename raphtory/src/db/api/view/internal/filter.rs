use crate::{
    db::api::{state::ops::NodeFilterOp, view::internal::GraphView},
    prelude::GraphViewOps,
};

pub trait InternalFilter<'graph> {
    type Graph: GraphView + 'graph;

    type Filtered<FilteredGraph: GraphView + 'graph>: InternalFilter<'graph, Graph = FilteredGraph>;

    fn base_graph(&self) -> &Self::Graph;

    fn apply_filter<FilteredGraph: GraphView + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::Filtered<FilteredGraph>;
}

pub trait InternalNodeSelect<'graph> {
    type IterGraph: GraphView + 'graph;

    type IterFiltered<Filter: NodeFilterOp + 'graph>: InternalNodeSelect<
        'graph,
        IterGraph = Self::IterGraph,
    >;

    fn iter_graph(&self) -> &Self::IterGraph;

    fn apply_iter_filter<Filter: NodeFilterOp + 'graph>(
        &self,
        filter: Filter,
    ) -> Self::IterFiltered<Filter>;
}

pub trait InternalEdgeSelect<'graph> {
    type IterGraph: GraphView + 'graph;

    type IterFiltered<FilteredGraph: GraphView + 'graph>: InternalEdgeSelect<
        'graph,
        IterGraph = Self::IterGraph,
    >;

    fn iter_graph(&self) -> &Self::IterGraph;

    fn apply_iter_filter<FilteredGraph: GraphView + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::IterFiltered<FilteredGraph>;
}
