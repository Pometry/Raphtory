use crate::{db::api::state::ops::NodeFilterOp, prelude::GraphViewOps};

pub trait InternalFilter<'graph> {
    type Graph: GraphViewOps<'graph> + 'graph;

    type Filtered<FilteredGraph: GraphViewOps<'graph> + 'graph>: InternalFilter<
        'graph,
        Graph = FilteredGraph,
    >;

    fn base_graph(&self) -> &Self::Graph;

    fn apply_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::Filtered<FilteredGraph>;
}

pub trait InternalNodeSelect<'graph> {
    type IterGraph: GraphViewOps<'graph> + 'graph;

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
    type IterGraph: GraphViewOps<'graph> + 'graph;

    type IterFiltered<FilteredGraph: GraphViewOps<'graph> + 'graph>: InternalEdgeSelect<
        'graph,
        IterGraph = Self::IterGraph,
    >;

    fn iter_graph(&self) -> &Self::IterGraph;

    fn apply_iter_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::IterFiltered<FilteredGraph>;
}
