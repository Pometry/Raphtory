use crate::prelude::GraphViewOps;

pub trait Filter<'graph> {
    type Graph: GraphViewOps<'graph> + 'graph;

    type Filtered<FilteredGraph: GraphViewOps<'graph> + 'graph>: Filter<
        'graph,
        Graph = FilteredGraph,
    >;

    fn base_graph(&self) -> &Self::Graph;

    fn apply_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::Filtered<FilteredGraph>;
}

pub trait IterFilter<'graph> {
    type IterGraph: GraphViewOps<'graph> + 'graph;

    type IterFiltered<FilteredGraph: GraphViewOps<'graph> + 'graph>: IterFilter<'graph>;

    fn iter_graph(&self) -> &Self::IterGraph;

    fn apply_iter_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::IterFiltered<FilteredGraph>;
}
