use crate::prelude::GraphViewOps;

pub trait BaseFilter<'graph> {
    type BaseGraph: GraphViewOps<'graph> + 'graph;

    type Filtered<FilteredGraph: GraphViewOps<'graph> + 'graph>: BaseFilter<
        'graph,
        BaseGraph = FilteredGraph,
    >;

    fn base_graph(&self) -> &Self::BaseGraph;

    fn apply_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::Filtered<FilteredGraph>;
}

pub trait IterFilter<'graph> {
    type IterGraph: GraphViewOps<'graph> + 'graph;

    type IterFiltered<FilteredGraph: GraphViewOps<'graph> + 'graph>: IterFilter<
        'graph,
        IterGraph = FilteredGraph,
    >;

    fn iter_graph(&self) -> &Self::IterGraph;

    fn apply_iter_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::IterFiltered<FilteredGraph>;
}
