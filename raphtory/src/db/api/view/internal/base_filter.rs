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

pub trait OneHopFilter<'graph> {
    type OneHopGraph: GraphViewOps<'graph> + 'graph;

    type OneHopFiltered<FilteredGraph: GraphViewOps<'graph> + 'graph>: OneHopFilter<
        'graph,
        OneHopGraph = FilteredGraph,
    >;

    fn one_hop_graph(&self) -> &Self::OneHopGraph;

    fn apply_one_hop_filter<FilteredGraph: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::OneHopFiltered<FilteredGraph>;
}
