use crate::prelude::GraphViewOps;

pub trait OneHopFilter<'graph> {
    type BaseGraph: GraphViewOps<'graph> + 'graph;
    type FilteredGraph: GraphViewOps<'graph> + 'graph;

    type Filtered<GH: GraphViewOps<'graph> + 'graph>: OneHopFilter<'graph, FilteredGraph = GH>
        + 'graph;
    fn current_filter(&self) -> &Self::FilteredGraph;

    fn base_graph(&self) -> &Self::BaseGraph;

    fn one_hop_filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH>;
}
