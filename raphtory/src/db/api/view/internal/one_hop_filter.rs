use crate::prelude::GraphViewOps;

pub trait OneHopFilter<'graph> {
    type Graph: GraphViewOps<'graph> + 'graph;
    type Filtered<GH: GraphViewOps<'graph> + 'graph>: OneHopFilter<'graph> + 'graph;
    fn current_filter(&self) -> &Self::Graph;

    fn one_hop_filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH>;
}
