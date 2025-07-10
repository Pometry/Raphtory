use crate::prelude::GraphViewOps;

pub trait BaseFilter<'graph> {
    type Current: GraphViewOps<'graph> + 'graph;

    type Filtered<Next: GraphViewOps<'graph> + 'graph>: BaseFilter<'graph, Current = Next>;

    fn current_filtered_graph(&self) -> &Self::Current;

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next>;
}

pub trait OneHopFilter<'graph> {
    type Current: GraphViewOps<'graph> + 'graph;

    type Filtered<Next: GraphViewOps<'graph> + 'graph>: OneHopFilter<'graph, Current = Next>;

    fn current_filtered_graph(&self) -> &Self::Current;

    fn apply_filter<Next: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: Next,
    ) -> Self::Filtered<Next>;
}
