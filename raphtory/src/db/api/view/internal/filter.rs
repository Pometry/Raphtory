use crate::db::api::view::internal::GraphView;

pub trait InternalFilter<'graph> {
    type Graph: GraphView + 'graph + 'graph;

    type Filtered<FilteredGraph: GraphView + 'graph + 'graph>: InternalFilter<
        'graph,
        Graph = FilteredGraph,
    >;

    fn base_graph(&self) -> &Self::Graph;

    fn apply_filter<FilteredGraph: GraphView + 'graph + 'graph>(
        &self,
        filtered_graph: FilteredGraph,
    ) -> Self::Filtered<FilteredGraph>;
}
pub(crate) mod internal {
    use crate::db::api::{state::ops::NodeFilterOp, view::internal::GraphView};

    pub trait InternalNodeSelect<'graph> {
        type IterGraph: GraphView + 'graph + 'graph;

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
}

pub(crate) use internal::*;
