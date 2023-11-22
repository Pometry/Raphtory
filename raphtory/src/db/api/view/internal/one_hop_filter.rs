use crate::{
    core::utils::time::IntoTime,
    db::{
        api::view::internal::TimeSemantics,
        graph::views::{layer_graph::LayeredGraph, window_graph::WindowedGraph},
    },
    prelude::{GraphViewOps, Layer, LayerOps, TimeOps},
};

pub trait OneHopFilter<'graph> {
    type Graph: GraphViewOps<'graph> + 'graph;
    type Filtered<GH: GraphViewOps<'graph> + 'graph>: OneHopFilter<'graph> + 'graph;
    fn current_filter(&self) -> &Self::Graph;

    fn one_hop_filtered<GH: GraphViewOps<'graph> + 'graph>(
        &self,
        filtered_graph: GH,
    ) -> Self::Filtered<GH>;
}
