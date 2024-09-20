use crate::{
    core::entities::LayerIds,
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            view::{
                internal::{
                    EdgeFilterOps, Immutable, InheritCoreOps, InheritLayerOps, InheritListOps,
                    InheritMaterialize, InheritNodeFilterOps, InheritTimeSemantics, Static,
                },
                Base,
            },
        },
        graph::{edge::EdgeView, views::property_filter::PropFilter},
    },
    prelude::{EdgeViewOps, GraphViewOps},
};

#[derive(Debug, Clone)]
pub struct EdgePropertyFilteredGraph<G> {
    graph: G,
    t_prop_id: Option<usize>,
    c_prop_id: Option<usize>,
    filter: PropFilter,
}

impl<'graph, G> EdgePropertyFilteredGraph<G> {
    pub(crate) fn new(
        graph: G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        filter: impl Into<PropFilter>,
    ) -> Self {
        Self {
            graph,
            t_prop_id,
            c_prop_id,
            filter: filter.into(),
        }
    }
}

impl<G> Static for EdgePropertyFilteredGraph<G> {}
impl<G> Immutable for EdgePropertyFilteredGraph<G> {}

impl<'graph, G> Base for EdgePropertyFilteredGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for EdgePropertyFilteredGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for EdgePropertyFilteredGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for EdgePropertyFilteredGraph<G> {
    fn edges_filtered(&self) -> bool {
        true
    }

    fn edge_list_trusted(&self) -> bool {
        false
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        if self.graph.filter_edge(edge, layer_ids) {
            let props = EdgeView::new(&self.graph, edge.out_ref()).properties();
            let prop_value = self
                .t_prop_id
                .and_then(|prop_id| {
                    props
                        .temporal()
                        .get_by_id(prop_id)
                        .and_then(|prop_view| prop_view.latest())
                })
                .or_else(|| {
                    self.c_prop_id
                        .and_then(|prop_id| props.constant().get_by_id(prop_id))
                });
            self.filter.filter(prop_value.as_ref())
        } else {
            false
        }
    }
}
