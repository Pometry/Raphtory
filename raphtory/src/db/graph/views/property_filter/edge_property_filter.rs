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
        graph::{
            edge::EdgeView,
            views::property_filter::{PropFilter, PropValueFilter},
        },
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

#[cfg(test)]
mod test {
    use crate::{db::api::view::internal::CoreGraphOps, prelude::*};
    use itertools::Itertools;

    #[test]
    fn test_filter() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("test", 1i64)], None).unwrap();
        g.add_edge(1, 2, 3, [("test", 2i64)], None).unwrap();
        let prop_id = g.edge_meta().get_prop_id("test", false).unwrap();
        let gf = g.filter_edges_eq("test", Prop::I64(1)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2))]
        );
        let gf = g.filter_edges_gt("test", Prop::I64(1)).unwrap();
        assert_eq!(
            gf.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );
    }
}
