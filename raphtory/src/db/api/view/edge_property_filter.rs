use crate::{
    core::Prop,
    db::{
        api::view::internal::{CoreGraphOps, OneHopFilter},
        graph::views::property_filter::{
            edge_property_filter::EdgePropertyFilteredGraph, PropFilter,
        },
    },
};
use std::cmp::Ordering::{Equal, Greater, Less};

pub trait EdgePropertyFilterOps<'graph> {
    type FilteredViewType;

    fn filter_edges_eq(&self, property: &str, value: Prop) -> Self::FilteredViewType;

    fn filter_edges_lt(&self, property: &str, value: Prop) -> Self::FilteredViewType;

    fn filter_edges_gt(&self, property: &str, value: Prop) -> Self::FilteredViewType;
}

impl<'graph, G: OneHopFilter<'graph>> EdgePropertyFilterOps<'graph> for G {
    type FilteredViewType = G::Filtered<EdgePropertyFilteredGraph<G::FilteredGraph>>;

    fn filter_edges_lt(&self, property: &str, value: Prop) -> Self::FilteredViewType {
        let t_prop_id = self
            .current_filter()
            .edge_meta()
            .get_prop_id(property, false);
        let c_prop_id = self
            .current_filter()
            .edge_meta()
            .get_prop_id(property, true);

        self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropFilter::new(value, Less),
        ))
    }

    fn filter_edges_eq(&self, property: &str, value: Prop) -> Self::FilteredViewType {
        let t_prop_id = self
            .current_filter()
            .edge_meta()
            .get_prop_id(property, false);
        let c_prop_id = self
            .current_filter()
            .edge_meta()
            .get_prop_id(property, true);

        self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropFilter::new(value, Equal),
        ))
    }

    fn filter_edges_gt(&self, property: &str, value: Prop) -> Self::FilteredViewType {
        let t_prop_id = self
            .current_filter()
            .edge_meta()
            .get_prop_id(property, false);
        let c_prop_id = self
            .current_filter()
            .edge_meta()
            .get_prop_id(property, true);

        self.one_hop_filtered(EdgePropertyFilteredGraph::new(
            self.current_filter().clone(),
            t_prop_id,
            c_prop_id,
            PropFilter::new(value, Greater),
        ))
    }
}

#[cfg(test)]
mod test {
    use crate::{db::api::view::internal::CoreGraphOps, prelude::*};
    use itertools::Itertools;

    #[test]
    fn test_edge_property_filter_on_nodes() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, [("test", 1i64)], None).unwrap();
        g.add_edge(0, 1, 3, [("test", 3i64)], None).unwrap();
        g.add_edge(1, 2, 3, [("test", 2i64)], None).unwrap();
        g.add_edge(1, 2, 4, [("test", 0i64)], None).unwrap();
        let prop_id = g.edge_meta().get_prop_id("test", false).unwrap();
        let n1 = g.node(1).unwrap().filter_edges_eq("test", Prop::I64(1));
        assert_eq!(
            n1.edges().id().collect_vec(),
            vec![(GID::U64(1), GID::U64(2))]
        );
        let n2 = g.node(2).unwrap().filter_edges_gt("test", Prop::I64(1));
        assert_eq!(
            n2.edges().id().collect_vec(),
            vec![(GID::U64(2), GID::U64(3))]
        );
    }
}
