use std::borrow::Borrow;
use crate::db::api::view::internal::{CoreGraphOps, OneHopFilter};
use crate::db::graph::views::node_type_filtered_subgraph::TypeFilteredSubgraph;

pub trait NodeTypesFilter<'graph>: OneHopFilter<'graph> {
    fn type_filter<I: IntoIterator<Item=V>, V: Borrow<str>>(&self, node_types: I)
                                                            -> Self::Filtered<TypeFilteredSubgraph<Self::FilteredGraph>> {
        let meta = self.current_filter().node_meta().node_type_meta();
        let r = node_types.into_iter().flat_map(|nt| {
            meta.get_id(nt.borrow())
        }).collect();
        self.one_hop_filtered(TypeFilteredSubgraph::new(self.current_filter().clone(), r))
    }
}
