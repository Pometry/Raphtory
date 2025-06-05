use crate::db::api::view::internal::OneHopFilter;

pub trait ResetFilter<'graph>: OneHopFilter<'graph> {
    fn reset_filter(&self) -> Self::Filtered<Self::BaseGraph> {
        self.one_hop_filtered(self.base_graph().clone())
    }
}
