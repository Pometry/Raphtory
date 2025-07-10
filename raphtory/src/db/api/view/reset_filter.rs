use crate::db::api::view::internal::BaseFilter;

pub trait ResetFilter<'graph>: BaseFilter<'graph> {
    // fn reset_filter(&self) -> Self::Filtered<Self::BaseGraph> {
    //     self.apply_filter(self.base_graph().clone())
    // }
}
