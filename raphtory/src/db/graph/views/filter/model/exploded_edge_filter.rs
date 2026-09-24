use crate::db::{
    api::view::internal::Static,
    graph::views::filter::model::{windowed_filter::Windowed, EntityMarker, InternalViewWrapOps},
};
use raphtory_api::core::storage::timeindex::EventTime;

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl Static for ExplodedEdgeFilter {}

impl From<ExplodedEdgeFilter> for EntityMarker {
    fn from(_value: ExplodedEdgeFilter) -> Self {
        EntityMarker::ExplodedEdge
    }
}

impl InternalViewWrapOps for ExplodedEdgeFilter {
    type Window = Windowed<ExplodedEdgeFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}
