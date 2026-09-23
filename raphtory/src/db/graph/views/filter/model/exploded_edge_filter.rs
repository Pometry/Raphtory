use crate::db::graph::views::filter::model::{
    is_active_edge_filter::IsActiveEdge, is_deleted_filter::IsDeletedEdge,
    is_self_loop_filter::IsSelfLoopEdge, is_valid_filter::IsValidEdge, windowed_filter::Windowed,
    CombinedFilter, EdgeViewFilterOps, EntityMarker, InternalViewWrapOps, Wrap,
};
use raphtory_api::core::storage::timeindex::EventTime;

#[derive(Clone, Debug, Copy, Default, PartialEq, Eq)]
pub struct ExplodedEdgeFilter;

impl From<ExplodedEdgeFilter> for EntityMarker {
    fn from(_value: ExplodedEdgeFilter) -> Self {
        EntityMarker::ExplodedEdge
    }
}

impl Wrap for ExplodedEdgeFilter {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl InternalViewWrapOps for ExplodedEdgeFilter {
    type Window = Windowed<ExplodedEdgeFilter>;

    fn build_window(self, start: EventTime, end: EventTime) -> Self::Window {
        Windowed::from_times(start, end, self)
    }
}

impl EdgeViewFilterOps for ExplodedEdgeFilter {
    type Output<T: CombinedFilter> = T;

    fn is_active(&self) -> Self::Output<IsActiveEdge> {
        IsActiveEdge
    }

    fn is_valid(&self) -> Self::Output<IsValidEdge> {
        IsValidEdge
    }

    fn is_deleted(&self) -> Self::Output<IsDeletedEdge> {
        IsDeletedEdge
    }

    fn is_self_loop(&self) -> Self::Output<IsSelfLoopEdge> {
        IsSelfLoopEdge
    }
}
