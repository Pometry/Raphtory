use super::GraphStorage;
use crate::db::api::view::internal::InternalMaterialize;
use raphtory_api::GraphType;

impl InternalMaterialize for GraphStorage {
    fn graph_type(&self) -> GraphType {
        GraphType::EventGraph
    }

    fn include_deletions(&self) -> bool {
        false
    }
}
