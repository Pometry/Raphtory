use super::GraphStorage;
use crate::db::api::view::internal::{GraphType, InternalMaterialize};

impl InternalMaterialize for GraphStorage {
    fn graph_type(&self) -> GraphType {
        GraphType::EventGraph
    }

    fn include_deletions(&self) -> bool {
        false
    }
}
