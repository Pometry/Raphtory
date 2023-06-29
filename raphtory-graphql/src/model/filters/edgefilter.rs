use crate::model::filters::primitives::{StringFilter, StringVecFilter};
use crate::model::graph::edge::Edge;
use dynamic_graphql::InputObject;
use raphtory::db::view_api::{EdgeViewOps, VertexViewOps};

#[derive(InputObject)]
pub struct EdgeFilter {
    node_names: Option<StringVecFilter>,
    src: Option<StringFilter>,
    dst: Option<StringFilter>,
    layer_names: Option<StringVecFilter>,
}

impl EdgeFilter {
    pub(crate) fn matches(&self, edge: &Edge) -> bool {
        if let Some(names_filter) = &self.node_names {
            let src = edge.ee.src().name();
            let dst = edge.ee.dst().name();
            if !names_filter.contains(&src) || !names_filter.contains(&dst) {
                return false;
            }
        }

        if let Some(name_filter) = &self.src {
            if !name_filter.matches(&edge.ee.src().name()) {
                return false;
            }
        }

        if let Some(name_filter) = &self.dst {
            if !name_filter.matches(&edge.ee.dst().name()) {
                return false;
            }
        }

        if let Some(name_filter) = &self.layer_names {
            if !name_filter.contains(&edge.ee.layer_name()) {
                return false;
            }
        }

        true
    }
}
