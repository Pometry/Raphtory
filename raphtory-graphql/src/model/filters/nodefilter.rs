use std::borrow::Cow;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::VertexViewOps;
use crate::model::graph::node::Node;
use dynamic_graphql::{InputObject};
use dynamic_graphql::internal::{FromValue, InputTypeName, InputValueResult, Register, TypeName};
use raphtory::core::Prop;


#[derive(InputObject)]
pub struct NodeFilter {
    name: Option<StringFilter>,
    node_type:Option<StringFilter>,
    in_degree: Option<NumberFilter>,
    out_degree: Option<NumberFilter>,
}

#[derive(InputObject)]
pub struct StringFilter {
    eq: Option<String>,
    ne: Option<String>,
}

#[derive(InputObject)]
pub struct NumberFilter {
    gt: Option<usize>,
    lt: Option<usize>,
    eq: Option<usize>,
    ne: Option<usize>,
    gte: Option<usize>,
    lte: Option<usize>,
}

impl NodeFilter {
    pub(crate) fn matches(&self, node: &Node) -> bool {
        if let Some(name_filter) = &self.name {
            if let Some(eq) = &name_filter.eq {
                if node.vv.name() != *eq {
                    return false;
                }
            }

            if let Some(ne) = &name_filter.ne {
                if node.vv.name() == *ne {
                    return false;
                }
            }
        }

        if let Some(type_filter) = &self.node_type {
            let node_type= node.vv.property("type".to_string(),true).unwrap_or(Prop::Str("NONE".to_string())).to_string();
            if let Some(eq) = &type_filter.eq {
                if node_type != *eq {
                    return false;
                }
            }

            if let Some(ne) = &type_filter.ne {
                if node_type == *ne {
                    return false;
                }
            }
        }

        if let Some(in_degree_filter) = &self.in_degree {
            if !in_degree_filter.matches(node.vv.in_degree()) {
                return false;
            }
        }

        if let Some(out_degree_filter) = &self.out_degree {
            if !out_degree_filter.matches(node.vv.out_degree()) {
                return false;
            }
        }

        true
    }
}

impl NumberFilter {
    fn matches(&self, value: usize) -> bool {
        if let Some(gt) = self.gt {
            if value <= gt {
                return false;
            }
        }

        if let Some(lt) = self.lt {
            if value >= lt {
                return false;
            }
        }

        if let Some(eq) = self.eq {
            if value != eq {
                return false;
            }
        }

        if let Some(ne) = self.ne {
            if value == ne {
                return false;
            }
        }

        if let Some(gte) = self.gte {
            if value < gte {
                return false;
            }
        }

        if let Some(lte) = self.lte {
            if value > lte {
                return false;
            }
        }

        true
    }
}
