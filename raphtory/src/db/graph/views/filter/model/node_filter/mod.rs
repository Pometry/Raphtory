use crate::db::api::view::internal::Static;

#[derive(Clone, Debug, Default, Copy, PartialEq, Eq)]
pub struct NodeFilter;

impl Static for NodeFilter {}

pub use crate::db::graph::views::filter::model::expr::builder::NodeFilterFactory;
