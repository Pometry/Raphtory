use crate::model::graph::meta_graph::MetaGraph;
use crate::model::graph::namespace::Namespace;
use dynamic_graphql::Union;

#[derive(Union, Clone, PartialOrd, PartialEq, Ord, Eq)]
pub(crate) enum NamespacedItem {
    Namespace(Namespace),
    MetaGraph(MetaGraph),
}
