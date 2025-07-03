use crate::model::graph::{meta_graph::MetaGraph, namespace::Namespace};
use dynamic_graphql::Union;

// Either a namespace or a graph
// This is useful for when fetching a collection of both for the purposes of displaying all such
// items, paged.
#[derive(Union, Clone, PartialOrd, PartialEq, Ord, Eq)]
pub(crate) enum NamespacedItem {
    Namespace(Namespace),
    MetaGraph(MetaGraph),
}
