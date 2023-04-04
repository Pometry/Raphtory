use crate::edge::EdgeView;
use crate::view_api::{GraphViewOps, VertexListOps};
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::Prop;
use std::collections::HashMap;

/// This trait defines the operations that can be
/// performed on a list of edges in a temporal graph view.
pub trait EdgeListOps:
    IntoIterator<Item = EdgeView<Self::Graph>, IntoIter = Self::IterType> + Sized + Send
{
    type Graph: GraphViewOps;

    /// the type of list of vertices
    type VList: VertexListOps<Graph = Self::Graph>;

    /// the type of iterator
    type IterType: Iterator<Item = EdgeView<Self::Graph>> + Send;

    fn has_property(
        self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = bool> + Send>;

    fn property(
        self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Option<Prop>> + Send>;
    fn properties(
        self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = HashMap<String, Prop>> + Send>;
    fn property_names(self, include_static: bool) -> Box<dyn Iterator<Item = Vec<String>> + Send>;

    fn has_static_property(self, name: String) -> Box<dyn Iterator<Item = bool> + Send>;
    fn static_property(self, name: String) -> Box<dyn Iterator<Item = Option<Prop>> + Send>;

    /// gets a property of an edge with the given name
    /// includes the timestamp of the property
    fn property_history(self, name: String) -> Box<dyn Iterator<Item = Vec<(i64, Prop)>> + Send>;
    fn property_histories(
        self,
    ) -> Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + Send>;

    /// gets the source vertices of the edges in the list
    fn src(self) -> Self::VList;

    /// gets the destination vertices of the edges in the list
    fn dst(self) -> Self::VList;
}
