use crate::core::Prop;
use crate::db::edge::EdgeView;
use crate::db::view_api::{BoxedIter, GraphViewOps, VertexListOps};
use std::collections::HashMap;

/// This trait defines the operations that can be
/// performed on a list of edges in a temporal graph view.
pub trait EdgeListOps:
    IntoIterator<Item = Self::ValueType<EdgeView<Self::Graph>>, IntoIter = Self::IterType>
    + Sized
    + Send
{
    type Graph: GraphViewOps;
    type ValueType<T: Send + Sync>: Send;

    /// the type of list of vertices
    type VList: VertexListOps<Graph = Self::Graph>;

    /// the type of iterator
    type IterType: Iterator<Item = Self::ValueType<EdgeView<Self::Graph>>> + Send;

    fn has_property(
        self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Self::ValueType<bool>> + Send>;

    fn property(
        self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Self::ValueType<Option<Prop>>> + Send>;
    fn properties(
        self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Self::ValueType<HashMap<String, Prop>>> + Send>;
    fn property_names(
        self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Self::ValueType<Vec<String>>> + Send>;

    fn has_static_property(
        self,
        name: String,
    ) -> Box<dyn Iterator<Item = Self::ValueType<bool>> + Send>;
    fn static_property(
        self,
        name: String,
    ) -> Box<dyn Iterator<Item = Self::ValueType<Option<Prop>>> + Send>;

    /// gets a property of an edge with the given name
    /// includes the timestamp of the property
    fn property_history(
        self,
        name: String,
    ) -> Box<dyn Iterator<Item = Self::ValueType<Vec<(i64, Prop)>>> + Send>;
    fn property_histories(
        self,
    ) -> Box<dyn Iterator<Item = Self::ValueType<HashMap<String, Vec<(i64, Prop)>>>> + Send>;

    /// gets the source vertices of the edges in the list
    fn src(self) -> Self::VList;

    /// gets the destination vertices of the edges in the list
    fn dst(self) -> Self::VList;

    /// returns a list of exploded edges that include an edge at each point in time
    fn explode(self) -> Self::IterType;

    /// Get the timestamp for the earliest activity of the edge
    fn earliest_time(self) -> BoxedIter<i64>;

    /// Get the timestamp for the latest activity of the edge
    fn latest_time(self) -> BoxedIter<i64>;
}
