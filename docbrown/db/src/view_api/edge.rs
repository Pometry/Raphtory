use crate::edge::EdgeView;
use crate::view_api::{GraphViewOps, VertexListOps};
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::Prop;

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

    /// gets the source vertices of the edges in the list
    fn src(self) -> Self::VList;

    /// gets the destination vertices of the edges in the list
    fn dst(self) -> Self::VList;
}
