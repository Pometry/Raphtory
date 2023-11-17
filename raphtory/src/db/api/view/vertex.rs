use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, VID},
        utils::time::IntoTime,
        Direction,
    },
    db::{
        api::{
            properties::{internal::PropertiesOps, Properties},
            view::{
                edge::EdgeListOps,
                internal::{
                    CoreGraphOps, EdgeFilterOps, GraphOps, InternalLayerOps, TimeSemantics,
                },
                BoxedIter, GraphViewOps, TimeOps,
            },
        },
        graph::{vertex::VertexView, views::window_graph::WindowedGraph},
    },
    prelude::{EdgeViewOps, LayerOps},
};
use pyo3::ffi::PyBaseObject_Type;

pub(crate) trait BaseVertexViewOps: Clone {
    type BaseGraph: GraphViewOps;
    type Graph: GraphViewOps;
    type ValueType<T>;

    type PropType: PropertiesOps + Clone;
    type PathType: VertexViewOps<BaseGraph = Self::BaseGraph, Graph = Self::BaseGraph>;
    type Edge: EdgeViewOps<Graph = Self::BaseGraph>;
    type EList: EdgeListOps<Graph = Self::BaseGraph, Edge = Self::Edge>;

    fn map<O, F: for<'a> Fn(&'a Self::Graph, VID) -> O + Send + Sync>(
        &self,
        op: F,
    ) -> Self::ValueType<O>;

    fn as_props(&self) -> Self::ValueType<Properties<Self::PropType>>;

    fn map_edges<
        I: Iterator<Item = EdgeRef> + Send,
        F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync,
    >(
        &self,
        op: F,
    ) -> Self::EList;

    fn hop<I: Iterator<Item = VID> + Send, F: for<'a> Fn(&'a Self::Graph, VID) -> I + Send + Sync>(
        &self,
        op: F,
    ) -> Self::PathType;
}

/// Operations defined for a vertex
pub trait VertexViewOps: Clone {
    type BaseGraph: GraphViewOps;
    type Graph: GraphViewOps;
    type ValueType<T>;
    type PathType: VertexViewOps<BaseGraph = Self::BaseGraph, Graph = Self::BaseGraph>;
    type PropType: PropertiesOps + Clone;
    type Edge: EdgeViewOps<Graph = Self::BaseGraph>;
    type EList: EdgeListOps<Graph = Self::BaseGraph, Edge = Self::Edge>;

    /// Get the numeric id of the vertex
    fn id(&self) -> Self::ValueType<u64>;

    /// Get the name of this vertex if a user has set one otherwise it returns the ID.
    ///
    /// Returns:
    ///
    /// The name of the vertex if one exists, otherwise the ID as a string.
    fn name(&self) -> Self::ValueType<String>;

    /// Get the timestamp for the earliest activity of the vertex
    fn earliest_time(&self) -> Self::ValueType<Option<i64>>;

    /// Get the timestamp for the latest activity of the vertex
    fn latest_time(&self) -> Self::ValueType<Option<i64>>;

    /// Gets the history of the vertex (time that the vertex was added and times when changes were made to the vertex)
    fn history(&self) -> Self::ValueType<Vec<i64>>;

    /// Get a view of the temporal properties of this vertex.
    ///
    /// Returns:
    ///
    /// A view with the names of the properties as keys and the property values as values.
    fn properties(&self) -> Self::ValueType<Properties<Self::PropType>>;

    /// Get the degree of this vertex (i.e., the number of edges that are incident to it).
    ///
    /// Returns:
    ///
    /// The degree of this vertex.
    fn degree(&self) -> Self::ValueType<usize>;

    /// Get the in-degree of this vertex (i.e., the number of edges that point into it).
    ///
    /// Returns:
    ///
    /// The in-degree of this vertex.
    fn in_degree(&self) -> Self::ValueType<usize>;

    /// Get the out-degree of this vertex (i.e., the number of edges that point out of it).
    ///
    /// Returns:
    ///
    /// The out-degree of this vertex.
    fn out_degree(&self) -> Self::ValueType<usize>;

    /// Get the edges that are incident to this vertex.
    ///
    /// Returns:
    ///
    /// An iterator over the edges that are incident to this vertex.
    fn edges(&self) -> Self::EList;

    /// Get the edges that point into this vertex.
    ///
    /// Returns:
    ///
    /// An iterator over the edges that point into this vertex.
    fn in_edges(&self) -> Self::EList;

    /// Get the edges that point out of this vertex.
    ///
    /// Returns:
    ///
    /// An iterator over the edges that point out of this vertex.
    fn out_edges(&self) -> Self::EList;

    /// Get the neighbours of this vertex.
    ///
    /// Returns:
    ///
    /// An iterator over the neighbours of this vertex.
    fn neighbours(&self) -> Self::PathType;

    /// Get the neighbours of this vertex that point into this vertex.
    ///
    /// Returns:
    ///
    /// An iterator over the neighbours of this vertex that point into this vertex.
    fn in_neighbours(&self) -> Self::PathType;

    /// Get the neighbours of this vertex that point out of this vertex.
    ///
    /// Returns:
    ///
    /// An iterator over the neighbours of this vertex that point out of this vertex.
    fn out_neighbours(&self) -> Self::PathType;
}

impl<V: BaseVertexViewOps> VertexViewOps for V {
    type BaseGraph = V::BaseGraph;
    type Graph = V::Graph;
    type ValueType<T> = V::ValueType<T>;
    type PathType = V::PathType;
    type PropType = V::PropType;
    type Edge = V::Edge;
    type EList = V::EList;

    fn id(&self) -> Self::ValueType<u64> {
        self.map(|g, v| g.vertex_id(v))
    }

    fn name(&self) -> Self::ValueType<String> {
        self.map(|g, v| g.vertex_name(v))
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, v| g.vertex_earliest_time(v))
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.map(|g, v| g.vertex_latest_time(v))
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.map(|g, v| g.vertex_history(v))
    }

    fn properties(&self) -> Self::ValueType<Properties<Self::PropType>> {
        self.as_props()
    }

    fn degree(&self) -> Self::ValueType<usize> {
        self.map(|g, v| g.degree(v, Direction::BOTH, &g.layer_ids(), g.edge_filter()))
    }

    fn in_degree(&self) -> Self::ValueType<usize> {
        self.map(|g, v| g.degree(v, Direction::IN, &g.layer_ids(), g.edge_filter()))
    }

    fn out_degree(&self) -> Self::ValueType<usize> {
        self.map(|g, v| g.degree(v, Direction::OUT, &g.layer_ids(), g.edge_filter()))
    }

    fn edges(&self) -> Self::EList {
        self.map_edges(|g, v| g.vertex_edges(v, Direction::BOTH, g.layer_ids(), g.edge_filter()))
    }

    fn in_edges(&self) -> Self::EList {
        self.map_edges(|g, v| g.vertex_edges(v, Direction::IN, g.layer_ids(), g.edge_filter()))
    }

    fn out_edges(&self) -> Self::EList {
        self.map_edges(|g, v| g.vertex_edges(v, Direction::OUT, g.layer_ids(), g.edge_filter()))
    }

    fn neighbours(&self) -> Self::PathType {
        self.hop(|g, v| g.neighbours(v, Direction::BOTH, g.layer_ids(), g.edge_filter()))
    }

    fn in_neighbours(&self) -> Self::PathType {
        self.hop(|g, v| g.neighbours(v, Direction::IN, g.layer_ids(), g.edge_filter()))
    }

    fn out_neighbours(&self) -> Self::PathType {
        self.hop(|g, v| g.neighbours(v, Direction::OUT, g.layer_ids(), g.edge_filter()))
    }
}

/// A trait for operations on a list of vertices.
pub trait VertexListOps: IntoIterator<Item = Self::ValueType<Self::Vertex>> + Sized {
    type Vertex: VertexViewOps + TimeOps;
    type Edge: EdgeViewOps<Graph = <Self::Vertex as VertexViewOps>::BaseGraph>;

    /// The type of the iterator for the list of vertices
    type IterType<T>: Iterator<
        Item = Self::ValueType<<Self::Vertex as VertexViewOps>::ValueType<T>>,
    >;
    /// The type of the iterator for the list of edges
    type ValueType<T>;

    /// Return the timestamp of the earliest activity.
    fn earliest_time(self) -> Self::IterType<Option<i64>>;

    /// Return the timestamp of the latest activity.
    fn latest_time(self) -> Self::IterType<Option<i64>>;

    /// Create views for the vertices including all events between `start` (inclusive) and `end` (exclusive)
    fn window(
        self,
        start: i64,
        end: i64,
    ) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType>;

    /// Create views for the vertices including all events until `end` (inclusive)
    fn at(self, end: i64) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType>;

    /// Returns the ids of vertices in the list.
    ///
    /// Returns:
    /// The ids of vertices in the list.
    fn id(self) -> Self::IterType<u64>;
    fn name(self) -> Self::IterType<String>;

    /// Returns an iterator over properties of the vertices
    fn properties(self) -> Self::IterType<Properties<<Self::Vertex as VertexViewOps>::PropType>>;

    fn history(self) -> Self::IterType<Vec<i64>>;

    /// Returns an iterator over the degree of the vertices.
    ///
    /// Returns:
    /// An iterator over the degree of the vertices.
    fn degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the in-degree of the vertices.
    /// The in-degree of a vertex is the number of edges that connect to it from other vertices.
    ///
    /// Returns:
    /// An iterator over the in-degree of the vertices.
    fn in_degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the out-degree of the vertices.
    /// The out-degree of a vertex is the number of edges that connects to it from the vertex.
    ///
    /// Returns:
    ///
    /// An iterator over the out-degree of the vertices.
    fn out_degree(self) -> Self::IterType<usize>;

    /// Returns an iterator over the edges of the vertices.
    fn edges(self) -> Self::IterType<Self::Edge>;

    /// Returns an iterator over the incoming edges of the vertices.
    ///
    /// Returns:
    ///
    /// An iterator over the incoming edges of the vertices.
    fn in_edges(self) -> Self::IterType<Self::Edge>;

    /// Returns an iterator over the outgoing edges of the vertices.
    ///
    /// Returns:
    ///
    /// An iterator over the outgoing edges of the vertices.
    fn out_edges(self) -> Self::IterType<Self::Edge>;

    /// Returns an iterator over the neighbours of the vertices.
    ///
    /// Returns:
    ///
    /// An iterator over the neighbours of the vertices as VertexViews.
    fn neighbours(self) -> Self::IterType<Self::Vertex>;

    /// Returns an iterator over the incoming neighbours of the vertices.
    ///
    /// Returns:
    ///
    /// An iterator over the incoming neighbours of the vertices as VertexViews.
    fn in_neighbours(self) -> Self::IterType<Self::Vertex>;

    /// Returns an iterator over the outgoing neighbours of the vertices.
    ///
    /// Returns:
    ///
    /// An iterator over the outgoing neighbours of the vertices as VertexViews.
    fn out_neighbours(self) -> Self::IterType<Self::Vertex>;
}

impl<V: VertexViewOps + TimeOps> VertexListOps for BoxedIter<BoxedIter<V>> {
    type Vertex = V;
    type Edge = V::Edge;
    type IterType<T> = BoxedIter<BoxedIter<V::ValueType<T>>>;
    type ValueType<T> = BoxedIter<T>;

    fn earliest_time(self) -> Self::IterType<Option<i64>> {
        todo!()
    }

    fn latest_time(self) -> Self::IterType<Option<i64>> {
        todo!()
    }

    fn window(
        self,
        start: i64,
        end: i64,
    ) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        todo!()
    }

    fn at(self, end: i64) -> Self::IterType<<Self::Vertex as TimeOps>::WindowedViewType> {
        todo!()
    }

    fn id(self) -> Self::IterType<u64> {
        todo!()
    }

    fn name(self) -> Self::IterType<String> {
        todo!()
    }

    fn properties(self) -> Self::IterType<Properties<<Self::Vertex as VertexViewOps>::PropType>> {
        todo!()
    }

    fn history(self) -> Self::IterType<Vec<i64>> {
        todo!()
    }

    fn degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn in_degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn out_degree(self) -> Self::IterType<usize> {
        todo!()
    }

    fn edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn in_edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn out_edges(self) -> Self::IterType<Self::Edge> {
        todo!()
    }

    fn neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }

    fn in_neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }

    fn out_neighbours(self) -> Self::IterType<Self::Vertex> {
        todo!()
    }
}
