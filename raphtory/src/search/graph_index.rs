use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::{AsTime, TimeIndexEntry},
        utils::errors::GraphError,
    },
    db::api::storage::graph::storage_ops::GraphStorage,
    prelude::*,
    search::{edge_index::EdgeIndex, node_index::NodeIndex},
};
use std::fmt::{Debug, Formatter};
use tantivy::query::QueryParser;

#[derive(Clone)]
pub struct GraphIndex {
    pub(crate) node_index: NodeIndex,
    pub(crate) edge_index: EdgeIndex,
}

impl Debug for GraphIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphIndex")
            .field("node_index", &self.node_index)
            .field("edge_index", &self.edge_index)
            .finish()
    }
}

impl<'a> TryFrom<&'a GraphStorage> for GraphIndex {
    type Error = GraphError;

    fn try_from(graph: &GraphStorage) -> Result<Self, Self::Error> {
        let node_index = NodeIndex::index_nodes(graph)?;
        let edge_index = EdgeIndex::index_edges(graph)?;

        // node_index.print()?;

        Ok(GraphIndex {
            node_index,
            edge_index,
        })
    }
}

impl GraphIndex {
    pub fn new() -> Self {
        GraphIndex {
            node_index: NodeIndex::new(),
            edge_index: EdgeIndex::new(),
        }
    }

    pub fn print(&self) -> Result<(), GraphError> {
        self.node_index.print()?;
        self.edge_index.print()?;
        Ok(())
    }

    pub(crate) fn node_parser(&self) -> Result<QueryParser, GraphError> {
        Ok(QueryParser::for_index(&self.node_index.index, vec![]))
    }

    pub(crate) fn edge_parser(&self) -> Result<QueryParser, GraphError> {
        Ok(QueryParser::for_index(&self.edge_index.index, vec![]))
    }

    pub(crate) fn add_node_update(
        &self,
        graph: &GraphStorage,
        t: TimeIndexEntry,
        v: VID,
    ) -> Result<(), GraphError> {
        self.node_index.add_node_update(graph, t, v)
    }

    pub(crate) fn add_edge_update(
        &self,
        graph: &GraphStorage,
        edge_id: EID,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.edge_index
            .add_edge_update(graph, edge_id, t, src, dst, layer)
    }
}

#[cfg(test)]
mod graph_index_test {
    use crate::{
        db::api::view::internal::InternalIndexSearch,
        prelude::{AdditionOps, GraphViewOps},
    };

    #[test]
    fn test1() {
        let graph = crate::db::graph::graph::Graph::new();
        graph
            .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(2, 1, [("p6", 6)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(2, 2, [("p4", 5)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"))
            .unwrap();

        assert_eq!(graph.count_nodes(), 3);

        // Creates graph index
        let _ = graph.searcher().unwrap();
    }

    #[test]
    fn test2() {
        let graph = crate::db::graph::graph::Graph::new();
        // Creates graph index
        let _ = graph.searcher().unwrap();

        graph
            .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(2, 1, [("p6", 6)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(2, 2, [("p4", 5)], Some("fire_nation"))
            .unwrap();
        graph
            .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"))
            .unwrap();

        assert_eq!(graph.count_nodes(), 3);

        graph.searcher().unwrap().index.print().unwrap();
    }

    #[test]
    fn test3() {
        let graph = crate::db::graph::graph::Graph::new();
        // Creates graph index
        let _ = graph.searcher().unwrap();

        graph
            .add_edge(1, 1, 2, [("p1", 1), ("p2", 2)], Some("fire_nation"))
            .unwrap();
        graph
            .add_edge(2, 1, 2, [("p6", 6)], Some("fire_nation"))
            .unwrap();
        graph
            .add_edge(2, 2, 3, [("p4", 5)], Some("fire_nation"))
            .unwrap();
        graph
            .add_edge(3, 3, 4, [("p2", 4), ("p3", 3)], Some("water_tribe"))
            .unwrap();

        assert_eq!(graph.count_edges(), 3);

        graph.searcher().unwrap().index.print().unwrap();
    }
}
