use crate::{
    core::{
        entities::{EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
    },
    db::api::storage::graph::storage_ops::GraphStorage,
    prelude::*,
    search::{
        edge_index::EdgeIndex, fields, node_index::NodeIndex, property_index::PropertyIndex,
        searcher::Searcher,
    },
};
use raphtory_api::core::{storage::dict_mapper::MaybeNew, PropType};
use std::{
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
};
use tantivy::schema::{FAST, INDEXED, STORED};

#[derive(Clone)]
pub struct GraphIndex {
    pub(crate) node_index: NodeIndex,
    pub(crate) edge_index: EdgeIndex,
    pub(crate) path: Option<PathBuf>,
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
        // TODO: Say by the time we are here, we already know the path
        let path: Option<PathBuf> = None;
        let node_index = NodeIndex::index_nodes(graph, &path)?;
        // node_index.print()?;

        let edge_index = EdgeIndex::index_edges(graph, &path)?;
        // edge_index.print()?;

        Ok(GraphIndex {
            node_index,
            edge_index,
            path,
        })
    }
}

impl GraphIndex {
    pub fn new() -> Self {
        let path: Option<PathBuf> = None;
        GraphIndex {
            node_index: NodeIndex::new(&path),
            edge_index: EdgeIndex::new(&path),
            path,
        }
    }

    pub fn searcher(&self) -> Searcher {
        Searcher::new(self)
    }

    #[allow(dead_code)]
    // Useful for debugging
    pub fn print(&self) -> Result<(), GraphError> {
        self.node_index.print()?;
        self.edge_index.print()?;
        Ok(())
    }

    pub(crate) fn add_node_update(
        &self,
        graph: &GraphStorage,
        t: TimeIndexEntry,
        v: MaybeNew<VID>,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.node_index.add_node_update(graph, t, v, props)
    }

    pub(crate) fn add_node_constant_properties(
        &self,
        graph: &GraphStorage,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.node_index
            .add_node_constant_properties(graph, node_id, props)
    }

    pub(crate) fn update_node_constant_properties(
        &self,
        graph: &GraphStorage,
        node_id: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.node_index
            .update_node_constant_properties(graph, node_id, props)
    }

    pub(crate) fn add_edge_update(
        &self,
        graph: &GraphStorage,
        edge_id: MaybeNew<EID>,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .add_edge_update(graph, edge_id, t, src, dst, layer, props)
    }

    pub(crate) fn add_edge_constant_properties(
        &self,
        graph: &GraphStorage,
        edge_id: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .add_edge_constant_properties(graph, edge_id, layer, props)
    }

    pub(crate) fn update_edge_constant_properties(
        &self,
        graph: &GraphStorage,
        edge_id: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.edge_index
            .update_edge_constant_properties(graph, edge_id, layer, props)
    }

    pub(crate) fn create_edge_property_index(
        &self,
        prop_id: MaybeNew<usize>,
        prop_name: &str,
        prop_type: &PropType,
        is_static: bool, // Const or Temporal Property
    ) -> Result<(), GraphError> {
        self.edge_index.entity_index.create_property_index(
            prop_id,
            prop_name,
            prop_type,
            is_static,
            |schema| {
                schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
            },
            |schema| {
                schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::EDGE_ID, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::LAYER_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_edge_property,
            &self.path,
        )
    }

    pub(crate) fn create_node_property_index(
        &self,
        prop_id: MaybeNew<usize>,
        prop_name: &str,
        prop_type: &PropType,
        is_static: bool, // Const or Temporal Property
    ) -> Result<(), GraphError> {
        self.node_index.entity_index.create_property_index(
            prop_id,
            prop_name,
            prop_type,
            is_static,
            |schema| {
                schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
            },
            |schema| {
                schema.add_i64_field(fields::TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::SECONDARY_TIME, INDEXED | FAST | STORED);
                schema.add_u64_field(fields::NODE_ID, INDEXED | FAST | STORED);
            },
            PropertyIndex::new_node_property,
            &self.path,
        )
    }
}

#[cfg(test)]
mod graph_index_test {
    use crate::{
        db::{api::view::SearchableGraphOps, graph::views::filter::PropertyFilterOps},
        prelude::{AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps, PropertyFilter},
    };

    fn init_nodes_graph(graph: Graph) -> Graph {
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
        graph
    }

    fn init_edges_graph(graph: Graph) -> Graph {
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
        graph
    }

    #[test]
    fn test_if_bulk_load_create_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);

        let _ = graph.create_index().unwrap();
    }

    #[test]
    fn test_if_adding_nodes_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        let _ = graph.create_index().unwrap();

        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);
    }

    #[test]
    fn test_if_adding_edges_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        // Creates graph index
        let _ = graph.create_index().unwrap();

        let graph = init_edges_graph(graph);

        assert_eq!(graph.count_edges(), 3);
    }

    #[test]
    fn test_node_const_property_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);
        graph.create_index().unwrap();
        graph
            .node(1)
            .unwrap()
            .add_constant_properties([("x", 1u64)])
            .unwrap();

        let filter = PropertyFilter::property("x").constant().eq(1u64);
        let res = graph.search_nodes(filter, 20, 0).unwrap();
        let res = res.iter().map(|n| n.name()).collect::<Vec<_>>();
        assert_eq!(res, vec!["1"]);

        graph
            .node(1)
            .unwrap()
            .update_constant_properties([("x", 2u64)])
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(1u64);
        let res = graph.search_nodes(filter, 20, 0).unwrap();
        let res = res.iter().map(|n| n.name()).collect::<Vec<_>>();
        assert_eq!(res, Vec::<&str>::new());

        graph
            .node(1)
            .unwrap()
            .update_constant_properties([("x", 2u64)])
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(2u64);
        let res = graph.search_nodes(filter, 20, 0).unwrap();
        let res = res.iter().map(|n| n.name()).collect::<Vec<_>>();
        assert_eq!(res, vec!["1"]);
    }

    #[test]
    fn test_edge_const_property_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_edges_graph(graph);
        graph.create_index().unwrap();
        graph
            .edge(1, 2)
            .unwrap()
            .add_constant_properties([("x", 1u64)], Some("fire_nation"))
            .unwrap();

        let filter = PropertyFilter::property("x").constant().eq(1u64);
        let res = graph.search_edges(filter, 20, 0).unwrap();
        let res = res
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        assert_eq!(res, vec!["1->2"]);

        graph
            .edge(1, 2)
            .unwrap()
            .update_constant_properties([("x", 2u64)], Some("fire_nation"))
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(1u64);
        let res = graph.search_edges(filter, 20, 0).unwrap();
        let res = res
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        assert_eq!(res, Vec::<&str>::new());

        graph
            .edge(1, 2)
            .unwrap()
            .update_constant_properties([("x", 2u64)], Some("fire_nation"))
            .unwrap();
        let filter = PropertyFilter::property("x").constant().eq(2u64);
        let res = graph.search_edges(filter, 20, 0).unwrap();
        let res = res
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        assert_eq!(res, vec!["1->2"]);
    }
}
