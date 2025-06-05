use crate::{errors::GraphError, search::property_index::PropertyIndex};
use ahash::HashSet;
use std::{fs::create_dir_all, path::PathBuf};
use tantivy::{
    schema::Schema,
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
    Index, IndexReader, IndexSettings,
};

pub mod graph_index;
pub mod searcher;

mod collectors;
mod edge_filter_executor;
pub mod edge_index;
pub mod entity_index;
mod node_filter_executor;
pub mod node_index;
pub mod property_index;
mod query_builder;

pub(in crate::search) mod fields {
    pub const TIME: &str = "time";
    pub const SECONDARY_TIME: &str = "secondary_time";
    pub const NODE_ID: &str = "node_id";
    pub const NODE_NAME: &str = "node_name";
    pub const NODE_NAME_TOKENIZED: &str = "node_name_tokenized";
    pub const NODE_TYPE: &str = "node_type";
    pub const NODE_TYPE_TOKENIZED: &str = "node_type_tokenized";
    pub const EDGE_ID: &str = "edge_id";
    pub const SOURCE: &str = "src";
    pub const SOURCE_TOKENIZED: &str = "src_tokenized";
    pub const DESTINATION: &str = "dst";
    pub const DESTINATION_TOKENIZED: &str = "dst_tokenized";
    pub const LAYER_ID: &str = "layer_id";
}

pub(crate) const TOKENIZER: &str = "custom_default";

pub fn register_default_tokenizers(index: &Index) {
    let tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build();
    index.tokenizers().register(TOKENIZER, tokenizer);
}

pub(crate) fn new_index(
    schema: Schema,
    path: &Option<PathBuf>,
) -> Result<(Index, IndexReader), GraphError> {
    let index_builder = Index::builder()
        .settings(IndexSettings::default())
        .schema(schema);

    let index = if let Some(path) = path {
        create_dir_all(path).map_err(|e| {
            GraphError::IOErrorMsg(format!(
                "Failed to create index directory {}: {}",
                path.display(),
                e
            ))
        })?;

        index_builder.create_in_dir(path).map_err(|e| {
            GraphError::IndexErrorMsg(format!("Failed to create index in directory: {}", e))
        })?
    } else {
        index_builder.create_in_ram().map_err(|e| {
            GraphError::IndexErrorMsg(format!("Failed to create in-memory index: {}", e))
        })?
    };

    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()?;

    register_default_tokenizers(&index);

    Ok((index, reader))
}

fn resolve_props(props: &Vec<Option<PropertyIndex>>) -> HashSet<usize> {
    props
        .iter()
        .enumerate()
        .filter_map(|(idx, opt)| opt.as_ref().map(|_| idx))
        .collect()
}

#[cfg(test)]
mod test_index {
    #[cfg(feature = "search")]
    mod test_index_io {
        use crate::{
            db::{
                api::view::{internal::InternalStorageOps, StaticGraphViewOps},
                graph::views::filter::model::{AsNodeFilter, NodeFilter, NodeFilterBuilderOps},
            },
            errors::GraphError,
            prelude::*,
            serialise::GraphFolder,
        };
        use raphtory_api::core::{
            entities::properties::prop::Prop, storage::arc_str::ArcStr,
            utils::logging::global_info_logger,
        };

        fn init_graph<G>(graph: G) -> G
        where
            G: StaticGraphViewOps + AdditionOps + PropertyAdditionOps,
        {
            graph
                .add_node(
                    1,
                    "Alice",
                    vec![("p1", Prop::U64(2u64))],
                    Some("fire_nation"),
                )
                .unwrap();
            graph
        }

        fn assert_search_results<T: AsNodeFilter + Clone>(
            graph: &Graph,
            filter: &T,
            expected: Vec<&str>,
        ) {
            let res = graph
                .search_nodes(filter.clone(), 2, 0)
                .unwrap()
                .into_iter()
                .map(|n| n.name())
                .collect::<Vec<_>>();
            assert_eq!(res, expected);
        }

        #[test]
        fn test_create_no_index_persist_no_index_on_encode_load_no_index_on_decode() {
            // No index persisted since it was never created
            let graph = init_graph(Graph::new());

            let err = graph
                .search_nodes(NodeFilter::name().eq("Alice"), 2, 0)
                .expect_err("Expected error since index was not created");
            assert!(matches!(err, GraphError::IndexNotCreated));

            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            let graph = Graph::decode(path).unwrap();
            let index = graph.get_storage().unwrap().index.get();
            assert!(index.is_none());
        }

        #[test]
        fn test_create_index_persist_index_on_encode_load_index_on_decode() {
            let graph = init_graph(Graph::new());

            // Created index
            graph.create_index().unwrap();

            let filter = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter, vec!["Alice"]);

            // Persisted both graph and index
            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            // Loaded index that was persisted
            let graph = Graph::decode(path).unwrap();
            let index = graph.get_storage().unwrap().index.get();
            assert!(index.is_some());

            assert_search_results(&graph, &filter, vec!["Alice"]);
        }

        #[test]
        fn test_create_index_persist_index_on_encode_update_index_load_persisted_index_on_decode() {
            let graph = init_graph(Graph::new());

            // Created index
            graph.create_index().unwrap();

            let filter1 = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter1, vec!["Alice"]);

            // Persisted both graph and index
            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            // Updated both graph and index
            graph
                .add_node(
                    2,
                    "Tommy",
                    vec![("p1", Prop::U64(5u64))],
                    Some("water_tribe"),
                )
                .unwrap();
            let filter2 = NodeFilter::name().eq("Tommy");
            assert_search_results(&graph, &filter2, vec!["Tommy"]);

            // Loaded index that was persisted
            let graph = Graph::decode(path).unwrap();
            let index = graph.get_storage().unwrap().index.get();
            assert!(index.is_some());
            assert_search_results(&graph, &filter1, vec!["Alice"]);
            assert_search_results(&graph, &filter2, Vec::<&str>::new());

            // Updating and encode the graph and index should decode the updated the graph as well as index
            // So far we have the index that was created and persisted for the first time
            graph
                .add_node(
                    2,
                    "Tommy",
                    vec![("p1", Prop::U64(5u64))],
                    Some("water_tribe"),
                )
                .unwrap();
            let filter2 = NodeFilter::name().eq("Tommy");
            assert_search_results(&graph, &filter2, vec!["Tommy"]);

            // Should persist the updated graph and index
            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            // Should load the updated graph and index
            let graph = Graph::decode(path).unwrap();
            let index = graph.get_storage().unwrap().index.get();
            assert!(index.is_some());
            assert_search_results(&graph, &filter1, vec!["Alice"]);
            assert_search_results(&graph, &filter2, vec!["Tommy"]);
        }

        #[test]
        fn test_zip_encode_decode_index() {
            let graph = init_graph(Graph::new());
            graph.create_index().unwrap();
            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            let folder = GraphFolder::new_as_zip(path);
            graph.encode(folder.root_folder).unwrap();

            let graph = Graph::decode(path).unwrap();
            let node = graph.node("Alice").unwrap();
            let node_type = node.node_type();
            assert_eq!(node_type, Some(ArcStr::from("fire_nation")));

            let filter = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter, vec!["Alice"]);
        }

        #[test]
        fn test_create_index_in_ram() {
            global_info_logger();

            let graph = init_graph(Graph::new());
            graph.create_index_in_ram().unwrap();

            let filter = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter, vec!["Alice"]);

            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            let graph = Graph::decode(path).unwrap();
            let index = graph.get_storage().unwrap().index.get();
            assert!(index.is_none());

            let results = graph.search_nodes(filter.clone(), 2, 0);
            assert!(matches!(results, Err(GraphError::IndexNotCreated)));
        }

        #[test]
        fn test_cached_graph_view() {
            global_info_logger();
            let graph = init_graph(Graph::new());
            graph.create_index().unwrap();

            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            graph.cache(path).unwrap();

            graph
                .add_node(
                    2,
                    "Tommy",
                    vec![("p1", Prop::U64(5u64))],
                    Some("water_tribe"),
                )
                .unwrap();
            graph.write_updates().unwrap();

            let graph = Graph::decode(path).unwrap();
            let filter = NodeFilter::name().eq("Tommy");
            assert_search_results(&graph, &filter, vec!["Tommy"]);
        }

        #[test]
        fn test_cached_graph_view_create_index_after_graph_is_cached() {
            global_info_logger();
            let graph = init_graph(Graph::new());

            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            graph.cache(path).unwrap();
            // Creates index in a temp dir within graph dir
            graph.create_index().unwrap();

            graph
                .add_node(
                    2,
                    "Tommy",
                    vec![("p1", Prop::U64(5u64))],
                    Some("water_tribe"),
                )
                .unwrap();
            graph.write_updates().unwrap();

            let graph = Graph::decode(path).unwrap();
            let filter = NodeFilter::name().eq("Tommy");
            assert_search_results(&graph, &filter, vec!["Tommy"]);
        }
    }

    mod test_index_spec {
        #[cfg(feature = "search")]
        use crate::prelude::SearchableGraphOps;
        use crate::{
            db::{
                api::view::{IndexSpec, IndexSpecBuilder},
                graph::{
                    assertions::{filter_edges, filter_nodes, search_edges, search_nodes},
                    views::filter::model::{ComposableFilter, PropertyFilterOps},
                },
            },
            errors::GraphError,
            prelude::{AdditionOps, Graph, IndexMutationOps, PropertyFilter, StableDecode},
            serialise::{GraphFolder, StableEncode},
        };

        fn init_graph(graph: Graph) -> Graph {
            let nodes = vec![
                (
                    1,
                    "pometry",
                    [("p1", 5u64), ("p2", 50u64)],
                    Some("fire_nation"),
                    [("x", true)],
                ),
                (
                    1,
                    "raphtory",
                    [("p1", 10u64), ("p2", 100u64)],
                    Some("water_tribe"),
                    [("y", false)],
                ),
            ];

            for (time, name, props, group, const_props) in nodes {
                let node = graph.add_node(time, name, props, group).unwrap();
                node.add_constant_properties(const_props).unwrap();
            }

            let edges = vec![
                (
                    1,
                    "pometry",
                    "raphtory",
                    [("e_p1", 3.2f64), ("e_p2", 10f64)],
                    None,
                    [("e_x", true)],
                ),
                (
                    1,
                    "raphtory",
                    "pometry",
                    [("e_p1", 4.0f64), ("e_p2", 20f64)],
                    None,
                    [("e_y", false)],
                ),
            ];

            for (time, src, dst, props, label, const_props) in edges {
                let edge = graph.add_edge(time, src, dst, props, label).unwrap();
                edge.add_constant_properties(const_props, label).unwrap();
            }

            graph
        }

        #[test]
        fn test_with_all_props_index_spec() {
            let graph = init_graph(Graph::new());
            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_all_node_props()
                .with_all_edge_props()
                .build();
            assert_eq!(
                index_spec.props(&graph),
                vec![
                    vec!["x", "y"],
                    vec!["p1", "p2"],
                    vec!["e_x", "e_y"],
                    vec!["e_p1", "e_p2"]
                ]
            );
            graph.create_index_in_ram_with_spec(index_spec).unwrap();

            let filter = PropertyFilter::property("p1")
                .eq(5u64)
                .and(PropertyFilter::property("x").eq(true));
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["pometry"]);

            let filter = PropertyFilter::property("e_p1")
                .lt(5f64)
                .and(PropertyFilter::property("e_y").eq(false));
            let results = search_edges(&graph, filter);
            assert_eq!(results, vec!["raphtory->pometry"]);
        }

        #[test]
        fn test_with_selected_props_index_spec() {
            let graph = init_graph(Graph::new());
            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["y"])
                .unwrap()
                .with_temp_node_props(vec!["p1"])
                .unwrap()
                .with_const_edge_props(vec!["e_y"])
                .unwrap()
                .with_temp_edge_props(vec!["e_p1"])
                .unwrap()
                .build();
            assert_eq!(
                index_spec.props(&graph),
                vec![vec!["y"], vec!["p1"], vec!["e_y"], vec!["e_p1"]]
            );
            graph.create_index_in_ram_with_spec(index_spec).unwrap();

            let filter = PropertyFilter::property("p1")
                .eq(5u64)
                .or(PropertyFilter::property("y").eq(false));
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["pometry", "raphtory"]);

            let filter = PropertyFilter::property("y").eq(false);
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["raphtory"]);

            let filter = PropertyFilter::property("e_p1")
                .lt(5f64)
                .or(PropertyFilter::property("e_y").eq(false));
            let results = search_edges(&graph, filter);
            assert_eq!(results, vec!["pometry->raphtory", "raphtory->pometry"]);
        }

        #[test]
        fn test_with_invalid_property_returns_error() {
            let graph = init_graph(Graph::new());
            let result = IndexSpecBuilder::new(graph.clone()).with_const_node_props(["xyz"]);

            assert!(matches!(result, Err(GraphError::PropertyMissingError(p)) if p == "xyz"));
        }

        #[test]
        fn test_build_empty_spec_by_default() {
            let graph = init_graph(Graph::new());
            let index_spec = IndexSpecBuilder::new(graph.clone()).build();

            assert!(index_spec.node_const_props.is_empty());
            assert!(index_spec.node_temp_props.is_empty());
            assert!(index_spec.edge_const_props.is_empty());
            assert!(index_spec.edge_temp_props.is_empty());

            graph.create_index_in_ram_with_spec(index_spec).unwrap();

            let filter = PropertyFilter::property("p1")
                .eq(5u64)
                .and(PropertyFilter::property("x").eq(true));
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["pometry"]);

            let filter = PropertyFilter::property("e_p1")
                .lt(5f64)
                .or(PropertyFilter::property("e_y").eq(false));
            let results = search_edges(&graph, filter);
            assert_eq!(results, vec!["pometry->raphtory", "raphtory->pometry"]);
        }

        #[test]
        fn test_mixed_node_and_edge_props_index_spec() {
            let graph = init_graph(Graph::new());

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["x"])
                .unwrap()
                .with_all_temp_node_props()
                .with_all_edge_props()
                .build();
            assert_eq!(
                index_spec.props(&graph),
                vec![
                    vec!["x"],
                    vec!["p1", "p2"],
                    vec!["e_x", "e_y"],
                    vec!["e_p1", "e_p2"]
                ]
            );

            graph.create_index_in_ram_with_spec(index_spec).unwrap();

            let filter = PropertyFilter::property("p1")
                .eq(5u64)
                .or(PropertyFilter::property("y").eq(false));
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["pometry", "raphtory"]);

            let filter = PropertyFilter::property("e_p1")
                .lt(5f64)
                .or(PropertyFilter::property("e_y").eq(false));
            let results = search_edges(&graph, filter);
            assert_eq!(results, vec!["pometry->raphtory", "raphtory->pometry"]);
        }

        #[test]
        fn test_get_index_spec_newly_created_index() {
            let graph = init_graph(Graph::new());

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["x"])
                .unwrap()
                .with_all_temp_node_props()
                .with_all_edge_props()
                .build();

            graph
                .create_index_in_ram_with_spec(index_spec.clone())
                .unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
        }

        #[test]
        fn test_get_index_spec_updated_index() {
            let graph = init_graph(Graph::new());

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_edge_props(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let results = search_nodes(&graph, PropertyFilter::property("y").eq(false));
            assert_eq!(results, vec!["raphtory"]);
            let results = search_edges(&graph, PropertyFilter::property("e_y").eq(false));
            assert_eq!(results, vec!["raphtory->pometry"]);

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["y"])
                .unwrap()
                .with_temp_node_props(vec!["p2"])
                .unwrap()
                .with_const_edge_props(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let results = search_nodes(&graph, PropertyFilter::property("y").eq(false));
            assert_eq!(results, vec!["raphtory"]);
            let results = search_edges(&graph, PropertyFilter::property("e_y").eq(false));
            assert_eq!(results, vec!["raphtory->pometry"]);
        }

        #[test]
        fn test_get_index_spec_updated_index_persisted_and_loaded() {
            let graph = init_graph(Graph::new());

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_edge_props(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();

            let tmp_graph_dir = tempfile::tempdir().unwrap();
            let path = tmp_graph_dir.path().to_path_buf();
            graph.encode(path.clone()).unwrap();
            let graph = Graph::decode(path.clone()).unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let results = search_nodes(&graph, PropertyFilter::property("y").eq(false));
            assert_eq!(results, vec!["raphtory"]);
            let results = search_edges(&graph, PropertyFilter::property("e_y").eq(false));
            assert_eq!(results, vec!["raphtory->pometry"]);

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["y"])
                .unwrap()
                .with_temp_node_props(vec!["p2"])
                .unwrap()
                .with_const_edge_props(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();
            let tmp_graph_dir = tempfile::tempdir().unwrap();
            let path = tmp_graph_dir.path().to_path_buf();
            graph.encode(path.clone()).unwrap();
            let graph = Graph::decode(path).unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let results = search_nodes(&graph, PropertyFilter::property("y").eq(false));
            assert_eq!(results, vec!["raphtory"]);
            let results = search_edges(&graph, PropertyFilter::property("e_y").eq(false));
            assert_eq!(results, vec!["raphtory->pometry"]);
        }

        #[test]
        fn test_get_index_spec_loaded_index() {
            let graph = init_graph(Graph::new());

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["y"])
                .unwrap()
                .with_temp_node_props(vec!["p2"])
                .unwrap()
                .with_const_edge_props(vec!["e_y"])
                .unwrap()
                .with_temp_edge_props(vec!["e_p2"])
                .unwrap()
                .build();

            graph.create_index_with_spec(index_spec.clone()).unwrap();
            let tmp_graph_dir = tempfile::tempdir().unwrap();
            let path = tmp_graph_dir.path().to_path_buf();
            graph.encode(path.clone()).unwrap();

            let graph = Graph::decode(path).unwrap();
            let index_spec2 = graph.get_index_spec().unwrap();

            assert_eq!(index_spec, index_spec2);
        }

        #[test]
        fn test_get_index_spec_loaded_index_zip() {
            let graph = init_graph(Graph::new());

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["y"])
                .unwrap()
                .with_temp_node_props(vec!["p2"])
                .unwrap()
                .with_const_edge_props(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();

            let binding = tempfile::TempDir::new().unwrap();
            let path = binding.path();
            let folder = GraphFolder::new_as_zip(path);
            graph.encode(folder.root_folder).unwrap();

            let graph = Graph::decode(path).unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
        }

        #[test]
        fn test_no_new_node_prop_index_created_via_update_apis() {
            run_node_index_test(|graph, index_spec| {
                graph.create_index_with_spec(index_spec.clone())
            });

            run_node_index_test(|graph, index_spec| {
                graph.create_index_in_ram_with_spec(index_spec.clone())
            });
        }

        #[test]
        fn test_no_new_edge_prop_index_created_via_update_apis() {
            run_edge_index_test(|graph, index_spec| {
                graph.create_index_with_spec(index_spec.clone())
            });

            run_edge_index_test(|graph, index_spec| {
                graph.create_index_in_ram_with_spec(index_spec.clone())
            });
        }

        fn run_node_index_test<F>(create_index_fn: F)
        where
            F: Fn(&Graph, IndexSpec) -> Result<(), GraphError>,
        {
            let graph = init_graph(Graph::new());

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["y"])
                .unwrap()
                .with_temp_node_props(vec!["p1"])
                .unwrap()
                .build();
            create_index_fn(&graph, index_spec.clone()).unwrap();

            let filter = PropertyFilter::property("p2").temporal().latest().eq(50u64);
            assert_eq!(search_nodes(&graph, filter.clone()), vec!["pometry"]);

            let node = graph
                .add_node(1, "shivam", [("p1", 100u64)], Some("fire_nation"))
                .unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());

            let filter = PropertyFilter::property("p1")
                .temporal()
                .latest()
                .eq(100u64);
            assert_eq!(search_nodes(&graph, filter.clone()), vec!["shivam"]);

            node.add_constant_properties([("z", true)]).unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = PropertyFilter::property("z").constant().eq(true);
            assert_eq!(search_nodes(&graph, filter.clone()), vec!["shivam"]);

            node.update_constant_properties([("z", false)]).unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = PropertyFilter::property("z").constant().eq(false);
            assert_eq!(search_nodes(&graph, filter.clone()), vec!["shivam"]);
        }

        fn run_edge_index_test<F>(create_index_fn: F)
        where
            F: Fn(&Graph, IndexSpec) -> Result<(), GraphError>,
        {
            let graph = init_graph(Graph::new());

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["y"])
                .unwrap()
                .with_temp_node_props(vec!["p2"])
                .unwrap()
                .build();
            create_index_fn(&graph, index_spec.clone()).unwrap();

            let edge = graph
                .add_edge(1, "shivam", "kapoor", [("p1", 100u64)], None)
                .unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = PropertyFilter::property("p1")
                .temporal()
                .latest()
                .eq(100u64);
            assert_eq!(search_edges(&graph, filter.clone()), vec!["shivam->kapoor"]);

            edge.add_constant_properties([("z", true)], None).unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = PropertyFilter::property("z").constant().eq(true);
            assert_eq!(search_edges(&graph, filter.clone()), vec!["shivam->kapoor"]);

            edge.update_constant_properties([("z", false)], None)
                .unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = PropertyFilter::property("z").constant().eq(false);
            assert_eq!(search_edges(&graph, filter.clone()), vec!["shivam->kapoor"]);
        }

        #[test]
        fn test_const_prop_fallback_when_const_prop_indexed() {
            let graph = init_graph(Graph::new());

            let spec = IndexSpecBuilder::new(graph.clone())
                .with_const_node_props(vec!["x"])
                .unwrap()
                .with_const_edge_props(vec!["e_y"])
                .unwrap()
                .build();

            graph.create_index_in_ram_with_spec(spec).unwrap();

            let f1 = PropertyFilter::property("x").eq(true);
            assert_eq!(
                filter_nodes(&graph, f1.clone()),
                vec!["pometry".to_string()]
            );
            assert_eq!(search_nodes(&graph, f1), vec!["pometry".to_string()]);

            let f2 = PropertyFilter::property("e_y").eq(false);
            assert_eq!(
                filter_edges(&graph, f2.clone()),
                vec!["raphtory->pometry".to_string()]
            );
            assert_eq!(
                search_edges(&graph, f2),
                vec!["raphtory->pometry".to_string()]
            );
        }

        #[test]
        fn test_const_prop_fallback_when_const_prop_not_indexed() {
            let graph = init_graph(Graph::new());

            let spec = IndexSpecBuilder::new(graph.clone())
                .with_all_temp_node_props()
                .with_all_temp_edge_props()
                .build();

            graph.create_index_in_ram_with_spec(spec).unwrap();

            let f1 = PropertyFilter::property("x").eq(true);
            assert_eq!(
                filter_nodes(&graph, f1.clone()),
                vec!["pometry".to_string()]
            );
            assert_eq!(search_nodes(&graph, f1), vec!["pometry".to_string()]);
            let f2 = PropertyFilter::property("e_y").eq(false);
            assert_eq!(
                filter_edges(&graph, f2.clone()),
                vec!["raphtory->pometry".to_string()]
            );
            assert_eq!(
                search_edges(&graph, f2),
                vec!["raphtory->pometry".to_string()]
            );
        }
    }
}
