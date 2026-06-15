#[cfg(all(test, feature = "search"))]
mod graph_index_test {
    use raphtory::prelude::{AdditionOps, Graph, GraphViewOps, IndexMutationOps};

    use raphtory::db::graph::views::filter::model::{
        edge_filter::EdgeFilter, node_filter::NodeFilter, property_filter::ops::PropertyFilterOps,
        PropertyFilterFactory,
    };
    use raphtory_tests::assertions::{search_edges, search_nodes};

    fn init_nodes_graph(graph: Graph) -> Graph {
        graph
            .add_node(1, 1, [("p1", 1), ("p2", 2)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(2, 1, [("p6", 6)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(2, 2, [("p4", 5)], Some("fire_nation"), None)
            .unwrap();
        graph
            .add_node(3, 3, [("p2", 4), ("p3", 3)], Some("water_tribe"), None)
            .unwrap();
        graph
    }

    fn init_edges_graph(graph: Graph) -> Graph {
        graph
            .add_edge(1, 1, 2, [("p1", 1), ("p2", 2)], None)
            .unwrap();
        graph.add_edge(2, 1, 2, [("p6", 6)], None).unwrap();
        graph.add_edge(2, 2, 3, [("p4", 5)], None).unwrap();
        graph
            .add_edge(3, 3, 4, [("p2", 4), ("p3", 3)], None)
            .unwrap();
        graph
    }

    #[test]
    fn test_if_bulk_load_create_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);

        graph.create_index_in_ram().unwrap();
    }

    #[test]
    fn test_if_adding_nodes_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        graph.create_index_in_ram().unwrap();

        let graph = init_nodes_graph(graph);

        assert_eq!(graph.count_nodes(), 3);
    }

    #[test]
    fn test_if_adding_edges_to_existing_graph_index_is_ok() {
        let graph = Graph::new();
        // Creates graph index
        graph.create_index_in_ram().unwrap();

        let graph = init_edges_graph(graph);

        assert_eq!(graph.count_edges(), 3);
    }

    #[test]
    fn test_node_metadata_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_nodes_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph.node(1).unwrap().add_metadata([("x", 1u64)]).unwrap();

        let filter = NodeFilter.metadata("x").eq(1u64);
        assert_eq!(search_nodes(&graph, filter.clone()), vec!["1"]);

        graph
            .node(1)
            .unwrap()
            .update_metadata([("x", 2u64)])
            .unwrap();
        let filter = NodeFilter.metadata("x").eq(1u64);
        assert_eq!(search_nodes(&graph, filter.clone()), Vec::<&str>::new());

        graph
            .node(1)
            .unwrap()
            .update_metadata([("x", 2u64)])
            .unwrap();
        let filter = NodeFilter.metadata("x").eq(2u64);
        assert_eq!(search_nodes(&graph, filter.clone()), vec!["1"]);
    }

    #[test]
    fn test_edge_metadata_graph_index_is_ok() {
        let graph = Graph::new();
        let graph = init_edges_graph(graph);
        graph.create_index_in_ram().unwrap();
        graph
            .edge(1, 2)
            .unwrap()
            .add_metadata([("x", 1u64)], None)
            .unwrap();

        let filter = EdgeFilter.metadata("x").eq(1u64);
        assert_eq!(search_edges(&graph, filter.clone()), vec!["1->2"]);

        graph
            .edge(1, 2)
            .unwrap()
            .update_metadata([("x", 2u64)], None)
            .unwrap();
        let filter = EdgeFilter.metadata("x").eq(1u64);
        assert_eq!(search_edges(&graph, filter.clone()), Vec::<&str>::new());

        graph
            .edge(1, 2)
            .unwrap()
            .update_metadata([("x", 2u64)], None)
            .unwrap();
        let filter = EdgeFilter.metadata("x").eq(2u64);
        assert_eq!(search_edges(&graph, filter.clone()), vec!["1->2"]);
    }
}

#[cfg(all(test, feature = "search"))]
mod test_index {
    mod test_index_io {
        use raphtory::{
            db::{
                api::view::{internal::InternalStorageOps, ResolvedIndexSpec},
                graph::views::filter::model::{
                    node_filter::{ops::NodeFilterOps, NodeFilter},
                    TryAsCompositeFilter,
                },
            },
            errors::GraphError,
            prelude::*,
            serialise::GraphFolder,
        };
        use raphtory_api::core::{
            entities::properties::prop::Prop, storage::arc_str::ArcStr,
            utils::logging::global_info_logger,
        };
        use tempfile::TempDir;

        fn init_graph() -> Graph {
            let graph = Graph::new();

            graph
                .add_node(
                    1,
                    "Alice",
                    vec![("p1", Prop::U64(1000u64))],
                    Some("fire_nation"),
                    None,
                )
                .unwrap();
            graph
        }

        fn assert_search_results<T: TryAsCompositeFilter + Clone>(
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
            let graph = init_graph();

            let filter = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter, vec!["Alice"]);

            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            let graph = Graph::decode(path).unwrap();
            let is_indexed = graph.get_storage().unwrap().is_indexed();
            assert!(!is_indexed);
        }

        #[test]
        fn test_create_index_persist_index_on_encode_load_index_on_decode() {
            let graph = init_graph();

            // Created index
            graph.create_index().unwrap();

            let filter = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter, vec!["Alice"]);

            // Persisted both graph and index
            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            // Loaded index that was persisted
            let graph = Graph::decode(path).unwrap();
            let is_indexed = graph.get_storage().unwrap().is_indexed();
            assert!(is_indexed);

            assert_search_results(&graph, &filter, vec!["Alice"]);
        }

        #[test]
        fn test_encoding_graph_twice_to_same_storage_path_fails() {
            let graph = init_graph();
            graph.create_index().unwrap();
            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();
            let result = graph.encode(path);

            match result {
                Err(GraphError::NonEmptyGraphFolder(err_path)) => {
                    assert_eq!(path, err_path);
                }
                Ok(_) => panic!("Expected error on second encode, got Ok"),
                Err(e) => panic!("Unexpected error type: {:?}", e),
            }
        }

        #[test]
        fn test_create_index_persist_index_on_encode_update_index_load_persisted_index_on_decode() {
            let graph = init_graph();

            // Created index
            graph.create_index().unwrap();

            let filter1 = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter1, vec!["Alice"]);

            // Persisted both graph and index
            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            // Updated both graph and index
            graph
                .add_node(
                    2,
                    "Tommy",
                    vec![("p1", Prop::U64(5u64))],
                    Some("water_tribe"),
                    None,
                )
                .unwrap();
            let filter2 = NodeFilter::name().eq("Tommy");
            assert_search_results(&graph, &filter2, vec!["Tommy"]);

            // Loaded index that was persisted
            let graph = Graph::decode(path).unwrap();
            let is_indexed = graph.get_storage().unwrap().is_indexed();
            assert!(is_indexed);
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
                    None,
                )
                .unwrap();
            let filter2 = NodeFilter::name().eq("Tommy");
            assert_search_results(&graph, &filter2, vec!["Tommy"]);

            // Should persist the updated graph and index
            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            // Should load the updated graph and index
            let graph = Graph::decode(path).unwrap();
            let is_indexed = graph.get_storage().unwrap().is_indexed();
            assert!(is_indexed);
            assert_search_results(&graph, &filter1, vec!["Alice"]);
            assert_search_results(&graph, &filter2, vec!["Tommy"]);
        }

        #[test]
        fn test_zip_encode_decode_index() {
            let graph = init_graph();
            graph.create_index().unwrap();
            let tmp_dir = TempDir::new().unwrap();
            let zip_path = tmp_dir.path().join("graph.zip");
            let folder = GraphFolder::new_as_zip(zip_path);
            graph.encode(&folder).unwrap();

            let graph = Graph::decode(&folder).unwrap();
            let node = graph.node("Alice").unwrap();
            let node_type = node.node_type();
            assert_eq!(node_type, Some(ArcStr::from("fire_nation")));

            let filter = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter, vec!["Alice"]);
        }

        #[test]
        fn test_encoding_graph_twice_to_same_storage_path_fails_zip() {
            let graph = init_graph();
            graph.create_index().unwrap();
            let tmp_dir = TempDir::new().unwrap();
            let zip_path = tmp_dir.path().join("graph.zip");
            let folder = GraphFolder::new_as_zip(&zip_path);
            graph.encode(&folder).unwrap();
            graph
                .add_node(1, "Ozai", [("prop", 1)], Some("fire_nation"), None)
                .unwrap();
            let result = graph.encode(folder);
            match result {
                Err(GraphError::IOError { source, .. }) => {
                    assert!(
                        format!("{source}").to_lowercase().contains("file exists"),
                        "{}",
                        source
                    );
                }
                Ok(_) => panic!("Expected error on second encode, got Ok"),
                Err(e) => panic!("Unexpected error type: {:?}", e),
            }
        }

        #[test]
        fn test_immutable_graph_index_persistence() {
            let graph = init_graph();
            graph.create_index().unwrap();

            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            // This gives us immutable index
            let graph = Graph::decode(path).unwrap();

            // This tests that we are able to persist the immutable index
            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            let graph = Graph::decode(path).unwrap();
            let filter1 = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter1, vec!["Alice"]);
        }

        #[test]
        fn test_mutable_graph_index_persistence() {
            let graph = init_graph();
            graph.create_index().unwrap();

            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            // This gives us immutable index
            let graph = Graph::decode(path).unwrap();

            // This converts immutable index to mutable index
            graph
                .add_node(1, "Ozai", [("prop", 1)], Some("fire_nation"), None)
                .unwrap();

            // This tests that we are able to persist the mutable index
            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            let graph = Graph::decode(path).unwrap();
            let filter = NodeFilter::name().eq("Ozai");
            assert_search_results(&graph, &filter, vec!["Ozai"]);
        }

        #[test]
        fn test_loading_zip_index_creates_mutable_index() {
            let graph = init_graph();
            graph.create_index().unwrap();
            let tmp_dir = TempDir::new().unwrap();
            let zip_path = tmp_dir.path().join("graph.zip");
            let folder = GraphFolder::new_as_zip(&zip_path);
            graph.encode(&folder).unwrap();

            let graph = Graph::decode(&folder).unwrap();
            let immutable = graph
                .get_storage()
                .unwrap()
                .index()
                .read_recursive()
                .is_immutable();
            assert! {!immutable};
        }

        #[test]
        fn test_loading_index_creates_immutable_index() {
            let graph = init_graph();
            graph.create_index().unwrap();
            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            let graph = Graph::decode(path).unwrap();
            let immutable = graph
                .get_storage()
                .unwrap()
                .index()
                .read_recursive()
                .is_immutable();
            assert! {immutable};
        }

        #[test]
        fn test_create_index_in_ram() {
            global_info_logger();

            let graph = init_graph();
            graph.create_index_in_ram().unwrap();

            let filter = NodeFilter::name().eq("Alice");
            assert_search_results(&graph, &filter, vec!["Alice"]);

            let binding = TempDir::new().unwrap();
            let path = binding.path();
            graph.encode(path).unwrap();

            let graph = Graph::decode(path).unwrap();
            let is_indexed = graph.get_storage().unwrap().is_indexed();
            assert!(!is_indexed);

            assert_search_results(&graph, &filter, vec!["Alice"]);
        }

        #[test]
        #[ignore]
        fn test_too_many_open_files_graph_index() {
            let mut graphs = vec![];

            for _ in 0..1000 {
                let graph = init_graph();
                if let Err(e) = graph.create_index() {
                    match &e {
                        GraphError::IndexError { source } => {
                            panic!("Hit file descriptor limit after {} graphs. {:?}", 0, source);
                        }
                        other => {
                            panic!("Unexpected GraphError: {:?}", other);
                        }
                    }
                }
                graphs.push(graph);
            }
        }

        #[test]
        fn test_graph_index_creation_with_too_many_properties() {
            let graph = init_graph();
            let props: Vec<(String, Prop)> = (1..=100)
                .map(|i| (format!("p{i}"), Prop::U64(i as u64)))
                .collect();
            graph.node("Alice").unwrap().add_metadata(props).unwrap();

            if let Err(e) = graph.create_index() {
                match &e {
                    GraphError::IndexError { source } => {
                        panic!("Hit file descriptor limit after {} graphs. {:?}", 0, source);
                    }
                    other => {
                        panic!("Unexpected GraphError: {:?}", other);
                    }
                }
            }
        }

        #[test]
        // No new const prop index created because when index were created
        // these properties did not exist.
        fn test_graph_index_creation_for_incremental_node_update_no_new_prop_indexed() {
            let graph = init_graph();
            graph.create_index().unwrap();
            let props: Vec<(String, Prop)> = (1..=100)
                .map(|i| (format!("p{i}"), Prop::U64(i as u64)))
                .collect();
            graph.node("Alice").unwrap().add_metadata(props).unwrap();

            let tmp_dir = TempDir::new().unwrap();
            let path = tmp_dir.path().to_path_buf();
            graph.encode(&path).unwrap();
            let graph = Graph::decode(&path).unwrap();

            let spec = graph.get_index_spec().unwrap().props(&graph);
            assert_eq!(
                spec,
                ResolvedIndexSpec {
                    node_properties: vec!["p1".to_string()],
                    node_metadata: vec![],
                    edge_metadata: vec![],
                    edge_properties: vec![]
                }
            );
        }
    }

    mod test_index_spec {
        use raphtory::{
            db::{
                api::view::{IndexSpec, IndexSpecBuilder},
                graph::views::filter::model::{
                    edge_filter::EdgeFilter, node_filter::NodeFilter,
                    property_filter::ops::PropertyFilterOps, ComposableFilter,
                    PropertyFilterFactory, TemporalPropertyFilterFactory,
                },
            },
            errors::GraphError,
            prelude::{AdditionOps, Graph, IndexMutationOps, SearchableGraphOps, StableDecode},
            serialise::{GraphFolder, StableEncode},
        };
        use raphtory_tests::assertions::{search_edges, search_nodes};
        use tempfile::{tempdir, TempDir};

        fn init_graph() -> Graph {
            let graph = Graph::new();

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

            for (time, name, props, group, metadata) in nodes {
                let node = graph.add_node(time, name, props, group, None).unwrap();
                node.add_metadata(metadata).unwrap();
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

            for (time, src, dst, props, label, metadata) in edges {
                let edge = graph.add_edge(time, src, dst, props, label).unwrap();
                edge.add_metadata(metadata, label).unwrap();
            }

            graph
        }

        #[test]
        #[ignore = "TODO: #2372"]
        fn test_with_all_props_index_spec() {
            let graph = init_graph();
            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_all_node_properties_and_metadata()
                .with_all_edge_properties_and_metadata()
                .build();
            assert_eq!(
                index_spec.props(&graph).to_vec(),
                vec![
                    vec!["x", "y"],
                    vec!["p1", "p2"],
                    vec!["e_x", "e_y"],
                    vec!["e_p1", "e_p2"]
                ]
            );
            graph.create_index_in_ram_with_spec(index_spec).unwrap();

            let filter = NodeFilter
                .property("p1")
                .eq(5u64)
                .and(NodeFilter.metadata("x").eq(true));
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["pometry"]);

            let filter = EdgeFilter
                .property("e_p1")
                .lt(5f64)
                .and(EdgeFilter.metadata("e_y").eq(false));
            let results = search_edges(&graph, filter);
            assert_eq!(results, vec!["raphtory->pometry"]);
        }

        #[test]
        #[ignore = "TODO: #2372"]
        fn test_with_selected_props_index_spec() {
            let graph = init_graph();
            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["y"])
                .unwrap()
                .with_node_properties(vec!["p1"])
                .unwrap()
                .with_edge_metadata(vec!["e_y"])
                .unwrap()
                .with_edge_properties(vec!["e_p1"])
                .unwrap()
                .build();
            assert_eq!(
                index_spec.props(&graph).to_vec(),
                vec![vec!["y"], vec!["p1"], vec!["e_y"], vec!["e_p1"]]
            );
            graph.create_index_in_ram_with_spec(index_spec).unwrap();

            let filter = NodeFilter
                .property("p1")
                .eq(5u64)
                .or(NodeFilter.metadata("y").eq(false));
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["pometry", "raphtory"]);

            let filter = NodeFilter.metadata("y").eq(false);
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["raphtory"]);

            let filter = EdgeFilter
                .property("e_p1")
                .lt(5f64)
                .or(EdgeFilter.metadata("e_y").eq(false));
            let results = search_edges(&graph, filter);
            assert_eq!(results, vec!["pometry->raphtory", "raphtory->pometry"]);
        }

        #[test]
        fn test_with_invalid_property_returns_error() {
            let graph = init_graph();
            let result = IndexSpecBuilder::new(graph.clone()).with_node_metadata(["xyz"]);

            assert!(matches!(result, Err(GraphError::PropertyMissingError(p)) if p == "xyz"));
        }

        #[test]
        fn test_build_empty_spec_by_default() {
            let graph = init_graph();
            let index_spec = IndexSpecBuilder::new(graph.clone()).build();

            assert!(index_spec.node_metadata().is_empty());
            assert!(index_spec.node_properties().is_empty());
            assert!(index_spec.edge_metadata().is_empty());
            assert!(index_spec.edge_properties().is_empty());

            graph.create_index_in_ram_with_spec(index_spec).unwrap();

            let filter = NodeFilter
                .property("p1")
                .eq(5u64)
                .and(NodeFilter.metadata("x").eq(true));
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["pometry"]);

            let filter = EdgeFilter
                .property("e_p1")
                .lt(5f64)
                .or(EdgeFilter.metadata("e_y").eq(false));
            let results = search_edges(&graph, filter);
            assert_eq!(results, vec!["pometry->raphtory", "raphtory->pometry"]);
        }

        #[test]
        #[ignore = "TODO: #2372"]
        fn test_mixed_node_and_edge_props_index_spec() {
            let graph = init_graph();

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["x"])
                .unwrap()
                .with_all_node_properties()
                .with_all_edge_properties_and_metadata()
                .build();
            assert_eq!(
                index_spec.props(&graph).to_vec(),
                vec![
                    vec!["x"],
                    vec!["p1", "p2"],
                    vec!["e_x", "e_y"],
                    vec!["e_p1", "e_p2"]
                ]
            );

            graph.create_index_in_ram_with_spec(index_spec).unwrap();

            let filter = NodeFilter
                .property("p1")
                .eq(5u64)
                .or(NodeFilter.metadata("y").eq(false));
            let results = search_nodes(&graph, filter);
            assert_eq!(results, vec!["pometry", "raphtory"]);

            let filter = EdgeFilter
                .property("e_p1")
                .lt(5f64)
                .or(EdgeFilter.metadata("e_y").eq(false));
            let results = search_edges(&graph, filter);
            assert_eq!(results, vec!["pometry->raphtory", "raphtory->pometry"]);
        }

        #[test]
        fn test_get_index_spec_newly_created_index() {
            let graph = init_graph();

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["x"])
                .unwrap()
                .with_all_node_properties()
                .with_all_edge_properties_and_metadata()
                .build();

            graph
                .create_index_in_ram_with_spec(index_spec.clone())
                .unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
        }

        #[test]
        #[ignore = "TODO: #2372"]
        fn test_get_index_spec_updated_index() {
            let graph = init_graph();

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_edge_metadata(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let results = search_nodes(&graph, NodeFilter.metadata("y").eq(false));
            assert_eq!(results, vec!["raphtory"]);
            let results = search_edges(&graph, EdgeFilter.metadata("e_y").eq(false));
            assert_eq!(results, vec!["raphtory->pometry"]);

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["y"])
                .unwrap()
                .with_node_properties(vec!["p2"])
                .unwrap()
                .with_edge_metadata(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let results = search_nodes(&graph, NodeFilter.metadata("y").eq(false));
            assert_eq!(results, vec!["raphtory"]);
            let results = search_edges(&graph, EdgeFilter.metadata("e_y").eq(false));
            assert_eq!(results, vec!["raphtory->pometry"]);
        }

        #[test]
        #[ignore = "TODO: #2372"]
        fn test_get_index_spec_updated_index_persisted_and_loaded() {
            let graph = init_graph();

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_edge_metadata(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();

            let tmp_graph_dir = tempdir().unwrap();
            let path = tmp_graph_dir.path().to_path_buf();
            graph.encode(&path).unwrap();
            let graph = Graph::decode(&path).unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let results = search_nodes(&graph, NodeFilter.metadata("y").eq(false));
            assert_eq!(results, vec!["raphtory"]);
            let results = search_edges(&graph, EdgeFilter.metadata("e_y").eq(false));
            assert_eq!(results, vec!["raphtory->pometry"]);

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["y"])
                .unwrap()
                .with_node_properties(vec!["p2"])
                .unwrap()
                .with_edge_metadata(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();
            let tmp_graph_dir = tempdir().unwrap();
            let path = tmp_graph_dir.path().to_path_buf();
            graph.encode(path.clone()).unwrap();
            let graph = Graph::decode(&path).unwrap();

            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let results = search_nodes(&graph, NodeFilter.metadata("y").eq(false));
            assert_eq!(results, vec!["raphtory"]);
            let results = search_edges(&graph, EdgeFilter.metadata("e_y").eq(false));
            assert_eq!(results, vec!["raphtory->pometry"]);
        }

        #[test]
        fn test_get_index_spec_loaded_index() {
            let graph = init_graph();

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["y"])
                .unwrap()
                .with_node_properties(vec!["p2"])
                .unwrap()
                .with_edge_metadata(vec!["e_y"])
                .unwrap()
                .with_edge_properties(vec!["e_p2"])
                .unwrap()
                .build();

            graph.create_index_with_spec(index_spec.clone()).unwrap();
            let tmp_graph_dir = tempdir().unwrap();
            let path = tmp_graph_dir.path().to_path_buf();
            graph.encode(path.clone()).unwrap();

            let graph = Graph::decode(&path).unwrap();
            let index_spec2 = graph.get_index_spec().unwrap();

            assert_eq!(index_spec, index_spec2);
        }

        #[test]
        fn test_get_index_spec_loaded_index_zip() {
            let graph = init_graph();

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["y"])
                .unwrap()
                .with_node_properties(vec!["p2"])
                .unwrap()
                .with_edge_metadata(vec!["e_y"])
                .unwrap()
                .build();
            graph.create_index_with_spec(index_spec.clone()).unwrap();

            let binding = TempDir::new().unwrap();
            let path = binding.path();
            let folder = GraphFolder::new_as_zip(path);
            graph.encode(folder).unwrap();

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
            let graph = init_graph();

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["y"])
                .unwrap()
                .with_node_properties(vec!["p1"])
                .unwrap()
                .build();
            create_index_fn(&graph, index_spec.clone()).unwrap();

            let filter = NodeFilter.property("p2").temporal().last().eq(50u64);
            assert_eq!(search_nodes(&graph, filter.clone()), vec!["pometry"]);

            let node = graph
                .add_node(1, "shivam", [("p1", 100u64)], Some("fire_nation"), None)
                .unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());

            let filter = NodeFilter.property("p1").temporal().last().eq(100u64);
            assert_eq!(search_nodes(&graph, filter.clone()), vec!["shivam"]);

            node.add_metadata([("z", true)]).unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = NodeFilter.metadata("z").eq(true);
            assert_eq!(search_nodes(&graph, filter.clone()), vec!["shivam"]);

            node.update_metadata([("z", false)]).unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = NodeFilter.metadata("z").eq(false);
            assert_eq!(search_nodes(&graph, filter.clone()), vec!["shivam"]);
        }

        fn run_edge_index_test<F>(create_index_fn: F)
        where
            F: Fn(&Graph, IndexSpec) -> Result<(), GraphError>,
        {
            let graph = init_graph();

            let index_spec = IndexSpecBuilder::new(graph.clone())
                .with_node_metadata(vec!["y"])
                .unwrap()
                .with_node_properties(vec!["p2"])
                .unwrap()
                .build();
            create_index_fn(&graph, index_spec.clone()).unwrap();

            let edge = graph
                .add_edge(1, "shivam", "kapoor", [("p1", 100u64)], None)
                .unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = EdgeFilter.property("p1").temporal().last().eq(100u64);
            assert_eq!(search_edges(&graph, filter.clone()), vec!["shivam->kapoor"]);

            edge.add_metadata([("z", true)], None).unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = EdgeFilter.metadata("z").eq(true);
            assert_eq!(search_edges(&graph, filter.clone()), vec!["shivam->kapoor"]);

            edge.update_metadata([("z", false)], None).unwrap();
            assert_eq!(index_spec, graph.get_index_spec().unwrap());
            let filter = EdgeFilter.metadata("z").eq(false);
            assert_eq!(search_edges(&graph, filter.clone()), vec!["shivam->kapoor"]);
        }
    }
}
