use raphtory::{db::graph::assertions::assert_graph_equal, prelude::*, serialise::StableDecode};
use raphtory_api::core::storage::graph_folder::{GraphFolder, GraphPaths};

#[test]
fn test_zip_from_folder() {
    let graph = Graph::new();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(1, 1, NO_PROPS, None, None).unwrap();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

    // Create a regular folder and encode the graph
    let temp_folder = tempfile::TempDir::new().unwrap();
    let initial_folder = GraphFolder::from(temp_folder.path().join("initial"));
    graph.encode(&initial_folder).unwrap();

    assert!(initial_folder.graph_path().unwrap().exists());
    assert!(initial_folder.graph_meta_path().unwrap().exists());

    // Create a zip file from the folder
    let output_zip_path = temp_folder.path().join("output.zip");
    let output_zip_file = std::fs::File::create(&output_zip_path).unwrap();
    initial_folder.zip_from_folder(output_zip_file).unwrap();

    assert!(output_zip_path.exists());

    // Verify the output zip contains the same graph
    let zip_folder = GraphFolder::new_as_zip(&output_zip_path);
    let decoded_graph = Graph::decode(&zip_folder).unwrap();

    assert_graph_equal(&graph, &decoded_graph);
}

#[test]
fn test_zip_from_zip() {
    let graph = Graph::new();
    graph.add_node(0, 0, NO_PROPS, None, None).unwrap();
    graph.add_node(1, 1, NO_PROPS, None, None).unwrap();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();

    // Create an initial zip file
    let temp_folder = tempfile::TempDir::new().unwrap();
    let initial_zip_path = temp_folder.path().join("initial.zip");
    let initial_folder = GraphFolder::new_as_zip(&initial_zip_path);
    graph.encode(&initial_folder).unwrap();

    assert!(initial_zip_path.exists());

    // Create a new zip file from the existing zip
    let output_zip_path = temp_folder.path().join("output.zip");
    let output_zip_file = std::fs::File::create(&output_zip_path).unwrap();
    initial_folder.zip_from_folder(output_zip_file).unwrap();

    assert!(output_zip_path.exists());

    // Verify zip file sizes
    let initial_size = std::fs::metadata(&initial_zip_path).unwrap().len();
    let output_size = std::fs::metadata(&output_zip_path).unwrap().len();
    assert_eq!(initial_size, output_size);

    // Verify the output zip contains the same graph
    let zip_folder = GraphFolder::new_as_zip(&output_zip_path);
    let decoded_graph = Graph::decode(&zip_folder).unwrap();

    assert_graph_equal(&graph, &decoded_graph);
}

#[test]
fn test_unzip_to_folder() {
    let graph = Graph::new();

    graph
        .add_edge(0, 0, 1, [("test prop 1", Prop::map(NO_PROPS))], None)
        .unwrap();
    graph
        .add_edge(
            1,
            2,
            3,
            [("test prop 1", Prop::map([("key", "value")]))],
            Some("layer_a"),
        )
        .unwrap();
    graph
        .add_edge(2, 3, 4, [("test prop 2", "value")], Some("layer_b"))
        .unwrap();
    graph
        .add_edge(3, 1, 4, [("test prop 3", 10.0)], None)
        .unwrap();
    graph
        .add_edge(4, 1, 3, [("test prop 4", true)], None)
        .unwrap();

    graph
        .node(1)
        .unwrap()
        .add_updates(5, [("test node prop", 5i32)], None)
        .unwrap();

    let temp_folder = tempfile::TempDir::new().unwrap();
    let folder = temp_folder.path().join("graph");
    let graph_folder = GraphFolder::from(&folder);

    graph.encode(&graph_folder).unwrap();
    assert!(graph_folder.graph_path().unwrap().exists());

    // Zip the folder
    let mut zip_bytes = Vec::new();
    let cursor = std::io::Cursor::new(&mut zip_bytes);
    graph_folder.zip_from_folder(cursor).unwrap();

    // Unzip to a new folder
    let folder = temp_folder.path().join("unzip");
    let unzip_folder = GraphFolder::from(&folder);
    let cursor = std::io::Cursor::new(&zip_bytes);
    unzip_folder.unzip_to_folder(cursor).unwrap();

    // Verify the extracted folder has the same structure
    assert!(unzip_folder.graph_path().unwrap().exists());
    assert!(unzip_folder.graph_meta_path().unwrap().exists());

    // Verify the extracted graph is the same as the original
    let extracted_graph = Graph::decode(&unzip_folder).unwrap();
    assert_graph_equal(&graph, &extracted_graph);
}
