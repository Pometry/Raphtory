use std::path::Path;

use super::graph_folder::GraphFolder;
#[cfg(feature = "search")]
use crate::prelude::IndexMutationOps;
use crate::{
    db::api::{mutation::AdditionOps, view::StaticGraphViewOps},
    errors::GraphError,
    serialise::parquet::{ParquetDecoder, ParquetEncoder},
};
use std::{fs, fs::File};
use tempfile;

pub trait StableEncode: StaticGraphViewOps + AdditionOps {
    /// Encode the graph into bytes.
    fn encode_to_bytes(&self) -> Vec<u8>;

    /// Encode the graph into the given path.
    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError>;
}

impl<T: ParquetEncoder + StaticGraphViewOps + AdditionOps> StableEncode for T {
    fn encode_to_bytes(&self) -> Vec<u8> {
        // Encode to a temp zip file and return the bytes
        let tempdir = tempfile::tempdir().unwrap();
        let zip_path = tempdir.path().join("graph.zip");
        let folder = GraphFolder::new_as_zip(&zip_path);

        self.encode(&folder).unwrap();
        fs::read(&zip_path).unwrap()
    }

    fn encode(&self, path: impl Into<GraphFolder>) -> Result<(), GraphError> {
        let folder: GraphFolder = path.into();

        if folder.write_as_zip_format {
            let file = File::create(&folder.get_base_path())?;
            self.encode_parquet_to_zip(file)?;

            #[cfg(feature = "search")]
            self.persist_index_to_disk_zip(&folder)?;
        } else {
            folder.reserve()?;
            self.encode_parquet(&folder.get_graph_path())?;

            #[cfg(feature = "search")]
            self.persist_index_to_disk(&folder)?;
        }

        folder.write_metadata(self)?;

        Ok(())
    }
}

pub trait StableDecode: StaticGraphViewOps + AdditionOps {
    // Decode the graph from the given bytes array.
    // `path_for_decoded_graph` gets passed to the newly created graph.
    fn decode_from_bytes(
        bytes: &[u8],
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError>;

    // Decode the graph from the given path.
    // `path_for_decoded_graph` gets passed to the newly created graph.
    fn decode(
        path: impl Into<GraphFolder>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError>;

    /// Returns true if the graph can be decoded from the given path.
    fn is_decodable(path: impl AsRef<Path>) -> bool;
}

impl<T: ParquetDecoder + StaticGraphViewOps + AdditionOps> StableDecode for T {
    fn decode_from_bytes(
        bytes: &[u8],
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        // Write bytes to a temp zip file and decode
        let tempdir = tempfile::tempdir()?;
        let zip_path = tempdir.path().join("graph.zip");
        let folder = GraphFolder::new_as_zip(&zip_path);
        std::fs::write(&zip_path, bytes)?;

        let graph = Self::decode(&folder, path_for_decoded_graph)?;

        Ok(graph)
    }

    fn decode(
        path: impl Into<GraphFolder>,
        path_for_decoded_graph: Option<&Path>,
    ) -> Result<Self, GraphError> {
        let graph;
        let folder: GraphFolder = path.into();

        if folder.is_zip() {
            let reader = std::fs::File::open(&folder.get_base_path())?;
            graph = Self::decode_parquet_from_zip(reader, path_for_decoded_graph)?;
        } else {
            graph = Self::decode_parquet(&folder.get_graph_path(), path_for_decoded_graph)?;
        }

        #[cfg(feature = "search")]
        graph.load_index(&folder)?;

        Ok(graph)
    }

    fn is_decodable(path: impl AsRef<Path>) -> bool {
        Self::is_parquet_decodable(path)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::types::{Int32Type, UInt8Type};
    use tempfile::TempDir;

    use crate::{
        db::{
            api::{
                mutation::DeletionOps, properties::internal::InternalMetadataOps,
                view::MaterializedGraph,
            },
            graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
        },
        prelude::*,
        serialise::{
            graph_folder::GraphFolder, metadata::assert_metadata_correct, StableDecode,
            StableEncode,
        },
        test_utils::{build_edge_list, build_graph_from_edge_list},
    };
    use chrono::{DateTime, NaiveDateTime};
    use proptest::proptest;
    use raphtory_api::core::storage::arc_str::ArcStr;

    #[test]
    fn node_no_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn node_with_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", [("age", Prop::U32(47))], None)
            .unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[cfg(feature = "search")] // TODO: Why is this gated?
    #[test]
    fn test_node_name() {
        let g = Graph::new();
        g.add_edge(1, "ben", "hamza", NO_PROPS, None).unwrap();
        g.add_edge(2, "haaroon", "hamza", NO_PROPS, None).unwrap();
        g.add_edge(3, "ben", "haaroon", NO_PROPS, None).unwrap();
        let temp_file = TempDir::new().unwrap();
        g.encode(&temp_file).unwrap();
        let g2 = MaterializedGraph::decode(&temp_file, None).unwrap();
        assert_eq!(g2.nodes().name().collect_vec(), ["ben", "hamza", "haaroon"]);
        let node_names: Vec<_> = g2.nodes().iter().map(|n| n.name()).collect();
        assert_eq!(node_names, ["ben", "hamza", "haaroon"]);
        let g2_m = g2.materialize().unwrap();
        assert_eq!(
            g2_m.nodes().name().collect_vec(),
            ["ben", "hamza", "haaroon"]
        );
        let g3 = g.materialize().unwrap();
        assert_eq!(g3.nodes().name().collect_vec(), ["ben", "hamza", "haaroon"]);
        let node_names: Vec<_> = g3.nodes().iter().map(|n| n.name()).collect();
        assert_eq!(node_names, ["ben", "hamza", "haaroon"]);

        let temp_file = TempDir::new().unwrap();
        g3.encode(&temp_file).unwrap();
        let g4 = MaterializedGraph::decode(&temp_file, None).unwrap();
        assert_eq!(g4.nodes().name().collect_vec(), ["ben", "hamza", "haaroon"]);
        let node_names: Vec<_> = g4.nodes().iter().map(|n| n.name()).collect();
        assert_eq!(node_names, ["ben", "hamza", "haaroon"]);
    }

    #[test]
    fn node_with_metadata() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        let n1 = g1
            .add_node(2, "Bob", [("age", Prop::U32(47))], None)
            .unwrap();

        n1.update_metadata([("name", Prop::Str("Bob".into()))])
            .expect("Failed to update metadata");

        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_no_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", NO_PROPS, None).unwrap();
        g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_no_props_delete() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new().persistent_graph();
        g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        g1.delete_edge(19, "Alice", "Bob", None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = PersistentGraph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);

        let edge = g2.edge("Alice", "Bob").expect("Failed to get edge");
        let deletions = edge.deletions().to_vec();
        assert_eq!(deletions, vec![19]);
    }

    #[test]
    fn edge_t_props() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.add_node(2, "Bob", NO_PROPS, None).unwrap();
        g1.add_edge(3, "Alice", "Bob", [("kind", "friends")], None)
            .unwrap();

        g1.add_edge(
            3,
            "Alice",
            "Bob",
            [("image", Prop::from_arr::<Int32Type>(vec![3i32, 5]))],
            None,
        )
        .unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_metadata() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        let e1 = g1.add_edge(3, "Alice", "Bob", NO_PROPS, None).unwrap();
        e1.update_metadata([("friends", true)], None)
            .expect("Failed to update metadata");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn edge_layers() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g1.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);
    }

    #[test]
    fn test_all_the_t_props_on_node() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_node(1, "Alice", props.clone(), None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);

        let node = g2.node("Alice").expect("Failed to get node");

        assert!(props.into_iter().all(|(name, expected)| {
            node.properties()
                .temporal()
                .get(name)
                .filter(|prop_view| {
                    let (t, prop) = prop_view.iter().next().expect("Failed to get prop");
                    prop == expected && t == 1
                })
                .is_some()
        }))
    }

    #[test]
    fn test_all_the_t_props_on_edge() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        g1.add_edge(1, "Alice", "Bob", props.clone(), None).unwrap();
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);

        let edge = g2.edge("Alice", "Bob").expect("Failed to get edge");

        assert!(props.into_iter().all(|(name, expected)| {
            edge.properties()
                .temporal()
                .get(name)
                .filter(|prop_view| {
                    let (t, prop) = prop_view.iter().next().expect("Failed to get prop");
                    prop == expected && t == 1
                })
                .is_some()
        }))
    }

    #[test]
    fn test_all_the_metadata_on_edge() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        let e = g1.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_metadata(props.clone(), Some("a"))
            .expect("Failed to update metadata");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);

        let edge = g2
            .edge("Alice", "Bob")
            .expect("Failed to get edge")
            .layers("a")
            .unwrap();

        for (new, old) in edge.metadata().iter_filtered().zip(props.iter()) {
            assert_eq!(new.0, old.0);
            assert_eq!(new.1, old.1);
        }
    }

    #[test]
    fn test_all_the_metadata_on_node() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        let g1 = Graph::new();
        let n = g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_metadata(props.clone())
            .expect("Failed to update metadata");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);

        let node = g2.node("Alice").expect("Failed to get node");

        assert!(props.into_iter().all(|(name, expected)| {
            node.metadata()
                .get(name)
                .filter(|prop| prop == &expected)
                .is_some()
        }))
    }

    #[test]
    fn graph_metadata() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let g1 = Graph::new();
        g1.add_metadata(props.clone())
            .expect("Failed to add metadata");

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);

        props.into_iter().for_each(|(name, prop)| {
            let id = g2.get_metadata_id(name).expect("Failed to get prop id");
            assert_eq!(prop, g2.get_metadata(id).expect("Failed to get prop"));
        });
    }

    #[test]
    fn graph_temp_properties() {
        let mut props = vec![];
        write_props_to_vec(&mut props);

        let g1 = Graph::new();
        for t in 0..props.len() {
            g1.add_properties(t as i64, props[t..t + 1].to_vec())
                .expect("Failed to add metadata");
        }

        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");
        g1.encode(&temp_file).unwrap();
        let g2 = Graph::decode(&temp_file, None).unwrap();
        assert_graph_equal(&g1, &g2);

        props
            .into_iter()
            .enumerate()
            .for_each(|(expected_t, (name, expected))| {
                for (t, prop) in g2
                    .properties()
                    .temporal()
                    .get(name)
                    .expect("Failed to get prop view")
                {
                    assert_eq!(prop, expected);
                    assert_eq!(t, expected_t as i64);
                }
            });
    }

    #[test]
    #[ignore = "Disabled until metadata is implemented correctly"]
    fn test_incremental_writing_on_graph() {
        let g = Graph::new();
        let mut props = vec![];
        write_props_to_vec(&mut props);
        let temp_file = tempfile::tempdir().unwrap();
        let folder = GraphFolder::from(&temp_file);

        assert_metadata_correct(&folder, &g);

        for t in 0..props.len() {
            g.add_properties(t as i64, props[t..t + 1].to_vec())
                .expect("Failed to add metadata");
        }

        g.add_metadata(props.clone())
            .expect("Failed to add metadata");

        let n = g.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_metadata(props.clone())
            .expect("Failed to update metadata");

        let e = g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_metadata(props.clone(), Some("a"))
            .expect("Failed to update metadata");

        assert_metadata_correct(&folder, &g);

        g.add_edge(2, "Alice", "Bob", props.clone(), None).unwrap();
        g.add_node(1, "Charlie", props.clone(), None).unwrap();

        g.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();

        let g2 = Graph::decode(&temp_file, None).unwrap();

        assert_graph_equal(&g, &g2);
        assert_metadata_correct(&folder, &g);
    }

    #[test]
    #[ignore = "Disabled until metadata is implemented correctly"]
    fn test_incremental_writing_on_persistent_graph() {
        let g = PersistentGraph::new();
        let mut props = vec![];
        write_props_to_vec(&mut props);
        let temp_file = tempfile::tempdir().unwrap();
        let folder = GraphFolder::from(&temp_file);

        for t in 0..props.len() {
            g.add_properties(t as i64, props[t..t + 1].to_vec())
                .expect("Failed to add metadata");
        }

        g.add_metadata(props.clone())
            .expect("Failed to add metadata");

        let n = g.add_node(1, "Alice", NO_PROPS, None).unwrap();
        n.update_metadata(props.clone())
            .expect("Failed to update metadata");

        let e = g.add_edge(1, "Alice", "Bob", NO_PROPS, Some("a")).unwrap();
        e.update_metadata(props.clone(), Some("a"))
            .expect("Failed to update metadata");

        assert_metadata_correct(&folder, &g);

        g.add_edge(2, "Alice", "Bob", props.clone(), None).unwrap();
        g.add_node(1, "Charlie", props.clone(), None).unwrap();

        g.add_edge(7, "Alice", "Bob", NO_PROPS, Some("one"))
            .unwrap();
        g.add_edge(7, "Bob", "Charlie", [("friends", false)], Some("two"))
            .unwrap();

        let g2 = PersistentGraph::decode(&temp_file, None).unwrap();

        assert_graph_equal(&g, &g2);
        assert_metadata_correct(&folder, &g);
    }

    #[test]
    fn encode_decode_prop_test() {
        proptest!(|(edges in build_edge_list(100, 100))| {
            let g = build_graph_from_edge_list(&edges);
            let bytes = g.encode_to_bytes();
            let tempdir = tempfile::tempdir().unwrap();
            let g2 = Graph::decode_from_bytes(&bytes, None).unwrap();
            assert_graph_equal(&g, &g2);
        })
    }

    #[test]
    fn encode_decode_zip() {
        let g = Graph::new();

        g.add_edge(0, 0, 1, [("test prop 1", Prop::map(NO_PROPS))], None)
            .unwrap();
        g.add_edge(
            1,
            2,
            3,
            [("test prop 1", Prop::map([("key", "value")]))],
            Some("layer_a"),
        )
        .unwrap();
        g.add_edge(2, 3, 4, [("test prop 2", "value")], Some("layer_b"))
            .unwrap();
        g.add_edge(3, 1, 4, [("test prop 3", 10.0)], None).unwrap();
        g.add_edge(4, 1, 3, [("test prop 4", true)], None).unwrap();

        let tempdir = tempfile::tempdir().unwrap();
        let folder = GraphFolder::new_as_zip(&tempdir.path().join("test_graph.zip"));
        g.encode(&folder).unwrap();
        let g2 = Graph::decode(&folder, None).unwrap();

        assert_graph_equal(&g, &g2);
    }

    #[test]
    fn test_is_decodable() {
        let tempdir = TempDir::new().unwrap();
        let temp_file = tempdir.path().join("graph");

        // Test with non-existent path - should return false
        assert!(!Graph::is_parquet_decodable(&temp_file));

        // Test with directory containing parquet files - should return true
        let g1 = Graph::new();
        g1.add_node(1, "Alice", NO_PROPS, None).unwrap();
        g1.encode(&temp_file).unwrap();
        assert!(Graph::is_parquet_decodable(&temp_file));

        // Test with directory containing only IPC files - should return false
        let ipc_tempdir = TempDir::new().unwrap();
        let edges_dir = ipc_tempdir.path().join("edges");
        std::fs::create_dir_all(&edges_dir).unwrap();
        let adj_out_dsts = edges_dir.join("AdjOutDsts.ipc");
        let adj_out_srcs = edges_dir.join("AdjOutSrcs.ipc");
        std::fs::write(&adj_out_dsts, "fake ipc content").unwrap();
        std::fs::write(&adj_out_srcs, "fake ipc content").unwrap();
        assert!(!Graph::is_parquet_decodable(&ipc_tempdir.path()));
    }

    fn write_props_to_vec(props: &mut Vec<(&str, Prop)>) {
        props.push(("name", Prop::Str("Alice".into())));
        props.push(("age", Prop::U32(47)));
        props.push(("score", Prop::I32(27)));
        props.push(("is_adult", Prop::Bool(true)));
        props.push(("height", Prop::F32(1.75)));
        props.push(("weight", Prop::F64(75.5)));
        props.push((
            "children",
            Prop::List(Arc::new(vec![
                Prop::Str("Bob".into()),
                Prop::Str("Charlie".into()),
            ])),
        ));
        props.push((
            "properties",
            Prop::map(props.iter().map(|(k, v)| (ArcStr::from(*k), v.clone()))),
        ));
        let fmt = "%Y-%m-%d %H:%M:%S";
        props.push((
            "time",
            Prop::NDTime(
                NaiveDateTime::parse_from_str("+10000-09-09 01:46:39", fmt)
                    .expect("Failed to parse time"),
            ),
        ));

        props.push((
            "dtime",
            Prop::DTime(
                DateTime::parse_from_rfc3339("2021-09-09T01:46:39Z")
                    .unwrap()
                    .into(),
            ),
        ));

        props.push((
            "array",
            Prop::from_arr::<UInt8Type>(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        ));
    }
}
