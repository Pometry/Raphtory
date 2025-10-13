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
mod proto_test {
    use crate::{
        prelude::*,
        serialise::{proto::GraphType, ProtoGraph},
    };

    use super::*;

    #[test]
    fn manually_test_append() {
        let mut graph1 = proto::Graph::default();
        graph1.set_graph_type(GraphType::Event);
        graph1.new_node(GidRef::Str("1"), VID(0), 0);
        graph1.new_node(GidRef::Str("2"), VID(1), 0);
        graph1.new_edge(VID(0), VID(1), EID(0));
        graph1.update_edge_tprops(
            EID(0),
            TimeIndexEntry::start(1),
            0,
            iter::empty::<(usize, Prop)>(),
        );
        let mut bytes1 = graph1.encode_to_vec();

        let mut graph2 = proto::Graph::default();
        graph2.new_node(GidRef::Str("3"), VID(2), 0);
        graph2.new_edge(VID(0), VID(2), EID(1));
        graph2.update_edge_tprops(
            EID(1),
            TimeIndexEntry::start(2),
            0,
            iter::empty::<(usize, Prop)>(),
        );
        bytes1.extend(graph2.encode_to_vec());

        let graph = Graph::decode_from_bytes(&bytes1).unwrap();
        assert_eq!(graph.nodes().name().collect_vec(), ["1", "2", "3"]);
        assert_eq!(
            graph.edges().id().collect_vec(),
            [
                (GID::Str("1".to_string()), GID::Str("2".to_string())),
                (GID::Str("1".to_string()), GID::Str("3".to_string()))
            ]
        )
    }
    // we rely on this to make sure writing no updates does not actually write anything to file
    #[test]
    fn empty_proto_is_empty_bytes() {
        let proto = ProtoGraph::default();
        let bytes = proto.encode_to_vec();
        assert!(bytes.is_empty())
    }
}
