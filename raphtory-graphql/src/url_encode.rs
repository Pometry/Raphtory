use base64::{prelude::BASE64_URL_SAFE, DecodeError, Engine};
use raphtory::{
    db::api::view::MaterializedGraph,
    errors::GraphError,
    prelude::{StableDecode, StableEncode},
};
use std::path::Path;
#[derive(thiserror::Error, Debug)]
pub enum UrlDecodeError {
    #[error("Bincode operation failed")]
    GraphError {
        #[from]
        source: GraphError,
    },
    #[error("Base64 decoding failed")]
    DecodeError {
        #[from]
        source: DecodeError,
    },
}

pub fn url_encode_graph<G: Into<MaterializedGraph>>(graph: G) -> Result<String, GraphError> {
    let g: MaterializedGraph = graph.into();
    let bytes = g.encode_to_bytes();

    Ok(BASE64_URL_SAFE.encode(bytes))
}

pub fn url_decode_graph<T: AsRef<[u8]>>(graph: T, storage_path: Option<&Path>) -> Result<MaterializedGraph, GraphError> {
    let bytes = BASE64_URL_SAFE.decode(graph.as_ref()).unwrap();

    MaterializedGraph::decode_from_bytes(&bytes, storage_path)
}

#[cfg(test)]
mod tests {
    use raphtory::{db::graph::graph::assert_graph_equal, prelude::*};

    use super::*;

    #[test]
    fn test_url_encode_decode() {
        let graph = Graph::new();
        graph.add_edge(1, 2, 3, [("bla", "blu")], None).unwrap();
        let edge = graph.add_edge(2, 3, 4, [("foo", 42)], Some("7")).unwrap();

        edge.add_metadata([("14", 15f64)], Some("7")).unwrap();

        let node = graph.add_node(17, 0, NO_PROPS, None).unwrap();
        node.add_metadata([("blerg", "test")]).unwrap();

        let bytes = url_encode_graph(graph.clone()).unwrap();
        let tempdir = tempfile::tempdir().unwrap();
        let storage_path = tempdir.path().to_path_buf();
        let decoded_graph = url_decode_graph(bytes, Some(&storage_path)).unwrap();

        let g2 = decoded_graph.into_events().unwrap();

        assert_graph_equal(&graph, &g2);
    }
}
