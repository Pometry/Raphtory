use std::io::{copy, Read, Write};

use base64::{prelude::BASE64_URL_SAFE, DecodeError, Engine};
use raphtory::{
    db::api::view::MaterializedGraph,
    errors::GraphError,
    prelude::{ParquetDecoder, ParquetEncoder},
    serialise::StableEncode,
};
use zip::write::SimpleFileOptions;

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
    let temp_dir = tempfile::tempdir()?;
    g.encode_parquet(temp_dir.path())?;
    // now zip the entire directory, don't bother with compression
    let mut bytes = Vec::new();
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut bytes));

    let mut paths = vec![temp_dir.path().to_path_buf()];
    while let Some(path) = paths.pop() {
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // If it's a directory, we should add it to the list to process later
                paths.push(path);
            } else {
                // If it's a file, we should write it directly
                let options =
                    SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
                let rel_path = path.strip_prefix(temp_dir.path()).unwrap();
                zip.start_file_from_path(rel_path, options)?;
                let mut file = std::fs::File::open(path)?;
                copy(&mut file, &mut zip)?;
            }
        }
    }
    zip.finish()?;
    Ok(BASE64_URL_SAFE.encode(bytes))
}

pub fn url_decode_graph<T: AsRef<[u8]>>(graph: T) -> Result<MaterializedGraph, GraphError> {
    let zip_bytes = BASE64_URL_SAFE.decode(graph.as_ref()).unwrap();

    let temp_dir = tempfile::tempdir()?;
    let mut zip = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes))?;

    for i in 0..zip.len() {
        let mut file = zip.by_index(i)?;
        let out_path = temp_dir.path().join(file.enclosed_name().unwrap());
        dbg!(&out_path);

        if let Some(path) = out_path.parent() {
            std::fs::create_dir_all(path)?;
        }
        let mut out_file = std::fs::File::create(&out_path)?;
        std::io::copy(&mut file, &mut out_file)?;
    }

    MaterializedGraph::decode_parquet(temp_dir.path())
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
        let decoded_graph = url_decode_graph(bytes).unwrap();

        let g2 = decoded_graph.into_events().unwrap();

        assert_graph_equal(&graph, &g2);
    }
}
