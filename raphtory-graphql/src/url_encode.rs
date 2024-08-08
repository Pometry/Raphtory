use base64::{prelude::BASE64_URL_SAFE, DecodeError, Engine};
use raphtory::{core::utils::errors::GraphError, db::api::view::MaterializedGraph};

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
    Ok(BASE64_URL_SAFE.encode(g.bincode()?))
}

pub fn url_decode_graph<T: AsRef<[u8]>>(graph: T) -> Result<MaterializedGraph, UrlDecodeError> {
    Ok(MaterializedGraph::from_bincode(
        &BASE64_URL_SAFE.decode(graph)?,
    )?)
}
