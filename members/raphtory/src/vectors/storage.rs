use super::{
    cache::VectorCache,
    db::{EdgeDb, EntityDb, NodeDb},
    template::DocumentTemplate,
    vectorised_graph::VectorisedGraph,
};
use crate::{
    db::api::view::StaticGraphViewOps,
    errors::{GraphError, GraphResult},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
pub(super) struct VectorMeta {
    pub(super) template: DocumentTemplate,
}

impl VectorMeta {
    pub(super) fn write_to_path(&self, path: &Path) -> Result<(), GraphError> {
        let file = File::create(meta_path(path))?;
        serde_json::to_writer(file, self)?;
        Ok(())
    }
}

impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    pub fn read_from_path(path: &Path, graph: G, cache: VectorCache) -> GraphResult<Self> {
        let meta_string = std::fs::read_to_string(meta_path(path))?;
        let meta: VectorMeta = serde_json::from_str(&meta_string)?;

        let node_db = NodeDb::from_path(&node_vectors_path(path))?;
        let edge_db = EdgeDb::from_path(&edge_vectors_path(path))?;

        Ok(VectorisedGraph {
            template: meta.template,
            source_graph: graph,
            cache,
            node_db,
            edge_db,
        })
    }
}

fn meta_path(path: &Path) -> PathBuf {
    path.join("meta")
}

pub(super) fn node_vectors_path(path: &Path) -> PathBuf {
    path.join("nodes")
}

pub(super) fn edge_vectors_path(path: &Path) -> PathBuf {
    path.join("edges")
}
