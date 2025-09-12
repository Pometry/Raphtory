use super::{
    cache::VectorCache,
    entity_db::{EdgeDb, EntityDb, NodeDb},
    template::DocumentTemplate,
    vectorised_graph::VectorisedGraph,
};
use crate::{
    db::api::view::StaticGraphViewOps,
    errors::{GraphError, GraphResult},
    vectors::{
        vector_collection::{lancedb::LanceDb, VectorCollectionFactory},
        Embedding,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

#[derive(Serialize, Deserialize, Debug)]
pub(super) struct VectorMeta {
    pub(super) template: DocumentTemplate,
    pub(super) sample: Embedding,
}

impl VectorMeta {
    pub(super) fn write_to_path(&self, path: &Path) -> Result<(), GraphError> {
        let file = File::create(meta_path(path))?;
        serde_json::to_writer(file, self)?;
        Ok(())
    }
}

impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    pub async fn read_from_path(path: &Path, graph: G, cache: VectorCache) -> GraphResult<Self> {
        let meta_string = std::fs::read_to_string(meta_path(path))?;
        let meta: VectorMeta = serde_json::from_str(&meta_string)?;

        let factory = LanceDb;
        let db_path = db_path(path);
        let dim = meta.sample.len();
        // TODO: put table names in common place? maybe some trait function for EntityDb that returns it
        let node_db = NodeDb(factory.from_path(&db_path, "nodes", dim).await?);
        let edge_db = EdgeDb(factory.from_path(&db_path, "edges", dim).await?);

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

pub(super) fn db_path(path: &Path) -> PathBuf {
    path.join("db")
}
