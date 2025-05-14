use arroy::{distances::Cosine, Database as ArroyDatabase, Reader};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use crate::{core::utils::errors::GraphError, db::api::view::StaticGraphViewOps};

use super::{
    embedding_cache::EmbeddingCache,
    template::DocumentTemplate,
    vectorisable::open_env,
    vectorised_graph::{EdgeDb, NodeDb, VectorDb, VectorisedGraph},
    EmbeddingFunction,
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
    pub fn read_from_path(
        path: &Path,
        graph: G,
        embedding: Arc<dyn EmbeddingFunction>,
        cache_storage: Arc<Option<EmbeddingCache>>,
    ) -> Option<Self> {
        // TODO: return Result instead of Option
        let meta_string = std::fs::read_to_string(meta_path(path)).ok()?;
        let meta: VectorMeta = serde_json::from_str(&meta_string).ok()?;

        let node_db = open_database(&node_vectors_path(path));
        let edge_db = open_database(&edge_vectors_path(path));

        Some(VectorisedGraph {
            template: meta.template,
            source_graph: graph,
            embedding,
            cache_storage,
            node_db: NodeDb(node_db),
            edge_db: EdgeDb(edge_db),
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

fn open_database(path: &Path) -> VectorDb {
    // TODO: fix unwraps!
    let env = open_env(path);
    let rtxn = env.read_txn().unwrap();
    // let db: ArroyDatabase<Cosine> = env.database_options().types().open(&rtxn).unwrap().unwrap(); // alternative, this comes from https://github.com/meilisearch/arroy/blob/main/examples/graph.rs
    let db: ArroyDatabase<Cosine> = env.open_database(&rtxn, None).unwrap().unwrap(); // this is the old implementation, causing an issue I think
    let first_vector = Reader::open(&rtxn, 0, db)
        .ok()
        .and_then(|reader| reader.iter(&rtxn).ok()?.next()?.ok());
    let dimensions = if let Some((_, vector)) = first_vector {
        // FIXME: maybe there should not be any db at all if this is the case?
        vector.len().into()
    } else {
        OnceLock::new()
    };
    rtxn.commit().unwrap();
    VectorDb {
        vectors: db,
        env,
        _tempdir: None,
        dimensions,
    }
}
