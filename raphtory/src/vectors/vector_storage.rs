use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
    sync::Arc,
};

use crate::{core::utils::errors::GraphError, db::api::view::StaticGraphViewOps};

use super::{
    document_ref::DocumentRef, embedding_cache::EmbeddingCache, entity_id::EntityId,
    template::DocumentTemplate, vectorised_graph::VectorisedGraph, EmbeddingFunction,
};

#[derive(Serialize, Deserialize)]
struct VectorStorage {
    template: DocumentTemplate,
    graph_documents: Vec<DocumentRef>,
    node_documents: HashMap<EntityId, Vec<DocumentRef>>,
    edge_documents: HashMap<EntityId, Vec<DocumentRef>>,
}

impl<G: StaticGraphViewOps> VectorisedGraph<G> {
    // pub fn read_from_path(
    //     path: &Path,
    //     graph: G,
    //     embedding: Arc<dyn EmbeddingFunction>,
    //     cache_storage: Arc<Option<EmbeddingCache>>,
    // ) -> Option<Self> {
    //     // TODO: return Result instead of Option
    //     let file = File::open(&path).ok()?;
    //     let mut reader = BufReader::new(file);
    //     let VectorStorage {
    //         template,
    //         graph_documents,
    //         node_documents,
    //         edge_documents,
    //     } = bincode::deserialize_from(&mut reader).ok()?;

    //     Some(VectorisedGraph::new(
    //         graph,
    //         template,
    //         embedding,
    //         cache_storage,
    //         Arc::new(graph_documents.into()),
    //         Arc::new(node_documents.into()),
    //         Arc::new(edge_documents.into()),
    //     ))
    // }

    // pub fn write_to_path(&self, path: &Path) -> Result<(), GraphError> {
    //     let storage = VectorStorage {
    //         template: self.template.clone(),
    //         graph_documents: self.graph_documents.read().clone(),
    //         node_documents: self.node_documents.read().clone(),
    //         edge_documents: self.edge_documents.read().clone(),
    //     };
    //     let file = File::create(path)?;
    //     let mut writer = BufWriter::new(file);
    //     bincode::serialize_into(&mut writer, &storage)?;
    //     Ok(())
    // }
}
