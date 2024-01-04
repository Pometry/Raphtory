use crate::vectors::{document_ref::DocumentRef, entity_id::EntityId};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

#[derive(Deserialize, Serialize)]
pub struct EmbeddingStore {
    pub(crate) graph_document: Vec<DocumentRef>,
    pub(crate) node_documents: HashMap<EntityId, Vec<DocumentRef>>,
    pub(crate) edge_documents: HashMap<EntityId, Vec<DocumentRef>>,
}

impl EmbeddingStore {
    pub(crate) fn load_from_path(path: &Path) -> Option<Self> {
        let file = File::open(&path).ok()?;
        let mut reader = BufReader::new(file);
        bincode::deserialize_from(&mut reader).ok()
    }

    pub(crate) fn save_to_path(&self, path: &Path) {
        let file = File::open(&path)
            .ok()
            .expect("Couldn't create file to store embedding store");
        let mut writer = BufWriter::new(file);
        bincode::serialize_into(&mut writer, self).expect("Couldn't serialize embedding store");
    }
}
