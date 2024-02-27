use super::{document_ref::DocumentRef, entity_id::EntityId, Embedding};
use faiss::{index::IndexImpl, index_factory, Idx, Index, MetricType};
use itertools::Itertools;
use std::collections::HashMap;

#[derive(Clone)]
pub(crate) struct DocumentPointer {
    pub(crate) entity: EntityId,
    pub(crate) subindex: usize, // TODO: reduce this inside and provide nice error when there are too much documents for some entity
}

pub(crate) struct FaissIndex {
    mapping: Vec<DocumentPointer>,
    index: IndexImpl,
}

impl FaissIndex {
    fn get(&self, idx: &Idx) -> DocumentPointer {
        self.mapping
            .get(idx.get().unwrap() as usize)
            .unwrap()
            .clone()
    }

    /// This function returns a vector just to take ownership of Faiss results
    pub(crate) fn search(&mut self, query: &Embedding, limit: usize) -> Vec<DocumentPointer> {
        let result = self.index.search(query.as_slice(), limit);
        match result {
            Ok(result) => result.labels.iter().map(|idx| self.get(idx)).collect_vec(),
            Err(_) => vec![],
        }
    }
}

pub(crate) struct FaissStore {
    pub(crate) nodes: FaissIndex,
    pub(crate) edges: FaissIndex,
}

impl FaissStore {
    pub(crate) fn from_refs(
        nodes: &HashMap<EntityId, Vec<DocumentRef>>,
        edges: &HashMap<EntityId, Vec<DocumentRef>>,
    ) -> Self {
        Self {
            nodes: build_entity_index(nodes),
            edges: build_entity_index(edges),
        }
    }

    // pub(crate) fn search_nodes(
    //     &self,
    //     query: Embedding,
    //     limit: usize,
    // ) -> Vec<(DocumentPointer, f32)> {
    //     search(&self.nodes, query, limit)
    // }

    // pub(crate) fn search_edges(
    //     &self,
    //     query: Embedding,
    //     limit: usize,
    // ) -> Vec<(DocumentPointer, f32)> {
    //     search(&self.edges, query, limit)
    // }
}

fn build_entity_index(entities: &HashMap<EntityId, Vec<DocumentRef>>) -> FaissIndex {
    let mut index = index_factory(3, "IDMap,Flat", MetricType::InnerProduct).unwrap(); // FIXME: this can't be 3, needs to be variable!!!!!!!
    let mut mapping = vec![];
    for (entity, doc_refs) in entities {
        for (subindex, doc_ref) in doc_refs.iter().enumerate() {
            let entity = entity.clone();
            mapping.push(DocumentPointer { entity, subindex });
            let embedding = doc_ref.embedding.as_slice();
            let index_for_faiss = mapping.len() as i64;
            // this can be improved by putting embeddings in a contiguous memory slice
            index
                .add_with_ids(embedding, &[index_for_faiss.into()])
                .unwrap();
        }
    }
    FaissIndex { index, mapping }
}
