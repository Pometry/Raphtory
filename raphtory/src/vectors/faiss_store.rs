use super::{document_ref::DocumentRef, entity_id::EntityId, Embedding};
use faiss::{index::IndexImpl, index_factory, Index, MetricType};
use itertools::Itertools;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(crate) struct DocumentPointer {
    pub(crate) entity: EntityId,
    pub(crate) subindex: usize, // TODO: reduce this inside and provide nice error when there are too much documents for some entity
}

pub(crate) struct FaissIndex {
    mapping: Vec<DocumentPointer>,
    index: IndexImpl,
}

impl FaissIndex {
    fn get(&self, idx: u64) -> DocumentPointer {
        self.mapping.get(idx as usize).unwrap().clone()
    }

    /// This function returns a vector just to take ownership of Faiss results
    pub(crate) fn search(&mut self, query: &Embedding, limit: usize) -> Vec<DocumentPointer> {
        // TODO: assert that the length of the query is correct
        let result = self.index.search(query.as_slice(), limit);
        match result {
            Ok(result) => {
                dbg!(&result);
                let valid_labels = result.labels.iter().filter_map(|idx| idx.get());
                valid_labels.map(|idx| self.get(idx)).collect_vec()
            }
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
        // TODO: review, this doesnt froup if there are empty groups!
        let maybe_node_group = nodes.iter().next();
        let maybe_edge_group = edges.iter().next();
        let maybe_group = maybe_node_group.or(maybe_edge_group);
        let maybe_vector = maybe_group.and_then(|(_, docs)| docs.get(0));
        let dim = maybe_vector.map(|vec| vec.embedding.len()).unwrap_or(1) as u32;
        dbg!(&dim);
        Self {
            nodes: build_entity_index(nodes, dim),
            edges: build_entity_index(edges, dim),
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

fn build_entity_index(entities: &HashMap<EntityId, Vec<DocumentRef>>, dim: u32) -> FaissIndex {
    dbg!(&dim);
    let mut index = index_factory(dim, "IDMap,Flat", MetricType::InnerProduct).unwrap();
    let mut mapping = vec![];
    for (entity, doc_refs) in entities {
        for (subindex, doc_ref) in doc_refs.iter().enumerate() {
            let entity = entity.clone();
            let index_for_faiss = mapping.len() as i64;
            mapping.push(DocumentPointer { entity, subindex });
            let embedding = doc_ref.embedding.as_slice();
            // this can be improved by putting embeddings in a contiguous memory slice
            index
                .add_with_ids(embedding, &[index_for_faiss.into()])
                .unwrap();
        }
    }
    FaissIndex { index, mapping }
}
