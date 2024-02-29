use super::{document_ref::DocumentRef, entity_id::EntityId, Embedding};
use faiss::{index::IndexImpl, index_factory, Idx, Index, MetricType};
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
    let mut index = index_factory(dim, "IVF4096_HNSW32,Flat", MetricType::InnerProduct).unwrap();

    let flattened = entities.iter().flat_map(|(entity, docs)| {
        docs.iter()
            .enumerate()
            .map(|(subindex, doc)| (entity.clone(), subindex, doc))
    });
    let mapping = flattened
        .clone()
        .map(|(entity, subindex, _)| DocumentPointer { entity, subindex })
        .collect_vec();
    let data_vec = flattened
        .clone()
        .flat_map(|(_, _, doc)| doc.embedding.clone())
        .collect_vec();
    let data = data_vec.as_slice();
    let ids: Vec<Idx> = flattened
        .enumerate()
        .map(|(id, _)| (id as i64).into())
        .collect_vec();

    index.train(data).unwrap();
    index.add_with_ids(data, ids.as_slice()).unwrap();

    FaissIndex { index, mapping }
}
