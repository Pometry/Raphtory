use crate::{
    db::api::view::StaticGraphViewOps,
    vectors::{
        document_template::DocumentTemplate,
        entity_id::EntityId,
        similarity_search_utils::{find_top_k, score_documents},
        vectorised_graph::VectorisedGraph,
        Document, Embedding,
    },
};
use itertools::Itertools;
use std::collections::HashMap;

pub struct VectorisedCluster<'a, G: StaticGraphViewOps, T: DocumentTemplate<G>> {
    graphs: &'a HashMap<String, VectorisedGraph<G, T>>,
}

impl<'a, G: StaticGraphViewOps, T: DocumentTemplate<G>> VectorisedCluster<'a, G, T> {
    pub fn new(graphs: &'a HashMap<String, VectorisedGraph<G, T>>) -> Self {
        Self { graphs }
    }

    pub fn search_graph_documents(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Vec<Document> {
        self.search_graph_documents_with_scores(query, limit, window)
            .into_iter()
            .map(|(document, score)| document)
            .collect_vec()
    }

    pub fn search_graph_documents_with_scores(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Vec<(Document, f32)> {
        // FIXME: still need to use the window!!!!
        let documents = self
            .graphs
            .iter()
            .flat_map(|(name, graph)| graph.graph_documents.iter().cloned())
            .collect_vec();
        let scored_documents = score_documents(query, documents);
        let top_k = find_top_k(scored_documents, limit);

        top_k
            .map(|(doc, score)| match doc.entity_id {
                EntityId::Graph { ref name } => {
                    let graph = self.graphs.get(name).unwrap();
                    (
                        doc.regenerate(&graph.source_graph, graph.template.as_ref()),
                        score,
                    )
                }
                _ => panic!("got document that is not related to any graph"),
            })
            .collect_vec()
    }
}
