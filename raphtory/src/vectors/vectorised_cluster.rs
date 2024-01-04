use crate::{
    db::api::view::StaticGraphViewOps,
    vectors::{
        document_template::DocumentTemplate,
        entity_id::EntityId,
        vectorised_graph::{find_top_k, score_documents, VectorisedGraph},
        Document, Embedding,
    },
};
use itertools::Itertools;
use std::collections::HashMap;

pub struct VectorisedCluster<'a, G: StaticGraphViewOps, T: DocumentTemplate<G>> {
    graphs: &'a HashMap<String, VectorisedGraph<G, T>>,
}

impl<G: StaticGraphViewOps, T: DocumentTemplate<G>> VectorisedCluster<G, T> {
    pub fn new(graphs: &HashMap<String, VectorisedGraph<G, T>>) -> Self {
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
            .flat_map(|(name, graph)| graph.graph_documents.into_iter())
            .collect_vec();
        let scored_documents = score_documents(query, documents);
        let top_k = find_top_k(scored_documents, limit);

        top_k
            .map(|(doc, score)| match doc.entity_id {
                EntityId::Graph { name } => {
                    let graph = self.graphs.get(&name).unwrap();
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
