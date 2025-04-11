use crate::{
    db::api::view::StaticGraphViewOps,
    prelude::Graph,
    vectors::{
        entity_id::EntityId,
        similarity_search_utils::{find_top_k, score_documents},
        vectorised_graph::VectorisedGraph,
        Document, Embedding,
    },
};
use itertools::Itertools;
use std::collections::HashMap;

pub struct VectorisedCluster<'a, G: StaticGraphViewOps> {
    graphs: &'a HashMap<String, VectorisedGraph<G>>,
}

impl<'a, G: StaticGraphViewOps> VectorisedCluster<'a, G> {
    pub fn new(graphs: &'a HashMap<String, VectorisedGraph<G>>) -> Self {
        Self { graphs }
    }

    pub fn search_graph_documents(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Vec<Document<G>> {
        self.search_graph_documents_with_scores(query, limit, window)
            .into_iter()
            .map(|(document, _score)| document)
            .collect_vec()
    }

    pub fn search_graph_documents_with_scores(
        &self,
        query: &Embedding,
        limit: usize,
        window: Option<(i64, i64)>,
    ) -> Vec<(Document<G>, f32)> {
        let documents = self
            .graphs
            .iter()
            .flat_map(|(_name, graph)| graph.graph_documents.read().clone())
            .filter(|doc| doc.exists_on_window::<Graph>(None, &window))
            .collect_vec();
        let scored_documents = score_documents(query, documents);
        let top_k = find_top_k(scored_documents, limit);

        top_k
            .map(|(doc, score)| match &doc.entity_id {
                EntityId::Graph { name } => {
                    let name = name.clone().unwrap();
                    let graph = self.graphs.get(&name).unwrap();
                    (doc.regenerate(&graph.source_graph, &graph.template), score)
                }
                _ => panic!("got document that is not related to any graph"),
            })
            .collect_vec()
    }
}
