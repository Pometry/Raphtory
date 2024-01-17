use crate::{
    db::{
        api::view::StaticGraphViewOps,
        graph::{edge::EdgeView, node::NodeView},
    },
    vectors::{
        document_ref::DocumentRef, document_template::DocumentTemplate, entity_id::EntityId,
        vectorised_graph::VectorisedGraph, DocumentInput, DocumentOps, Embedding,
        EmbeddingFunction,
    },
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter},
    marker::PhantomData,
    path::Path,
    sync::Arc,
};

#[derive(Serialize, Deserialize)]
struct StoredDocument {
    reference: DocumentRef,
    content: String,
}

#[derive(Deserialize, Serialize)]
pub struct StoredVectorisedGraph {
    pub name: String,
    pub(crate) graph_documents: Vec<StoredDocument>,
    pub(crate) node_documents: HashMap<EntityId, Vec<StoredDocument>>,
    pub(crate) edge_documents: HashMap<EntityId, Vec<StoredDocument>>,
}

struct StoredDocumentTemplate<G: StaticGraphViewOps> {
    pub(crate) graph_documents: Vec<DocumentInput>,
    pub(crate) node_documents: HashMap<EntityId, Vec<DocumentInput>>,
    pub(crate) edge_documents: HashMap<EntityId, Vec<DocumentInput>>,
    phantom: PhantomData<G>,
}

impl<G: StaticGraphViewOps> From<&StoredVectorisedGraph> for StoredDocumentTemplate<G> {
    fn from(value: &StoredVectorisedGraph) -> Self {
        Self {
            graph_documents: from_document_vector(&value.graph_documents),
            node_documents: from_document_hashmap(&value.node_documents),
            edge_documents: from_document_hashmap(&value.edge_documents),
            phantom: Default::default(),
        }
    }
}

fn from_document_hashmap(
    stored_documents: &HashMap<EntityId, Vec<StoredDocument>>,
) -> HashMap<EntityId, Vec<DocumentInput>> {
    stored_documents
        .iter()
        .map(|(id, docs)| (id.clone(), from_document_vector(docs)))
        .collect()
}

fn from_document_vector(stored_documents: &Vec<StoredDocument>) -> Vec<DocumentInput> {
    stored_documents
        .iter()
        .map(|doc| DocumentInput {
            content: doc.content.clone(),
            life: doc.reference.life,
        })
        .collect_vec()
}

impl<G: StaticGraphViewOps> DocumentTemplate<G> for StoredDocumentTemplate<G> {
    fn graph(&self, graph: &G) -> Box<dyn Iterator<Item = DocumentInput>> {
        // TODO: should be having Arc inside of StoredDocumentTemplate to avoid clonig here as cannot have lifetimes in the output
        let cloned = self.graph_documents.clone();
        Box::new(cloned.into_iter())
    }

    fn node(&self, node: &NodeView<G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        let id = EntityId::from_node(node);
        let docs = self.node_documents.get(&id);
        let cloned = docs.map(|v| v.clone()).unwrap_or_else(|| vec![]);
        Box::new(cloned.into_iter())
    }

    fn edge(&self, edge: &EdgeView<G, G>) -> Box<dyn Iterator<Item = DocumentInput>> {
        let id = EntityId::from_edge(edge);
        let docs = self.edge_documents.get(&id);
        let cloned = docs.map(|v| v.clone()).unwrap_or_else(|| vec![]);
        Box::new(cloned.into_iter())
    }
}

struct MissingEmbedding;

impl EmbeddingFunction for MissingEmbedding {
    fn call(&self, texts: Vec<String>) -> BoxFuture<'static, Vec<Embedding>> {
        panic!("Missing embedding function as the graph was loaded from a file");
    }
}

fn extract_references(documents: Vec<StoredDocument>) -> Vec<DocumentRef> {
    documents.into_iter().map(|doc| doc.reference).collect_vec()
}

fn regenerate_documents<G: StaticGraphViewOps, T: DocumentTemplate<G>>(
    documents: &Vec<DocumentRef>,
    graph: &G,
    template: &T,
) -> Vec<StoredDocument> {
    documents
        .iter()
        .map(|doc| StoredDocument {
            reference: doc.clone(),
            content: doc.regenerate(graph, template).into_content(),
        })
        .collect_vec()
}

impl StoredVectorisedGraph {
    pub fn load_vectorised_graph<G: StaticGraphViewOps>(
        self,
        graph: G,
    ) -> Option<VectorisedGraph<G, StoredDocumentTemplate<G>>> {
        let template: StoredDocumentTemplate<G> = (&self).into();

        let Self {
            graph_documents,
            node_documents,
            edge_documents,
            ..
        } = self;

        let graph_documents = extract_references(graph_documents);
        let node_documents: HashMap<_, _> = node_documents
            .into_iter()
            .map(|(key, value)| (key, extract_references(value)))
            .collect();
        let edge_documents: HashMap<_, _> = edge_documents
            .into_iter()
            .map(|(key, value)| (key, extract_references(value)))
            .collect();

        Some(VectorisedGraph::new(
            graph,
            Arc::new(template),
            Arc::new(MissingEmbedding),
            Arc::new(graph_documents),
            Arc::new(node_documents),
            Arc::new(edge_documents),
            vec![],
        ))
    }

    pub fn save_vectorised_graph<G: StaticGraphViewOps, T: DocumentTemplate<G>>(
        graph: VectorisedGraph<G, T>,
        path: &Path,
    ) {
        let source = &graph.source_graph;
        let template = graph.template.as_ref();
        let name = graph
            .source_graph
            .properties()
            .get("name")
            .unwrap()
            .to_string();
        let graph_documents =
            regenerate_documents(graph.graph_documents.as_ref(), source, template);
        let node_documents = graph
            .node_documents
            .iter()
            .map(|(id, docs)| (id.clone(), regenerate_documents(docs, source, template)))
            .collect();
        let edge_documents = graph
            .edge_documents
            .iter()
            .map(|(id, docs)| (id.clone(), regenerate_documents(docs, source, template)))
            .collect();

        let graph_embeddings = Self {
            name,
            graph_documents,
            node_documents,
            edge_documents,
        };

        graph_embeddings.save_to_path(path);
    }

    pub fn load_from_path(path: &Path) -> Option<Self> {
        let file = File::open(&path).ok()?;
        let mut reader = BufReader::new(file);
        bincode::deserialize_from(&mut reader).ok()
    }

    pub fn save_to_path(&self, path: &Path) {
        let file = File::open(&path)
            .ok()
            .expect("Couldn't create file to store embedding store");
        let mut writer = BufWriter::new(file);
        bincode::serialize_into(&mut writer, self).expect("Couldn't serialize embedding store");
    }
}
