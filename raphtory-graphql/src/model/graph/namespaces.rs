use crate::model::graph::namespace::Namespace;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};

#[derive(ResolvedObject, Clone)]
pub(crate) struct Namespaces {
    namespaces: Vec<Namespace>,
}

impl Namespaces {
    pub(crate) fn new(namespaces: Vec<Namespace>) -> Self {
        Self { namespaces }
    }
}

#[ResolvedObjectFields]
impl Namespaces {
    async fn list(&self) -> Vec<Namespace> {
        self.namespaces.clone()
    }

    async fn page(&self, limit: usize, offset: usize) -> Vec<Namespace> {
        let start = offset * limit;
        self.namespaces
            .iter()
            .skip(start)
            .take(limit)
            .cloned()
            .collect()
    }

    async fn count(&self) -> usize {
        self.namespaces.len()
    }
}
