use crate::model::graph::{edge::GqlEdge, namespace::Namespace};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use tokio::task::spawn_blocking;

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
        let self_clone = self.clone();
        spawn_blocking(move || {
            let start = offset * limit;
            self_clone
                .namespaces
                .iter()
                .map(|n| n.clone())
                .skip(start)
                .take(limit)
                .collect()
        })
        .await
        .unwrap()
    }

    async fn count(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.namespaces.iter().count())
            .await
            .unwrap()
    }
}
