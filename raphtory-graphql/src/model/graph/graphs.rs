use async_graphql::parser::Error;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};

#[derive(ResolvedObject)]
pub(crate) struct GqlGraphs {
    names: Vec<String>,
    namespaces: Vec<Option<String>>,
}

impl GqlGraphs {
    pub fn new(names: Vec<String>, namespaces: Vec<Option<String>>) -> Self {
        Self { names, namespaces }
    }
}

#[ResolvedObjectFields]
impl GqlGraphs {
    async fn name(&self) -> Result<Vec<String>, Error> {
        Ok(self.names.clone())
    }

    async fn path(&self) -> Result<Vec<String>, Error> {
        Ok(self
            .names
            .clone()
            .into_iter()
            .zip(self.namespaces.clone().into_iter())
            .map(|(name, namespace)| match namespace {
                Some(ns) => format!("{}/{}", ns, name),
                None => name,
            })
            .collect())
    }
}
