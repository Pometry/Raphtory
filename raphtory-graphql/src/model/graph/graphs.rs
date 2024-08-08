use async_graphql::parser::Error;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use std::path::PathBuf;

#[derive(ResolvedObject)]
pub(crate) struct GqlGraphs {
    names: Vec<String>,
    paths: Vec<PathBuf>,
}

impl GqlGraphs {
    pub fn new(names: Vec<String>, paths: Vec<PathBuf>) -> Self {
        Self { names, paths }
    }
}

#[ResolvedObjectFields]
impl GqlGraphs {
    async fn name(&self) -> Result<Vec<String>, Error> {
        Ok(self.names.clone())
    }

    async fn path(&self) -> Result<Vec<String>, Error> {
        let paths = self
            .paths
            .iter()
            .map(|path| path.display().to_string())
            .collect_vec();
        Ok(paths)
    }
}
