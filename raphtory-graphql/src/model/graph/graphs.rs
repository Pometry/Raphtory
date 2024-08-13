use crate::data::get_graph_name;
use async_graphql::parser::Error;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use std::path::PathBuf;

#[derive(ResolvedObject)]
pub(crate) struct GqlGraphs {
    paths: Vec<PathBuf>,
}

impl GqlGraphs {
    pub fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }
}

#[ResolvedObjectFields]
impl GqlGraphs {
    async fn name(&self) -> Vec<String> {
        self.paths.iter().map(|path| get_graph_name(path)).collect()
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
