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
    //Name and path here do not return a result as we only want to let the user know about
    //valid graph paths. No point blowing up if there is one busted fule
    async fn name(&self) -> Vec<String> {
        self.paths
            .iter()
            .filter_map(|path| get_graph_name(path).ok())
            .collect()
    }

    async fn path(&self) -> Vec<String> {
        let paths = self
            .paths
            .iter()
            .filter_map(|path| path.to_str().map(|s| s.to_string()))
            .collect_vec();
        paths
    }
}
