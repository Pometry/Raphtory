use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::core::utils::errors::GraphError;

use crate::paths::ExistingGraphFolder;

#[derive(ResolvedObject)]
pub(crate) struct GqlGraphs {
    folders: Vec<ExistingGraphFolder>,
}

impl GqlGraphs {
    pub fn new(paths: Vec<ExistingGraphFolder>) -> Self {
        Self { folders: paths }
    }
}

#[ResolvedObjectFields]
impl GqlGraphs {
    //Name and path here do not return a result as we only want to let the user know about
    //valid graph paths. No point blowing up if there is one busted fule

    async fn name(&self) -> Vec<String> {
        self.folders
            .iter()
            .filter_map(|folder| folder.get_graph_name().ok())
            .collect()
    }

    async fn path(&self) -> Vec<String> {
        let paths = self
            .folders
            .iter()
            .map(|folder| folder.get_original_path_str().to_owned())
            .collect_vec();
        paths
    }

    async fn namespace(&self) -> Vec<Option<String>> {
        let folders = self
            .folders
            .iter()
            .map(|folder| {
                folder
                    .get_original_path()
                    .parent()
                    .and_then(|p| p.to_str().map(|s| s.to_string()))
            })
            .collect();
        folders
    }

    async fn created(&self) -> Result<Vec<i64>, GraphError> {
        self.folders.iter().map(|folder| folder.created()).collect()
    }

    async fn last_opened(&self) -> Result<Vec<i64>, GraphError> {
        self.folders
            .iter()
            .map(|folder| folder.last_opened())
            .collect()
    }

    async fn last_updated(&self) -> Result<Vec<i64>, GraphError> {
        self.folders
            .iter()
            .map(|folder| folder.last_updated())
            .collect()
    }
}
