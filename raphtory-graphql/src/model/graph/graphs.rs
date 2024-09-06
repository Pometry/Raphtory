use std::fs;
use crate::data::get_graph_name;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use raphtory::core::utils::errors::GraphError;

#[derive(ResolvedObject)]
pub(crate) struct GqlGraphs {
    work_dir: PathBuf,
    paths: Vec<PathBuf>,
}

impl GqlGraphs {
    pub fn new(work_dir: PathBuf, paths: Vec<PathBuf>) -> Self {
        Self { work_dir, paths }
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

    async fn last_opened(&self) -> Result<Vec<i64>, GraphError> {
        let last_opened = self
            .paths
            .iter()
            .filter_map(|path| {
                let full_path = std::env::current_dir()
                    .unwrap()
                    .join(self.work_dir.clone())
                    .join(self.path.clone());

                let metadata = fs::metadata(full_path)?;

                let accessed_time = metadata.accessed()?;
                let accessed_time_duration = accessed_time.duration_since(UNIX_EPOCH)?;
                let accessed_time_seconds = accessed_time_duration.as_secs() as i64;
                accessed_time_seconds
            })
            .collect_vec();

        Ok(last_opened)
    }

    async fn last_updated(&self) -> Result<Vec<i64>, GraphError> {
        let last_updated = self
            .paths
            .iter()
            .filter_map(|path| {
                let full_path = std::env::current_dir()
                    .unwrap()
                    .join(self.work_dir.clone())
                    .join(self.path.clone());

                let metadata = fs::metadata(full_path)?;

                let modified_time = metadata.modified()?;
                let modified_time_duration = modified_time.duration_since(UNIX_EPOCH)?;
                let modified_time_seconds = modified_time_duration.as_secs() as i64;
                modified_time_seconds
            })
            .collect_vec();

        Ok(last_updated)
    }
}
