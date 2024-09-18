use crate::data::get_graph_name;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::core::utils::errors::GraphError;
use std::{fs, path::PathBuf, time::UNIX_EPOCH};

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

    async fn created(&self) -> Result<Vec<i64>, GraphError> {
        self.paths
            .iter()
            .map(|path| {
                let full_path = std::env::current_dir()?
                    .join(self.work_dir.clone())
                    .join(path);

                let metadata = fs::metadata(full_path.clone())?;

                let created_time = metadata.created()?;
                let created_time_duration = created_time.duration_since(UNIX_EPOCH)?;
                let created_time_millis = created_time_duration.as_millis() as i64;

                Ok(created_time_millis)
            })
            .collect()
    }

    async fn last_opened(&self) -> Result<Vec<i64>, GraphError> {
        self.paths
            .iter()
            .map(|path| {
                let full_path = std::env::current_dir()?
                    .join(self.work_dir.clone())
                    .join(path);

                let metadata = fs::metadata(full_path.clone())?;

                let accessed_time = metadata.accessed()?;
                let accessed_time_duration = accessed_time.duration_since(UNIX_EPOCH)?;
                let accessed_time_millis = accessed_time_duration.as_millis() as i64;

                Ok(accessed_time_millis)
            })
            .collect()
    }

    async fn last_updated(&self) -> Result<Vec<i64>, GraphError> {
        self.paths
            .iter()
            .map(|path| {
                let full_path = std::env::current_dir()?
                    .join(self.work_dir.clone())
                    .join(path);

                let metadata = fs::metadata(full_path.clone())?;

                let modified_time = metadata.modified()?;
                let modified_time_duration = modified_time.duration_since(UNIX_EPOCH)?;
                let modified_time_millis = modified_time_duration.as_millis() as i64;

                Ok(modified_time_millis)
            })
            .collect()
    }
}
