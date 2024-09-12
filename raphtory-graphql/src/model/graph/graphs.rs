use crate::data::ExistingGraphFolder;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;

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
            .filter_map(|folder| Some(folder.user_facing_path.to_str()?.to_owned()))
            .collect_vec();
        paths
    }
}
