use crate::errors::GraphError;
use raphtory_storage::stage::{Stage, Stageable};

/// Stage and version methods for graphs.
pub trait StageOps: Stageable {
    fn stage(&self) -> Result<Stage<Self>, GraphError>
    where
        Self: Sized,
    {
        Stage::new(self).map_err(GraphError::from)
    }

    fn version(&self) -> Result<u32, GraphError> {
        Ok(self.core_graph().version()?)
    }

    fn src_version(&self) -> Result<Option<u32>, GraphError> {
        Ok(self.core_graph().src_version()?)
    }

    fn active_version(&self) -> Result<u32, GraphError> {
        Ok(self.core_graph().active_version()?)
    }

    fn all_versions(&self) -> Result<Vec<u32>, GraphError> {
        Ok(self.core_graph().all_versions()?)
    }

    fn set_active_version(&self, version: u32) -> Result<(), GraphError> {
        Ok(self.core_graph().set_active_version(version)?)
    }
}

impl<G: Stageable> StageOps for G {}
