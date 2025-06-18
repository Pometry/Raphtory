use raphtory_core::entities::VID;

use crate::TemporalGraph;

#[derive(Debug, Copy, Clone)]
pub struct LockedNodeEntry<'a, EXT> {
    vid: VID,
    node_support: &'a ReadLockedTemporalGraph<EXT>,
}

impl<'a, EXT> LockedNodeEntry<'a, EXT> {
    pub fn new(vid: VID, node_support: &'a ReadLockedTemporalGraph<EXT>) -> Self {
        Self { vid, node_support }
    }

    pub fn vid(&self) -> VID {
        self.vid
    }

    pub fn node_support(&self) -> &ReadLockedTemporalGraph<EXT> {
        &self.node_support
    }
}

#[derive(Debug, Copy, Clone)]
pub struct UnlockedNodeEntry<'a, EXT> {
    vid: VID,
    node_support: &'a TemporalGraph<EXT>,
}

impl<'a, EXT> UnlockedNodeEntry<'a, EXT> {
    pub fn new(vid: VID, node_support: &'a TemporalGraph<EXT>) -> Self {
        Self { vid, node_support }
    }

    pub fn vid(&self) -> VID {
        self.vid
    }

    pub fn node_support(&self) -> &TemporalGraph<EXT> {
        &self.node_support
    }
}
