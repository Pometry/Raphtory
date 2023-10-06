use crate::{
    arrow::{edge_chunk::EdgeChunk, Time},
    core::entities::{LayerIds, EID, VID},
    prelude::Layer,
};
use rayon::prelude::*;
use std::ops::Range;

pub struct CoreArrowEdgeView<'a> {
    eid: EID,
    offset: usize,
    chunk: &'a EdgeChunk,
}

impl<'a> CoreArrowEdgeView<'a> {
    pub fn has_layer(&self, layers: &LayerIds) -> bool {
        // FIXME: actually implement layers
        match layers {
            LayerIds::None => false,
            LayerIds::All => true,
            // FIXME: actually implement layers (right now only default layer exists)
            LayerIds::One(id) => id == &0,
            LayerIds::Multiple(ids) => ids.iter().any(|id| self.has_layer(&LayerIds::One(*id))),
        }
    }

    pub fn layer_ids(&self) -> LayerIds {
        // FIXME: actually implement layers (right now only default layer exists)
        LayerIds::One(0)
    }

    pub fn additions(&self) -> &[Time] {
        self.chunk.additions().into_value(self.offset)
    }

    pub fn active(&self, layer_ids: &LayerIds, w: Range<i64>) -> bool {
        let additions = self.additions();

        match layer_ids {
            LayerIds::None => false,
            LayerIds::All => match additions.binary_search(&w.start) {
                Ok(_) => true,
                Err(i) => additions.get(i).filter(|&t| t < &w.end).is_some(),
            },
            LayerIds::One(id) => {
                // FIXME: actually implement layers (right now only default layer exists)
                if *id == 0 {
                    self.active(&LayerIds::All, w)
                } else {
                    false
                }
            }
            LayerIds::Multiple(ids) => ids
                .par_iter()
                .any(|id| self.active(&LayerIds::One(*id), w.clone())),
        }
    }

    pub fn src(&self) -> VID {
        VID(self.chunk.source().value(self.offset) as usize)
    }

    pub fn dst(&self) -> VID {
        VID(self.chunk.destination().value(self.offset) as usize)
    }

    pub fn e_id(&self) -> EID {
        self.eid
    }
}
