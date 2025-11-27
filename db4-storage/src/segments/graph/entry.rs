use crate::api::graph::{GraphEntryOps, GraphRefOps};
use crate::generic_t_props::WithTProps;
use crate::segments::graph::segment::MemGraphProps;
use crate::GraphTProps;
use parking_lot::RwLockReadGuard;
use raphtory_api::core::entities::properties::prop::Prop;
use raphtory_core::entities::properties::tprop::TPropCell;
use std::ops::Deref;

/// A borrowed view enabling read operations on an in-memory graph segment.
pub struct MemGraphEntry<'a> {
    mem: RwLockReadGuard<'a, MemGraphProps>,
}

impl<'a> MemGraphEntry<'a> {
    pub fn new(mem: RwLockReadGuard<'a, MemGraphProps>) -> Self {
        Self { mem }
    }
}

impl<'a> GraphEntryOps<'a> for MemGraphEntry<'a> {
    type Ref<'b> = MemGraphRef<'b>
    where
        'a: 'b;

    fn as_ref<'b>(&'b self) -> Self::Ref<'b>
    where
        'a: 'b,
    {
        MemGraphRef {
            mem: self.mem.deref(),
        }
    }
}

/// A lightweight, copyable reference to graph properties.
#[derive(Copy, Clone, Debug)]
pub struct MemGraphRef<'a> {
    mem: &'a MemGraphProps,
}

impl<'a> MemGraphRef<'a> {
    pub fn new(mem: &'a MemGraphProps) -> Self {
        Self { mem }
    }
}

impl<'a> WithTProps<'a> for MemGraphRef<'a> {
    type TProp = TPropCell<'a>;

    fn num_layers(&self) -> usize {
        // TODO: Support multiple layers for graph props.
        1
    }

    fn into_t_props(
        self,
        _layer_id: usize,
        prop_id: usize,
    ) -> impl Iterator<Item = Self::TProp> + Send + Sync + 'a {
        // Graph properties are stored in DEFAULT_LAYER
        self.mem.get_temporal_prop(prop_id).into_iter()
    }
}

impl<'a> GraphRefOps<'a> for MemGraphRef<'a> {
    type TProps = GraphTProps<'a>;

    fn get_temporal_prop(self, prop_id: usize) -> Self::TProps {
        GraphTProps::new_with_layer(self, 0, prop_id)
    }

    fn get_metadata(self, prop_id: usize) -> Option<Prop> {
        self.mem.get_metadata(prop_id)
    }
}
