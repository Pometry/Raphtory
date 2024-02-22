use super::{EdgeLike, TimeIndexLike};
use crate::{
    core::{
        entities::{
            edges::edge_store::EdgeStore,
            properties::tprop::{LayeredTProp, LockedLayeredTProp},
            LayerIds,
        },
        storage::{timeindex::TimeIndex, ArcEntry},
    },
    db::api::view::IntoDynBoxed,
    prelude::TimeIndexEntry,
};
use std::{marker::PhantomData, ops::Deref};

#[cfg(feature = "arrow")]
use {crate::arrow::edge::Edge, std::iter};

pub enum CoreEdgeView<'a> {
    Mem(ArcEntry<EdgeStore>, PhantomData<&'a ()>),
    #[cfg(feature = "arrow")]
    Arrow(Edge<'a>),
}

impl<'a> Deref for CoreEdgeView<'a> {
    type Target = dyn EdgeLike + 'a;

    fn deref(&self) -> &Self::Target {
        match self {
            CoreEdgeView::Mem(e, _) => e.deref(),
            #[cfg(feature = "arrow")]
            CoreEdgeView::Arrow(e) => e,
        }
    }
}

impl CoreEdgeView<'_> {
    pub(crate) fn has_temporal_prop(&self, layer_ids: &LayerIds, prop_id: usize) -> bool {
        match self {
            CoreEdgeView::Mem(e, _) => e.has_temporal_prop(layer_ids, prop_id),
            #[cfg(feature = "arrow")]
            CoreEdgeView::Arrow(e) => e.has_temporal_prop(prop_id),
        }
    }

    pub(crate) fn last_deletion_before(
        &self,
        layer_ids: &LayerIds,
        t: i64,
    ) -> Option<TimeIndexEntry> {
        match self {
            CoreEdgeView::Mem(e, _) => e.last_deletion_before(layer_ids, t),
            #[cfg(feature = "arrow")]
            CoreEdgeView::Arrow(e) => e.last_deletion_before(layer_ids, t),
        }
    }

    pub fn layer_ids_iter(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            CoreEdgeView::Mem(e, _) => Box::new(e.layer_ids_iter()),
            #[cfg(feature = "arrow")]
            CoreEdgeView::Arrow(e) => Box::new(e.layer_ids_iter()),
        }
    }

    pub(crate) fn temporal_prop_layer(
        &self,
        layer_id: usize,
        prop_id: usize,
    ) -> Option<Box<dyn LayeredTProp + '_>> {
        match self {
            CoreEdgeView::Mem(e, _) => e.temporal_prop_layer(layer_id, prop_id).map(|t_prop| {
                let what: Box<dyn LayeredTProp + '_> = Box::new(LockedLayeredTProp::One(t_prop));
                what
            }),
            #[cfg(feature = "arrow")]
            CoreEdgeView::Arrow(e) => e.temporal_property(layer_id, prop_id),
        }
    }

    pub fn layer_ids(&self) -> LayerIds {
        match self {
            CoreEdgeView::Mem(e, _) => e.layer_ids(),
            #[cfg(feature = "arrow")]
            CoreEdgeView::Arrow(e) => e.layer_ids(),
        }
    }

    pub fn updates_iter<'a>(
        &'a self,
        layers: &'a LayerIds,
    ) -> Box<dyn Iterator<Item = (usize, TimeIndexLike<'a>, &'a TimeIndex<TimeIndexEntry>)> + 'a>
    {
        match self {
            CoreEdgeView::Mem(e, _) => e
                .updates_iter(layers)
                .map(|(l, a, d)| (l, TimeIndexLike::Ref(a), d))
                .into_dyn_boxed(),
            #[cfg(feature = "arrow")]
            CoreEdgeView::Arrow(e) => {
                let layer = e.layer();
                if layers.contains(&layer) {
                    let boxed = TimeIndexLike::External(e.timestamps());
                    iter::once((layer, boxed, &TimeIndex::Empty)).into_dyn_boxed()
                } else {
                    iter::empty().into_dyn_boxed()
                }
            }
        }
    }
}
