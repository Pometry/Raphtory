#[cfg(feature = "arrow")]
use crate::arrow::edge::CoreArrowEdgeView;
use crate::{
    core::{
        entities::{edges::edge_store::EdgeStore, LayerIds, EID, VID},
        storage::{
            timeindex::{LayeredIndex, SortedTimeIndexT, TimeIndexOps, TimeIndexView},
            Entry,
        },
        Prop,
    },
    db::api::view::internal::{core_views::properties::TPropView, Base},
};
use std::ops::Deref;

pub trait CoreEdgeOps {
    /// src internal vertex id
    fn src(&self) -> VID;
    /// dst internal vertex id
    fn dst(&self) -> VID;
    /// internal edge id
    fn eid(&self) -> EID;

    /// view of all addition events of the edge in the specified layers
    fn additions<'a>(&'a self, layers: &'a LayerIds) -> TimeIndexView<'a>;

    /// view of all deletion events of the edge in the specified layers
    fn deletions<'a>(&'a self, layers: &'a LayerIds) -> TimeIndexView<'a>;

    /// const property of the edge
    ///
    /// # Arguments
    /// * `layers` - only consider specified layers
    /// * `id` - internal id of the property
    fn const_prop(&self, layers: &LayerIds, id: usize) -> Option<Prop>;

    /// check if const property of the edge exists
    ///
    /// # Arguments
    /// * `layers` - only consider specified layers
    /// * `id` - internal id of the property
    fn has_const_prop(&self, layers: &LayerIds, id: usize) -> bool;

    /// temporal property of the edge
    ///
    /// # Arguments
    /// * `layers` - only consider specified layers
    /// * `id` - internal id of the property
    fn temporal_prop(&self, layers: &LayerIds, id: usize) -> Option<TPropView>;

    /// check if temporal property of the edge exists
    ///
    /// # Arguments
    /// * `layers` - only consider specified layers
    /// * `id` - internal id of the property
    fn has_temporal_prop(&self, layers: &LayerIds, id: usize) -> bool;

    /// check if edge exists in a layer (i.e., it has addition or deletion events in the layer)
    ///
    /// # Arguments
    /// *
    fn has_layer(&self, layers: &LayerIds) -> bool {
        !self.additions(layers).is_empty() || !self.deletions(layers).is_empty()
    }

    fn layer_ids(&self) -> LayerIds;
}

impl<E: Deref<Target = EdgeStore>> CoreEdgeOps for E {
    #[inline]
    fn src(&self) -> VID {
        self.deref().src()
    }

    #[inline]
    fn dst(&self) -> VID {
        self.deref().dst()
    }

    #[inline]
    fn eid(&self) -> EID {
        self.deref().eid
    }

    fn additions<'a>(&'a self, layers: &'a LayerIds) -> TimeIndexView<'a> {
        LayeredIndex::new(layers, self.deref().additions().as_ref()).into()
    }

    fn deletions<'a>(&'a self, layers: &'a LayerIds) -> TimeIndexView<'a> {
        LayeredIndex::new(layers, self.deref().deletions().as_ref()).into()
    }

    fn const_prop(&self, layers: &LayerIds, id: usize) -> Option<Prop> {
        todo!()
    }

    fn has_const_prop(&self, layers: &LayerIds, id: usize) -> bool {
        todo!()
    }

    fn temporal_prop(&self, layers: &LayerIds, id: usize) -> Option<TPropView> {
        todo!()
    }

    fn has_temporal_prop(&self, layers: &LayerIds, id: usize) -> bool {
        todo!()
    }

    #[inline]
    fn layer_ids(&self) -> LayerIds {
        self.deref().layer_ids()
    }
}

#[cfg(feature = "arrow")]
impl<'a> CoreEdgeOps for CoreArrowEdgeView<'a> {
    #[inline]
    fn src(&self) -> VID {
        self.src()
    }

    #[inline]
    fn dst(&self) -> VID {
        self.dst()
    }

    #[inline]
    fn eid(&self) -> EID {
        self.e_id()
    }

    fn additions(&self, layers: &LayerIds) -> TimeIndexView {
        SortedTimeIndexT(self.additions_ref()).into()
    }

    fn deletions(&self, layers: &LayerIds) -> TimeIndexView {
        SortedTimeIndexT(self.deletions_ref()).into()
    }

    fn const_prop(&self, layers: &LayerIds, id: usize) -> Option<Prop> {
        todo!()
    }

    fn has_const_prop(&self, layers: &LayerIds, id: usize) -> bool {
        todo!()
    }

    fn temporal_prop(&self, layers: &LayerIds, id: usize) -> Option<TPropView> {
        todo!()
    }

    fn has_temporal_prop(&self, layers: &LayerIds, id: usize) -> bool {
        todo!()
    }

    fn layer_ids(&self) -> LayerIds {
        todo!()
    }
}

pub enum CoreEdgeView<'a> {
    MemRef(&'a EdgeStore),
    MemEntry(Entry<'a, EdgeStore>),
    #[cfg(feature = "arrow")]
    Arrow(CoreArrowEdgeView<'a>),
}

impl<'a> From<Entry<'a, EdgeStore>> for CoreEdgeView<'a> {
    fn from(value: Entry<'a, EdgeStore>) -> Self {
        Self::MemEntry(value)
    }
}

impl<'a> From<CoreArrowEdgeView<'a>> for CoreEdgeView<'a> {
    fn from(value: CoreArrowEdgeView<'a>) -> Self {
        Self::Arrow(value)
    }
}

impl<'a> From<&'a EdgeStore> for CoreEdgeView<'a> {
    fn from(value: &'a EdgeStore) -> Self {
        Self::MemRef(value)
    }
}

impl<'a> CoreEdgeOps for CoreEdgeView<'a> {
    #[inline]
    fn src(&self) -> VID {
        match self {
            CoreEdgeView::MemEntry(v) => v.src(),
            CoreEdgeView::Arrow(v) => v.src(),
            CoreEdgeView::MemRef(v) => v.src(),
        }
    }

    #[inline]
    fn dst(&self) -> VID {
        match self {
            CoreEdgeView::MemEntry(v) => v.dst(),
            CoreEdgeView::Arrow(v) => v.dst(),
            CoreEdgeView::MemRef(v) => v.dst(),
        }
    }

    #[inline]
    fn eid(&self) -> EID {
        match self {
            CoreEdgeView::MemEntry(v) => v.eid(),
            CoreEdgeView::Arrow(v) => v.eid(),
            CoreEdgeView::MemRef(v) => v.eid(),
        }
    }

    #[inline]
    fn additions<'b>(&'b self, layers: &'b LayerIds) -> TimeIndexView<'b> {
        match self {
            CoreEdgeView::MemEntry(v) => v.additions(layers),
            CoreEdgeView::Arrow(v) => v.additions(layers),
            CoreEdgeView::MemRef(v) => v.additions(layers),
        }
    }

    #[inline]
    fn deletions<'b>(&'b self, layers: &'b LayerIds) -> TimeIndexView<'b> {
        match self {
            CoreEdgeView::MemEntry(v) => v.deletions(layers),
            CoreEdgeView::Arrow(v) => v.deletions(layers),
            CoreEdgeView::MemRef(v) => v.deletions(layers),
        }
    }

    #[inline]
    fn const_prop(&self, layers: &LayerIds, id: usize) -> Option<Prop> {
        match self {
            CoreEdgeView::MemEntry(v) => v.const_prop(layers, id),
            CoreEdgeView::Arrow(v) => v.const_prop(layers, id),
            CoreEdgeView::MemRef(v) => v.const_prop(layers, id),
        }
    }

    fn has_const_prop(&self, layers: &LayerIds, id: usize) -> bool {
        todo!()
    }

    #[inline]
    fn temporal_prop(&self, layers: &LayerIds, id: usize) -> Option<TPropView> {
        match self {
            CoreEdgeView::MemEntry(v) => v.temporal_prop(layers, id),
            CoreEdgeView::Arrow(v) => v.temporal_prop(layers, id),
            CoreEdgeView::MemRef(v) => v.temporal_prop(layers, id),
        }
    }

    fn has_temporal_prop(&self, layers: &LayerIds, id: usize) -> bool {
        todo!()
    }

    fn layer_ids(&self) -> LayerIds {
        match self {
            CoreEdgeView::MemRef(v) => v.layer_ids(),
            CoreEdgeView::MemEntry(v) => v.layer_ids(),
            CoreEdgeView::Arrow(v) => v.layer_ids(),
        }
    }
}
