use crate::db::api::view::BoxedLIter;
use raphtory_api::{
    core::{
        entities::properties::prop::{Prop, PropType},
        storage::{arc_str::ArcStr, timeindex::EventTime},
    },
    inherit::Base,
    iter::IntoDynBoxed,
};

pub trait InternalTemporalPropertyViewOps {
    fn dtype(&self, id: usize) -> PropType;
    fn temporal_value(&self, id: usize) -> Option<Prop>;

    fn temporal_iter(&self, id: usize) -> BoxedLIter<(EventTime, Prop)>;

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(EventTime, Prop)>;
    fn temporal_history_iter(&self, id: usize) -> BoxedLIter<EventTime> {
        self.temporal_iter(id).map(|(t, _)| t).into_dyn_boxed()
    }

    fn temporal_history_iter_rev(&self, id: usize) -> BoxedLIter<EventTime> {
        self.temporal_iter_rev(id).map(|(t, _)| t).into_dyn_boxed()
    }

    fn temporal_values_iter(&self, id: usize) -> BoxedLIter<Prop> {
        self.temporal_iter(id).map(|(_, v)| v).into_dyn_boxed()
    }

    fn temporal_values_iter_rev(&self, id: usize) -> BoxedLIter<Prop> {
        self.temporal_iter_rev(id).map(|(_, v)| v).into_dyn_boxed()
    }

    fn temporal_value_at(&self, id: usize, t: EventTime) -> Option<Prop>;
}

pub trait TemporalPropertiesRowView {
    fn rows(&self) -> BoxedLIter<(EventTime, Vec<(usize, Prop)>)>;
}

pub trait InternalMetadataOps: Send + Sync {
    /// Find id for property name (note this only checks the meta-data, not if the property actually exists for the entity)
    fn get_metadata_id(&self, name: &str) -> Option<usize>;
    fn get_metadata_name(&self, id: usize) -> ArcStr;
    fn metadata_ids(&self) -> BoxedLIter<usize>;
    fn metadata_keys(&self) -> BoxedLIter<ArcStr> {
        Box::new(self.metadata_ids().map(|id| self.get_metadata_name(id)))
    }
    fn metadata_values(&self) -> BoxedLIter<Option<Prop>> {
        Box::new(self.metadata_ids().map(|k| self.get_metadata(k)))
    }
    fn get_metadata(&self, id: usize) -> Option<Prop>;
}

pub trait InternalTemporalPropertiesOps: Send + Sync {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize>;
    fn get_temporal_prop_name(&self, id: usize) -> ArcStr;

    fn temporal_prop_ids(&self) -> BoxedLIter<usize>;
    fn temporal_prop_keys(&self) -> BoxedLIter<ArcStr> {
        Box::new(
            self.temporal_prop_ids()
                .map(|id| self.get_temporal_prop_name(id)),
        )
    }
}

pub trait InternalPropertiesOps:
    InternalTemporalPropertiesOps + InternalTemporalPropertyViewOps + InternalMetadataOps
{
}

impl<P: InternalTemporalPropertiesOps + InternalTemporalPropertyViewOps + InternalMetadataOps>
    InternalPropertiesOps for P
{
}

pub trait InheritTemporalPropertyViewOps: Base {}
pub trait InheritTemporalPropertiesOps: Base {}
pub trait InheritMetadataPropertiesOps: Base {}
pub trait InheritPropertiesOps: Base {}

impl<P: InheritPropertiesOps> InheritMetadataPropertiesOps for P {}
impl<P: InheritPropertiesOps> InheritTemporalPropertiesOps for P {}

impl<P: InheritTemporalPropertyViewOps + Send + Sync> InternalTemporalPropertyViewOps for P
where
    P::Base: InternalTemporalPropertyViewOps,
{
    #[inline]
    fn dtype(&self, id: usize) -> PropType {
        self.base().dtype(id)
    }
    #[inline]
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.base().temporal_value(id)
    }

    #[inline]
    fn temporal_iter(&self, id: usize) -> BoxedLIter<(EventTime, Prop)> {
        self.base().temporal_iter(id)
    }

    #[inline]
    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(EventTime, Prop)> {
        self.base().temporal_iter_rev(id)
    }

    #[inline]
    fn temporal_history_iter(&self, id: usize) -> BoxedLIter<EventTime> {
        self.base().temporal_history_iter(id)
    }

    #[inline]
    fn temporal_history_iter_rev(&self, id: usize) -> BoxedLIter<EventTime> {
        self.base().temporal_history_iter_rev(id)
    }

    #[inline]
    fn temporal_values_iter(&self, id: usize) -> BoxedLIter<Prop> {
        self.base().temporal_values_iter(id)
    }

    #[inline]
    fn temporal_values_iter_rev(&self, id: usize) -> BoxedLIter<Prop> {
        self.base().temporal_values_iter_rev(id)
    }

    #[inline]
    fn temporal_value_at(&self, id: usize, t: EventTime) -> Option<Prop> {
        self.base().temporal_value_at(id, t)
    }
}

impl<P: InheritTemporalPropertiesOps> InheritTemporalPropertyViewOps for P {}

impl<P: InheritTemporalPropertiesOps + Send + Sync> InternalTemporalPropertiesOps for P
where
    P::Base: InternalTemporalPropertiesOps,
{
    #[inline]
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize> {
        self.base().get_temporal_prop_id(name)
    }

    #[inline]
    fn get_temporal_prop_name(&self, id: usize) -> ArcStr {
        self.base().get_temporal_prop_name(id)
    }

    #[inline]
    fn temporal_prop_ids(&self) -> BoxedLIter<usize> {
        self.base().temporal_prop_ids()
    }

    #[inline]
    fn temporal_prop_keys(&self) -> BoxedLIter<ArcStr> {
        self.base().temporal_prop_keys()
    }
}

impl<P: InheritMetadataPropertiesOps + Send + Sync> InternalMetadataOps for P
where
    P::Base: InternalMetadataOps,
{
    #[inline]
    fn get_metadata_id(&self, name: &str) -> Option<usize> {
        self.base().get_metadata_id(name)
    }

    #[inline]
    fn get_metadata_name(&self, id: usize) -> ArcStr {
        self.base().get_metadata_name(id)
    }

    #[inline]
    fn metadata_ids(&self) -> BoxedLIter<usize> {
        self.base().metadata_ids()
    }

    #[inline]
    fn metadata_keys(&self) -> BoxedLIter<ArcStr> {
        self.base().metadata_keys()
    }

    #[inline]
    fn metadata_values(&self) -> BoxedLIter<Option<Prop>> {
        self.base().metadata_values()
    }

    #[inline]
    fn get_metadata(&self, id: usize) -> Option<Prop> {
        self.base().get_metadata(id)
    }
}
