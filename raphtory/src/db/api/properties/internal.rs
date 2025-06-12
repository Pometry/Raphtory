use crate::errors::GraphError;
use crate::{core::storage::timeindex::AsTime, db::api::view::BoxedLIter};
use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use raphtory_api::{
    core::{
        entities::properties::prop::{Prop, PropType},
        storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
    },
    inherit::Base,
    iter::IntoDynBoxed,
};

#[enum_dispatch]
pub trait TemporalPropertyViewOps {
    fn dtype(&self, id: usize) -> PropType;
    fn temporal_value(&self, id: usize) -> Option<Prop>;

    fn temporal_iter(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)>;

    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)>;
    fn temporal_history_iter(&self, id: usize) -> BoxedLIter<i64> {
        self.temporal_iter(id).map(|(t, _)| t.t()).into_dyn_boxed()
    }

    fn temporal_history_iter_rev(&self, id: usize) -> BoxedLIter<i64> {
        self.temporal_iter_rev(id)
            .map(|(t, _)| t.t())
            .into_dyn_boxed()
    }

    fn temporal_history_date_time(&self, id: usize) -> Result<Vec<DateTime<Utc>>, GraphError> {
        self.temporal_history_iter(id)
            .map(|t| t.dt().map_err(GraphError::from))
            .collect::<Result<Vec<_>, GraphError>>()
    }

    fn temporal_values_iter(&self, id: usize) -> BoxedLIter<Prop> {
        self.temporal_iter(id).map(|(_, v)| v).into_dyn_boxed()
    }

    fn temporal_values_iter_rev(&self, id: usize) -> BoxedLIter<Prop> {
        self.temporal_iter_rev(id).map(|(_, v)| v).into_dyn_boxed()
    }

    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop>;
}

pub trait TemporalPropertiesRowView {
    fn rows(&self) -> BoxedLIter<(TimeIndexEntry, Vec<(usize, Prop)>)>;
}

#[enum_dispatch]
pub trait ConstantPropertiesOps: Send + Sync {
    /// Find id for property name (note this only checks the meta-data, not if the property actually exists for the entity)
    fn get_const_prop_id(&self, name: &str) -> Option<usize>;
    fn get_const_prop_name(&self, id: usize) -> ArcStr;
    fn const_prop_ids(&self) -> BoxedLIter<usize>;
    fn const_prop_keys(&self) -> BoxedLIter<ArcStr> {
        Box::new(self.const_prop_ids().map(|id| self.get_const_prop_name(id)))
    }
    fn const_prop_values(&self) -> BoxedLIter<Option<Prop>> {
        Box::new(self.const_prop_ids().map(|k| self.get_const_prop(k)))
    }
    fn get_const_prop(&self, id: usize) -> Option<Prop>;
}

#[enum_dispatch]
pub trait TemporalPropertiesOps {
    fn get_temporal_prop_id(&self, name: &str) -> Option<usize>;
    fn get_temporal_prop_name(&self, id: usize) -> ArcStr;

    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_>;
    fn temporal_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        Box::new(
            self.temporal_prop_ids()
                .map(|id| self.get_temporal_prop_name(id)),
        )
    }
}

pub trait PropertiesOps:
    TemporalPropertiesOps + TemporalPropertyViewOps + ConstantPropertiesOps
{
}

impl<P: TemporalPropertiesOps + TemporalPropertyViewOps + ConstantPropertiesOps> PropertiesOps
    for P
{
}

pub trait InheritTemporalPropertyViewOps: Base {}
pub trait InheritTemporalPropertiesOps: Base {}
pub trait InheritConstantPropertiesOps: Base {}
pub trait InheritPropertiesOps: Base {}

impl<P: InheritPropertiesOps> InheritConstantPropertiesOps for P {}
impl<P: InheritPropertiesOps> InheritTemporalPropertiesOps for P {}

impl<P: InheritTemporalPropertyViewOps + Send + Sync> TemporalPropertyViewOps for P
where
    P::Base: TemporalPropertyViewOps,
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
    fn temporal_iter(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.base().temporal_iter(id)
    }

    #[inline]
    fn temporal_iter_rev(&self, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.base().temporal_iter_rev(id)
    }

    #[inline]
    fn temporal_history_iter(&self, id: usize) -> BoxedLIter<i64> {
        self.base().temporal_history_iter(id)
    }

    #[inline]
    fn temporal_history_iter_rev(&self, id: usize) -> BoxedLIter<i64> {
        self.base().temporal_history_iter_rev(id)
    }

    #[inline]
    fn temporal_history_date_time(&self, id: usize) -> Result<Vec<DateTime<Utc>>, GraphError> {
        self.base().temporal_history_date_time(id)
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
    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        self.base().temporal_value_at(id, t)
    }
}

impl<P: InheritTemporalPropertiesOps> InheritTemporalPropertyViewOps for P {}

impl<P: InheritTemporalPropertiesOps + Send + Sync> TemporalPropertiesOps for P
where
    P::Base: TemporalPropertiesOps,
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
    fn temporal_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.base().temporal_prop_ids()
    }

    #[inline]
    fn temporal_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        self.base().temporal_prop_keys()
    }
}

impl<P: InheritConstantPropertiesOps + Send + Sync> ConstantPropertiesOps for P
where
    P::Base: ConstantPropertiesOps,
{
    #[inline]
    fn get_const_prop_id(&self, name: &str) -> Option<usize> {
        self.base().get_const_prop_id(name)
    }

    #[inline]
    fn get_const_prop_name(&self, id: usize) -> ArcStr {
        self.base().get_const_prop_name(id)
    }

    #[inline]
    fn const_prop_ids(&self) -> BoxedLIter<usize> {
        self.base().const_prop_ids()
    }

    #[inline]
    fn const_prop_keys(&self) -> BoxedLIter<ArcStr> {
        self.base().const_prop_keys()
    }

    #[inline]
    fn const_prop_values(&self) -> BoxedLIter<Option<Prop>> {
        self.base().const_prop_values()
    }

    #[inline]
    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.base().get_const_prop(id)
    }
}
