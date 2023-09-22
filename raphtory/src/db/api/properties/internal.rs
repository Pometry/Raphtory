use crate::{
    core::{ArcStr, Prop},
    db::api::view::internal::Base,
};
use enum_dispatch::enum_dispatch;

pub type Key = usize; //Fixme: This should really be the internal usize index but that means more reworking of the low-level api

#[enum_dispatch]
pub trait TemporalPropertyViewOps {
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.temporal_values(id).last().cloned()
    }
    fn temporal_history(&self, id: usize) -> Vec<i64>;
    fn temporal_values(&self, id: usize) -> Vec<Prop>;
    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        let history = self.temporal_history(id);
        match history.binary_search(&t) {
            Ok(index) => Some(self.temporal_values(id)[index].clone()),
            Err(index) => (index > 0).then(|| self.temporal_values(id)[index - 1].clone()),
        }
    }
}

#[enum_dispatch]
pub trait ConstPropertiesOps {
    /// Find id for property name (note this only checks the meta-data, not if the property actually exists for the entity)
    fn get_const_prop_id(&self, name: &str) -> Option<usize>;
    fn get_const_prop_name(&self, id: usize) -> ArcStr;
    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_>;
    fn const_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        Box::new(self.const_prop_ids().map(|id| self.get_const_prop_name(id)))
    }
    fn const_prop_values(&self) -> Vec<Prop> {
        self.const_prop_ids()
            .map(|k| {
                self.get_const_prop(k)
                    .expect("ids that come from the internal iterator should exist")
            })
            .collect()
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
    TemporalPropertiesOps + TemporalPropertyViewOps + ConstPropertiesOps
{
}

impl<P: TemporalPropertiesOps + TemporalPropertyViewOps + ConstPropertiesOps> PropertiesOps for P {}

pub trait InheritTemporalPropertyViewOps: Base {}
pub trait InheritTemporalPropertiesOps: Base {}
pub trait InheritStaticPropertiesOps: Base {}
pub trait InheritPropertiesOps: Base {}

impl<P: InheritPropertiesOps> InheritStaticPropertiesOps for P {}
impl<P: InheritPropertiesOps> InheritTemporalPropertiesOps for P {}

impl<P: InheritTemporalPropertyViewOps> TemporalPropertyViewOps for P
where
    P::Base: TemporalPropertyViewOps,
{
    #[inline]
    fn temporal_value(&self, id: usize) -> Option<Prop> {
        self.base().temporal_value(id)
    }

    #[inline]
    fn temporal_history(&self, id: usize) -> Vec<i64> {
        self.base().temporal_history(id)
    }

    #[inline]
    fn temporal_values(&self, id: usize) -> Vec<Prop> {
        self.base().temporal_values(id)
    }

    #[inline]
    fn temporal_value_at(&self, id: usize, t: i64) -> Option<Prop> {
        self.base().temporal_value_at(id, t)
    }
}

impl<P: InheritTemporalPropertiesOps> InheritTemporalPropertyViewOps for P {}

impl<P: InheritTemporalPropertiesOps> TemporalPropertiesOps for P
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

impl<P: InheritStaticPropertiesOps> ConstPropertiesOps for P
where
    P::Base: ConstPropertiesOps,
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
    fn const_prop_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        self.base().const_prop_ids()
    }

    #[inline]
    fn const_prop_keys(&self) -> Box<dyn Iterator<Item = ArcStr> + '_> {
        self.base().const_prop_keys()
    }

    #[inline]
    fn const_prop_values(&self) -> Vec<Prop> {
        self.base().const_prop_values()
    }

    #[inline]
    fn get_const_prop(&self, id: usize) -> Option<Prop> {
        self.base().get_const_prop(id)
    }
}
