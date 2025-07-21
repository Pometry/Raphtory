use crate::{
    core::utils::iter::GenLockedIter,
    db::api::{
        properties::{internal::InternalConstantPropertiesOps, PropertiesOps},
        view::BoxedLIter,
    },
};
use raphtory_api::{
    core::{entities::properties::prop::Prop, storage::arc_str::ArcStr},
    iter::IntoDynBoxed,
};
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct ConstantProperties<'a, P: InternalConstantPropertiesOps> {
    pub(crate) props: P,
    _marker: std::marker::PhantomData<&'a P>,
}

impl<'a, P: InternalConstantPropertiesOps + Sync> Debug for ConstantProperties<'a, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'a, P: InternalConstantPropertiesOps + Sync> PropertiesOps for ConstantProperties<'a, P> {
    fn get_by_id(&self, id: usize) -> Option<Prop> {
        self.props.get_const_prop(id)
    }

    fn get_id(&self, key: &str) -> Option<usize> {
        self.props.get_const_prop_id(key)
    }

    fn keys(&self) -> impl Iterator<Item = ArcStr> + Send + Sync + '_ {
        self.props.const_prop_keys()
    }

    fn ids(&self) -> impl Iterator<Item = usize> + Send + Sync + '_ {
        self.props.const_prop_ids()
    }
}

impl<'a, P: InternalConstantPropertiesOps + Sync> ConstantProperties<'a, P> {
    pub(crate) fn new(props: P) -> Self {
        Self {
            props,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, P: InternalConstantPropertiesOps + Sync + 'a> IntoIterator for ConstantProperties<'a, P> {
    type Item = (ArcStr, Option<Prop>);
    type IntoIter = BoxedLIter<'a, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(GenLockedIter::from(self, |const_prop| {
            const_prop.iter().into_dyn_boxed()
        }))
    }
}

impl<'a, P: InternalConstantPropertiesOps + Sync> IntoIterator for &'a ConstantProperties<'a, P> {
    type Item = (ArcStr, Option<Prop>);
    type IntoIter = Box<dyn Iterator<Item = (ArcStr, Option<Prop>)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().into_dyn_boxed()
    }
}

impl<'a, P: InternalConstantPropertiesOps + Sync, O: PropertiesOps> PartialEq<O>
    for ConstantProperties<'a, P>
{
    fn eq(&self, other: &O) -> bool {
        self.iter().eq(other.iter())
    }
}
