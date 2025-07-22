use crate::{
    core::utils::iter::GenLockedIter,
    db::api::{
        properties::{internal::InternalMetadataOps, PropertiesOps},
        view::BoxedLIter,
    },
};
use raphtory_api::{
    core::{entities::properties::prop::Prop, storage::arc_str::ArcStr},
    iter::IntoDynBoxed,
};
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub struct Metadata<'a, P: InternalMetadataOps> {
    pub(crate) props: P,
    _marker: std::marker::PhantomData<&'a P>,
}

impl<'a, P: InternalMetadataOps + Sync> Debug for Metadata<'a, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'a, P: InternalMetadataOps + Sync> PropertiesOps for Metadata<'a, P> {
    fn get_by_id(&self, id: usize) -> Option<Prop> {
        self.props.get_metadata(id)
    }

    fn get_id(&self, key: &str) -> Option<usize> {
        self.props.get_metadata_id(key)
    }

    fn keys(&self) -> impl Iterator<Item = ArcStr> + Send + Sync + '_ {
        self.props.metadata_keys()
    }

    fn ids(&self) -> impl Iterator<Item = usize> + Send + Sync + '_ {
        self.props.metadata_ids()
    }
}

impl<'a, P: InternalMetadataOps + Sync> Metadata<'a, P> {
    pub(crate) fn new(props: P) -> Self {
        Self {
            props,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, P: InternalMetadataOps + Sync + 'a> IntoIterator for Metadata<'a, P> {
    type Item = (ArcStr, Option<Prop>);
    type IntoIter = BoxedLIter<'a, Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(GenLockedIter::from(self, |metadata| {
            metadata.iter().into_dyn_boxed()
        }))
    }
}

impl<'a, P: InternalMetadataOps + Sync> IntoIterator for &'a Metadata<'a, P> {
    type Item = (ArcStr, Option<Prop>);
    type IntoIter = Box<dyn Iterator<Item = (ArcStr, Option<Prop>)> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().into_dyn_boxed()
    }
}

impl<'a, P: InternalMetadataOps + Sync, O: PropertiesOps> PartialEq<O> for Metadata<'a, P> {
    fn eq(&self, other: &O) -> bool {
        self.iter().eq(other.iter())
    }
}
