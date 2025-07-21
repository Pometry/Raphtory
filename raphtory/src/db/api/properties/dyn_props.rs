use crate::db::api::{
    properties::{
        internal::InternalPropertiesOps, Metadata, Properties, TemporalProperties,
        TemporalPropertyView,
    },
    view::{internal::Static, DynamicGraph},
};
use std::sync::Arc;

pub type DynProps = Arc<dyn InternalPropertiesOps + Send + Sync>;
pub type DynProperties = Properties<Arc<dyn InternalPropertiesOps + Send + Sync>>;

impl<P: InternalPropertiesOps + Clone + Send + Sync + Static + 'static> From<Properties<P>>
    for DynProperties
{
    fn from(value: Properties<P>) -> Self {
        Properties::new(Arc::new(value.props))
    }
}

impl From<Properties<DynamicGraph>> for DynProperties {
    fn from(value: Properties<DynamicGraph>) -> Self {
        let props: DynProps = Arc::new(value.props);
        Properties::new(props)
    }
}

pub type DynConstProperties = Metadata<'static, DynProps>;

impl<P: InternalPropertiesOps + Send + Sync + Static + 'static> From<Metadata<'static, P>>
    for DynConstProperties
{
    fn from(value: Metadata<P>) -> Self {
        Metadata::new(Arc::new(value.props))
    }
}

impl From<Metadata<'static, DynamicGraph>> for DynConstProperties {
    fn from(value: Metadata<'static, DynamicGraph>) -> Self {
        Metadata::new(Arc::new(value.props))
    }
}

pub type DynTemporalProperties = TemporalProperties<DynProps>;
pub type DynTemporalProperty = TemporalPropertyView<DynProps>;

impl<P: InternalPropertiesOps + Clone + Send + Sync + Static + 'static> From<TemporalProperties<P>>
    for DynTemporalProperties
{
    fn from(value: TemporalProperties<P>) -> Self {
        TemporalProperties::new(Arc::new(value.props))
    }
}
impl From<TemporalProperties<DynamicGraph>> for DynTemporalProperties {
    fn from(value: TemporalProperties<DynamicGraph>) -> Self {
        let props: Arc<dyn InternalPropertiesOps + Send + Sync> = Arc::new(value.props);
        TemporalProperties::new(props)
    }
}
