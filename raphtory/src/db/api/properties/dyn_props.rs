use crate::{
    db::api::{
        properties::{
            internal::{InheritPropertiesOps, PropertiesOps},
            ConstProperties, Properties, TemporalProperties, TemporalPropertyView,
        },
        view::{internal::Static, DynamicGraph},
    },
    search::IndexedGraph,
};
use std::sync::Arc;

pub type DynProps = Arc<dyn PropertiesOps + Send + Sync>;
impl InheritPropertiesOps for DynProps {}
pub type DynProperties = Properties<Arc<dyn PropertiesOps + Send + Sync>>;

impl<P: PropertiesOps + Clone + Send + Sync + Static + 'static> From<Properties<P>>
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

impl From<Properties<IndexedGraph<DynamicGraph>>> for DynProperties {
    fn from(value: Properties<IndexedGraph<DynamicGraph>>) -> Self {
        let props: DynProps = Arc::new(value.props);
        Properties::new(props)
    }
}
pub type DynConstProperties = ConstProperties<DynProps>;

impl<P: PropertiesOps + Send + Sync + Static + 'static> From<ConstProperties<P>>
    for DynConstProperties
{
    fn from(value: ConstProperties<P>) -> Self {
        ConstProperties {
            props: Arc::new(value.props),
        }
    }
}

pub type DynTemporalProperties = TemporalProperties<DynProps>;
pub type DynTemporalProperty = TemporalPropertyView<DynProps>;

impl<P: PropertiesOps + Clone + Send + Sync + Static + 'static> From<TemporalProperties<P>>
    for DynTemporalProperties
{
    fn from(value: TemporalProperties<P>) -> Self {
        TemporalProperties::new(Arc::new(value.props))
    }
}
impl From<TemporalProperties<DynamicGraph>> for DynTemporalProperties {
    fn from(value: TemporalProperties<DynamicGraph>) -> Self {
        let props: Arc<dyn PropertiesOps + Send + Sync> = Arc::new(value.props);
        TemporalProperties::new(props)
    }
}
