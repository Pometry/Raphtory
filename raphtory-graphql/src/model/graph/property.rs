use std::sync::Arc;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::core::Prop;
use raphtory::db::api::properties::internal::PropertiesOps;
use raphtory::db::api::properties::Properties;

#[derive(ResolvedObject)]
pub(crate) struct GqlProp {
    prop:Prop
}
impl GqlProp {
    pub(crate) fn new(prop:Prop) -> Self {Self{prop}}
}
impl From<Prop> for GqlProp {
    fn from(value: Prop) -> Self {
        GqlProp::new(value)
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlProperties{
    props:Properties
}
impl GqlProperties {
    pub(crate) fn new(props: Properties<DynProperties>) -> Self {
        Self { props }
    }
}
impl From<Properties<DynProperties>> for GqlProperties {
    fn from(value: Properties<DynProperties>) -> Self {
        GqlProperties::new(value)
    }
}

#[ResolvedObjectFields]
impl GqlProperties {
    async fn get(&self,key:&str) -> Option<GqlProp> {
        self.props.get(key).map_into()
    }
}



