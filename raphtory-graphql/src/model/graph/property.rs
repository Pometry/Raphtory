use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::core::Prop;
use raphtory::db::api::properties::internal::PropertiesOps;
use raphtory::db::api::properties::Properties;

#[derive(ResolvedObject)]
pub(crate) struct GqlProperties<P: PropertiesOps + Clone> {
    props:Properties<P>
}

impl GqlProperties<P: PropertiesOps + Clone> {
    pub(crate) fn new(props:P) -> Self {
        Self { props }
    }
}

#[ResolvedObjectFields]
impl GqlProperties {
    async fn key(&self) -> String {
        self.key.to_string()
    }

    async fn value(&self) -> String {
        self.value.to_string()
    }
}
