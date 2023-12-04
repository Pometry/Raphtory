use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::core::Prop;
use raphtory::db::api::properties::TemporalPropertyView;
use raphtory::python::graph::properties::{DynConstProperties, DynProperties, DynTemporalProperties};

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

// impl From<TemporalPropertyView<Arc<dyn PropertiesOps + std::marker::Send + Sync>>> for GqlProp {
//
// }

#[ResolvedObjectFields]
impl GqlProp {
    async fn as_string(&self) -> String {
        self.prop.to_string()
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlProperties{
    props:DynProperties
}
impl GqlProperties {
    pub(crate) fn new(props: DynProperties) -> Self {
        Self { props }
    }
}
impl From<DynProperties> for GqlProperties {
    fn from(value: DynProperties) -> Self {
        GqlProperties::new(value)
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlTemporalProperties{
    props:DynTemporalProperties
}
impl GqlTemporalProperties {
    pub(crate) fn new(props: DynTemporalProperties) -> Self {
        Self { props }
    }
}
impl From<DynTemporalProperties> for GqlTemporalProperties {
    fn from(value: DynTemporalProperties) -> Self {
        GqlTemporalProperties::new(value)
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlConstantProperties{
    props:DynConstProperties
}
impl GqlConstantProperties {
    pub(crate) fn new(props: DynConstProperties) -> Self {
        Self { props }
    }
}
impl From<DynConstProperties> for GqlConstantProperties {
    fn from(value: DynConstProperties) -> Self {
        GqlConstantProperties::new(value)
    }
}

#[ResolvedObjectFields]
impl GqlProperties {
    async fn get(&self,key:&str) -> Option<GqlProp> {
        self.props.get(key).map(|p|p.into())
    }
    async fn contains(&self,key:&str) -> bool {
        self.props.contains(key)
    }
    async fn keys(&self) -> Vec<String> {
        self.props.keys().map(|k| k.into()).collect()
    }

    async fn values(&self) -> Vec<GqlProp> {
        self.props.values().map(|x|x.into()).collect()
    }

    async fn items(&self) -> Vec<(String, GqlProp)> {
        self.props.iter().map(|(k,v)|(k.into(),v.into())).collect()
    }

    async fn temporal(&self) -> GqlTemporalProperties {
        self.props.temporal().into()
    }

    pub fn constant(&self) -> GqlConstantProperties {
        self.props.constant().into()
    }
}

#[ResolvedObjectFields]
impl GqlConstantProperties {
    async fn get(&self,key:&str) -> Option<GqlProp> {
        self.props.get(key).map(|p|p.into())
    }
    async fn contains(&self,key:&str) -> bool {
        self.props.contains(key)
    }
    async fn keys(&self) -> Vec<String> {
        self.props.keys().iter().map(|k| k.clone().into()).collect()
    }

    async fn values(&self) -> Vec<GqlProp> {
        self.props.values().iter().map(|x|x.clone().into()).collect()
    }

    async fn items(&self) -> Vec<(String, GqlProp)> {
        self.props.iter().map(|(k,v)|(k.into(),v.into())).collect()
    }
}

#[ResolvedObjectFields]
impl GqlTemporalProperties {
    // async fn get(&self,key:&str) -> Option<GqlProp> {
    //     self.props.get(key).map(|p|p.into())
    // }
    // async fn contains(&self,key:&str) -> bool {
    //     self.props.contains(key)
    // }
    // async fn keys(&self) -> Vec<String> {
    //     self.props.keys().map(|k| k.into()).collect()
    // }
    //
    // async fn values(&self) -> Vec<GqlProp> {
    //     self.props.values().map(|x|x.into()).collect()
    // }
    //
    // async fn items(&self) -> Vec<(String, GqlProp)> {
    //     self.props.iter().map(|(k,v)|(k.into(),v.into())).collect()
    // }
}
