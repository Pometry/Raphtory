use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::core::Prop;

#[derive(ResolvedObject)]
pub(crate) struct Property {
    key: String,
    value: Prop,
}

impl Property {
    pub(crate) fn new(key: String, value: Prop) -> Self {
        Self { key, value }
    }
}

#[ResolvedObjectFields]
impl Property {
    async fn key(&self, _ctx: &Context<'_>) -> String {
        self.key.to_string()
    }

    async fn value(&self, _ctx: &Context<'_>) -> String {
        self.value.to_string()
    }
}
