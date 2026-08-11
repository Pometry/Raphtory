use dynamic_graphql::{ExpandObject, ExpandObjectFields};
use raphtory_graphql::model::QueryRoot;

#[derive(ExpandObject)]
pub(crate) struct HelloQuery<'a>(&'a QueryRoot);

#[ExpandObjectFields]
impl<'a> HelloQuery<'a> {
    async fn hello_query(name: String) -> String {
        "Hello, ".to_owned() + name.as_str()
    }
}
