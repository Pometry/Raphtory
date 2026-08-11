use dynamic_graphql::{Mutation, MutationFields};
use raphtory_graphql::{model::MutRoot, plugin::schema::RegisterPlugin};

#[derive(Mutation)]
pub(crate) struct HelloMutation(MutRoot);

#[MutationFields]
impl HelloMutation {
    async fn hello_mutation(name: String) -> String {
        "Hello, ".to_owned() + name.as_str()
    }
}
