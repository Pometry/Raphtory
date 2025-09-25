use dynamic_graphql::{Enum, InputObject, OneOfInput, SimpleObject};
use raphtory::{
    db::api::view::{BoxableGraphView, IndexSpec, IndexSpecBuilder},
    errors::GraphError,
};

#[derive(Enum)]
pub enum AllPropertySpec {
    /// All properties and metadata.
    All,
    /// All metadata.
    AllMetadata,
    /// All properties.
    AllProperties,
}

#[derive(InputObject)]
pub struct SomePropertySpec {
    /// List of metadata.
    pub metadata: Vec<String>,
    /// List of properties.
    pub properties: Vec<String>,
}

#[allow(dead_code)]
#[derive(OneOfInput)]
pub enum PropsInput {
    /// All properties and metadata.
    All(AllPropertySpec),
    /// Some properties and metadata.
    Some(SomePropertySpec),
}

#[derive(InputObject)]
pub struct IndexSpecInput {
    /// Node properties.
    pub node_props: PropsInput,
    /// Edge properties.
    pub edge_props: PropsInput,
}

impl IndexSpecInput {
    pub(crate) fn to_index_spec<G>(self, graph: G) -> Result<IndexSpec, GraphError>
    where
        G: BoxableGraphView + Sized + Clone + 'static,
    {
        let mut builder = IndexSpecBuilder::new(graph);

        builder = match self.node_props {
            PropsInput::All(spec) => match spec {
                AllPropertySpec::All => builder.with_all_node_properties_and_metadata(),
                AllPropertySpec::AllMetadata => builder.with_all_node_metadata(),
                AllPropertySpec::AllProperties => builder.with_all_node_properties(),
            },
            PropsInput::Some(props) => builder
                .with_node_metadata(props.metadata)?
                .with_node_properties(props.properties)?,
        };

        builder = match self.edge_props {
            PropsInput::All(spec) => match spec {
                AllPropertySpec::All => builder.with_all_edge_properties_and_metadata(),
                AllPropertySpec::AllMetadata => builder.with_all_edge_metadata(),
                AllPropertySpec::AllProperties => builder.with_all_edge_properties(),
            },
            PropsInput::Some(props) => builder
                .with_edge_metadata(props.metadata)?
                .with_edge_properties(props.properties)?,
        };

        Ok(builder.build())
    }
}

#[derive(SimpleObject)]
#[graphql(name = "IndexSpec")]
pub struct GqlIndexSpec {
    /// Returns node metadata.
    pub node_metadata: Vec<String>,
    /// Returns node properties.
    pub node_properties: Vec<String>,
    /// Returns edge metadata.
    pub edge_metadata: Vec<String>,
    /// Returns edge properties.
    pub edge_properties: Vec<String>,
}
