use dynamic_graphql::{Enum, InputObject, OneOfInput, SimpleObject};
use raphtory::{
    db::api::view::{BoxableGraphView, IndexSpec, IndexSpecBuilder},
    errors::GraphError,
};

#[derive(Enum)]
pub enum AllPropertySpec {
    All,
    AllConstant,
    AllTemporal,
}

#[derive(InputObject)]
pub struct SomePropertySpec {
    pub constant: Vec<String>,
    pub temporal: Vec<String>,
}

#[allow(dead_code)]
#[derive(OneOfInput)]
pub enum PropsInput {
    All(AllPropertySpec),
    Some(SomePropertySpec),
}

#[derive(InputObject)]
pub struct IndexSpecInput {
    pub node_props: PropsInput,
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
                AllPropertySpec::All => builder.with_all_node_props(),
                AllPropertySpec::AllConstant => builder.with_all_const_node_props(),
                AllPropertySpec::AllTemporal => builder.with_all_temp_node_props(),
            },
            PropsInput::Some(props) => builder
                .with_const_node_props(props.constant)?
                .with_temp_node_props(props.temporal)?,
        };

        builder = match self.edge_props {
            PropsInput::All(spec) => match spec {
                AllPropertySpec::All => builder.with_all_edge_props(),
                AllPropertySpec::AllConstant => builder.with_all_edge_const_props(),
                AllPropertySpec::AllTemporal => builder.with_all_temp_edge_props(),
            },
            PropsInput::Some(props) => builder
                .with_const_edge_props(props.constant)?
                .with_temp_edge_props(props.temporal)?,
        };

        Ok(builder.build())
    }
}

#[derive(SimpleObject)]
pub struct GqlIndexSpec {
    pub node_const_props: Vec<String>,
    pub node_temp_props: Vec<String>,
    pub edge_const_props: Vec<String>,
    pub edge_temp_props: Vec<String>,
}
