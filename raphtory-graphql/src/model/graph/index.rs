use dynamic_graphql::{Enum, InputObject, OneOfInput, SimpleObject};
use raphtory::{
    db::api::view::{BoxableGraphView, IndexSpec, IndexSpecBuilder},
    errors::GraphError,
};

#[derive(Enum)]
pub enum PropertySpec {
    All,
    AllConstant,
    AllTemporal,
}

#[allow(dead_code)]
#[derive(OneOfInput)]
pub enum PropsInput {
    All(PropertySpec),
    Const(Vec<String>),
    Temp(Vec<String>),
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
                PropertySpec::All => builder.with_all_node_props(),
                PropertySpec::AllConstant => builder.with_all_const_node_props(),
                PropertySpec::AllTemporal => builder.with_all_temp_node_props(),
            },
            PropsInput::Const(props) => builder.with_const_node_props(props)?,
            PropsInput::Temp(props) => builder.with_temp_node_props(props)?,
        };

        builder = match self.edge_props {
            PropsInput::All(spec) => match spec {
                PropertySpec::All => builder.with_all_edge_props(),
                PropertySpec::AllConstant => builder.with_all_edge_const_props(),
                PropertySpec::AllTemporal => builder.with_all_temp_edge_props(),
            },
            PropsInput::Const(props) => builder.with_const_edge_props(props)?,
            PropsInput::Temp(props) => builder.with_temp_edge_props(props)?,
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
