use dynamic_graphql::{InputObject, OneOfInput, SimpleObject};
use raphtory::{
    db::api::view::{BoxableGraphView, IndexSpecBuilder},
    errors::GraphError,
};

#[allow(dead_code)]
#[derive(OneOfInput)]
pub enum NodePropsInput {
    All(bool),
    AllConst(bool),
    AllTemp(bool),
    Const(Vec<String>),
    Temp(Vec<String>),
}

#[allow(dead_code)]
#[derive(OneOfInput)]
pub enum EdgePropsInput {
    All(bool),
    AllConst(bool),
    AllTemp(bool),
    Const(Vec<String>),
    Temp(Vec<String>),
}

#[derive(InputObject)]
pub struct IndexSpecInput {
    pub node_props: Option<NodePropsInput>,
    pub edge_props: Option<EdgePropsInput>,
}

impl IndexSpecInput {
    pub fn to_builder<G>(self, graph: G) -> Result<IndexSpecBuilder<G>, GraphError>
    where
        G: BoxableGraphView + Sized + Clone + 'static,
    {
        let mut builder = IndexSpecBuilder::new(graph);

        if let Some(node) = self.node_props {
            builder = match node {
                NodePropsInput::All(true) => builder.with_all_node_props(),
                NodePropsInput::AllConst(true) => builder.with_all_const_node_props(),
                NodePropsInput::AllTemp(true) => builder.with_all_temp_node_props(),
                NodePropsInput::Const(props) => builder.with_const_node_props(props)?,
                NodePropsInput::Temp(props) => builder.with_temp_node_props(props)?,
                _ => builder,
            };
        }

        if let Some(edge) = self.edge_props {
            builder = match edge {
                EdgePropsInput::All(true) => builder.with_all_edge_props(),
                EdgePropsInput::AllConst(true) => builder.with_all_edge_const_props(),
                EdgePropsInput::AllTemp(true) => builder.with_all_temp_edge_props(),
                EdgePropsInput::Const(props) => builder.with_const_edge_props(props)?,
                EdgePropsInput::Temp(props) => builder.with_temp_edge_props(props)?,
                _ => builder,
            };
        }

        Ok(builder)
    }
}

#[derive(SimpleObject)]
pub struct IndexSpec {
    pub node_const_props: Vec<String>,
    pub node_temp_props: Vec<String>,
    pub edge_const_props: Vec<String>,
    pub edge_temp_props: Vec<String>,
}
