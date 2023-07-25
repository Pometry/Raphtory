use crate::model::{
    filters::edgefilter::EdgeFilter,
    graph::{edge::Edge, property::Property, property_update::PropertyUpdate},
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::Prop,
    db::{
        api::view::{
            internal::{DynamicGraph, IntoDynamic},
            *,
        },
        graph::vertex::VertexView,
    },
};

#[derive(ResolvedObject)]
pub(crate) struct Node {
    pub(crate) vv: VertexView<DynamicGraph>,
}

impl<G: GraphViewOps + IntoDynamic> From<VertexView<G>> for Node {
    fn from(value: VertexView<G>) -> Self {
        Self {
            vv: VertexView {
                graph: value.graph.clone().into_dynamic(),
                vertex: value.vertex,
            },
        }
    }
}

#[ResolvedObjectFields]
impl Node {
    async fn id(&self) -> u64 {
        self.vv.id()
    }

    pub async fn name(&self) -> String {
        self.vv.name()
    }

    pub async fn node_type(&self) -> String {
        self.vv
            .property("type".to_string(), true)
            .unwrap_or(Prop::Str("NONE".to_string()))
            .to_string()
    }

    async fn property_names<'a>(&self, _ctx: &Context<'a>) -> Vec<String> {
        self.vv.property_names(true)
    }

    async fn properties(&self) -> Option<Vec<Property>> {
        Some(
            self.vv
                .properties(true)
                .into_iter()
                .map(|(k, v)| Property::new(k, v))
                .collect_vec(),
        )
    }

    async fn property(&self, name: &str) -> Option<String> {
        Some(self.vv.property(name.to_string(), true)?.to_string())
    }

    async fn property_history(&self, name: String) -> Vec<PropertyUpdate> {
        self.vv
            .property_history(name)
            .into_iter()
            .map(|(time, prop)| PropertyUpdate::new(time, prop.to_string()))
            .collect_vec()
    }

    async fn in_neighbours<'a>(&self, layer: Option<String>) -> Vec<Node> {
        match layer.as_deref() {
            None => self.vv.in_neighbours().iter().map(|vv| vv.into()).collect(),
            Some(layer) => match self.vv.layer(layer) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.in_neighbours().iter().map(|vv| vv.into()).collect(),
            },
        }
    }

    async fn out_neighbours(&self, layer: Option<String>) -> Vec<Node> {
        match layer.as_deref() {
            None => self
                .vv
                .out_neighbours()
                .iter()
                .map(|vv| vv.into())
                .collect(),
            Some(layer) => match self.vv.layer(layer) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.out_neighbours().iter().map(|vv| vv.into()).collect(),
            },
        }
    }

    async fn neighbours<'a>(&self, layer: Option<String>) -> Vec<Node> {
        match layer.as_deref() {
            None => self.vv.neighbours().iter().map(|vv| vv.into()).collect(),
            Some(layer) => match self.vv.layer(layer) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.neighbours().iter().map(|vv| vv.into()).collect(),
            },
        }
    }

    async fn degree(&self, layers: Option<Vec<String>>) -> usize {
        match layers {
            None => self.vv.degree(),
            Some(layers) => layers
                .iter()
                .map(|layer| {
                    let degree = match self.vv.layer(layer) {
                        None => 0,
                        Some(vvv) => vvv.degree(),
                    };
                    return degree;
                })
                .sum(),
        }
    }

    async fn out_degree(&self, layer: Option<String>) -> usize {
        match layer.as_deref() {
            None => self.vv.out_degree(),
            Some(layer) => match self.vv.layer(layer) {
                None => 0,
                Some(vvv) => vvv.out_degree(),
            },
        }
    }

    async fn in_degree(&self, layer: Option<String>) -> usize {
        match layer.as_deref() {
            None => self.vv.in_degree(),
            Some(layer) => match self.vv.layer(layer) {
                None => 0,
                Some(vvv) => vvv.in_degree(),
            },
        }
    }

    async fn out_edges(&self, layer: Option<String>) -> Vec<Edge> {
        match layer.as_deref() {
            None => self.vv.out_edges().map(|ee| ee.clone().into()).collect(),
            Some(layer) => match self.vv.layer(layer) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.out_edges().map(|ee| ee.clone().into()).collect(),
            },
        }
    }

    async fn in_edges(&self, layer: Option<String>) -> Vec<Edge> {
        match layer.as_deref() {
            None => self.vv.in_edges().map(|ee| ee.clone().into()).collect(),
            Some(layer) => match self.vv.layer(layer) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.in_edges().map(|ee| ee.clone().into()).collect(),
            },
        }
    }

    async fn edges(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .vv
                .edges()
                .into_iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.vv.edges().map(|ee| ee.clone().into()).collect(),
        }
    }

    async fn exploded_edges(&self) -> Vec<Edge> {
        self.vv.out_edges().explode().map(|ee| ee.into()).collect()
    }

    async fn start_date(&self) -> Option<i64> {
        self.vv.earliest_time()
    }

    async fn end_date(&self) -> Option<i64> {
        self.vv.latest_time()
    }
}
