use crate::model::{
    filters::edgefilter::EdgeFilter,
    graph::{edge::Edge, property::Property, property_update::PropertyUpdate},
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::db::{
    api::view::{
        internal::{DynamicGraph, IntoDynamic},
        *,
    },
    graph::vertex::VertexView,
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
        if let Some(t) = self.vv.properties().get("type") {
            t.value().unwrap().to_string()
        } else if let Some(s) = self.vv.static_properties().get("type") {
            s.to_string()
        } else {
            "NONE".to_string()
        }
    }

    async fn property_names<'a>(&self, _ctx: &Context<'a>) -> Vec<String> {
        let t_props = self.vv.properties();
        t_props
            .keys()
            .into_iter()
            .chain(
                self.vv
                    .static_properties()
                    .keys()
                    .into_iter()
                    .filter(|k| t_props.get(k).is_none()),
            )
            .collect()
    }

    async fn properties(&self) -> Option<Vec<Property>> {
        let t_props = self.vv.properties();
        Some(
            t_props
                .iter()
                .map(|(k, v)| Property::new(k, v.value().unwrap()))
                .chain(
                    self.vv.static_properties().iter().filter_map(|(k, v)| {
                        t_props.get(&k).is_none().then_some(Property::new(k, v))
                    }),
                )
                .collect(),
        )
    }

    async fn property(&self, name: String) -> Option<Property> {
        if let Some(p) = self.vv.properties().get(&name) {
            p.value().map(|v| Property::new(name, v))
        } else {
            self.vv
                .static_properties()
                .get(&name)
                .map(|p| Property::new(name, p))
        }
    }

    async fn property_history(&self, name: String) -> Vec<PropertyUpdate> {
        self.vv
            .properties()
            .get(name)
            .into_iter()
            .flat_map(|p| {
                p.iter()
                    .map(|(time, prop)| PropertyUpdate::new(time, prop.to_string()))
            })
            .collect_vec()
    }

    async fn in_neighbours<'a>(&self, layer: Option<String>) -> Vec<Node> {
        match layer {
            None => self.vv.in_neighbours().iter().map(|vv| vv.into()).collect(),
            Some(layer) => match self.vv.layer(layer.as_str()) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.in_neighbours().iter().map(|vv| vv.into()).collect(),
            },
        }
    }

    async fn out_neighbours(&self, layer: Option<String>) -> Vec<Node> {
        match layer {
            None => self
                .vv
                .out_neighbours()
                .iter()
                .map(|vv| vv.into())
                .collect(),
            Some(layer) => match self.vv.layer(layer.as_str()) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.out_neighbours().iter().map(|vv| vv.into()).collect(),
            },
        }
    }

    async fn neighbours<'a>(&self, layer: Option<String>) -> Vec<Node> {
        match layer {
            None => self.vv.neighbours().iter().map(|vv| vv.into()).collect(),
            Some(layer) => match self.vv.layer(layer.as_str()) {
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
                .into_iter()
                .map(|layer| {
                    let degree = match self.vv.layer(layer.as_str()) {
                        None => 0,
                        Some(vvv) => vvv.degree(),
                    };
                    degree
                })
                .sum(),
        }
    }

    async fn out_degree(&self, layer: Option<String>) -> usize {
        match layer {
            None => self.vv.out_degree(),
            Some(layer) => match self.vv.layer(layer.as_str()) {
                None => 0,
                Some(vvv) => vvv.out_degree(),
            },
        }
    }

    async fn in_degree(&self, layer: Option<String>) -> usize {
        match layer {
            None => self.vv.in_degree(),
            Some(layer) => match self.vv.layer(layer.as_str()) {
                None => 0,
                Some(vvv) => vvv.in_degree(),
            },
        }
    }

    async fn out_edges(&self, layer: Option<String>) -> Vec<Edge> {
        match layer {
            None => self.vv.out_edges().map(|ee| ee.into()).collect(),
            Some(layer) => match self.vv.layer(layer.as_str()) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.out_edges().map(|ee| ee.into()).collect(),
            },
        }
    }

    async fn in_edges(&self, layer: Option<String>) -> Vec<Edge> {
        match layer {
            None => self.vv.in_edges().map(|ee| ee.into()).collect(),
            Some(layer) => match self.vv.layer(layer.as_str()) {
                None => {
                    vec![]
                }
                Some(vvv) => vvv.in_edges().map(|ee| ee.into()).collect(),
            },
        }
    }

    async fn edges(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .vv
                .edges()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.vv.edges().map(|ee| ee.into()).collect(),
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
