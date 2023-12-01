use crate::model::{
    filters::edge_filter::EdgeFilter,
    graph::{edge::Edge, get_expanded_edges, property::Property, property_update::PropertyUpdate},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::db::{api::view::*, graph::vertex::VertexView};
use std::collections::HashSet;

use super::property_update::PropertyUpdateGroup;

#[derive(ResolvedObject)]
pub(crate) struct Node {
    pub(crate) vv: VertexView<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<VertexView<G, GH>> for Node
{
    fn from(value: VertexView<G, GH>) -> Self {
        Self {
            vv: VertexView {
                base_graph: value.base_graph.into_dynamic(),
                graph: value.graph.into_dynamic(),
                vertex: value.vertex,
            },
        }
    }
}

#[ResolvedObjectFields]
impl Node {
    async fn id(&self) -> String {
        self.vv.id().to_string()
    }

    pub async fn name(&self) -> String {
        self.vv.name()
    }


    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn earliest_time(&self) -> Option<i64> {
        self.vv.earliest_time()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.vv.latest_time()
    }

    async fn start(&self) -> Option<i64> {
        self.vv.start()
    }

    async fn end(&self) -> Option<i64> {
        self.vv.end()
    }

    async fn history(&self) -> Vec<i64> { self.vv.history()}
    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////
    pub async fn node_type(&self) -> String {
        self.vv
            .properties()
            .get("type")
            .map(|p| p.to_string())
            .unwrap_or("NONE".to_string())
    }

    /// Returns all the property names this node has a value for
    async fn property_names(&self) -> Vec<String> {
        self.vv.properties().keys().map_into().collect()
    }

    /// Returns all the properties of the node
    async fn properties(&self) -> Vec<Property> {
            self.vv
                .properties()
                .iter()
                .map(|(k, v)| Property::new(k.into(), v))
                .collect(),
    }

    /// Returns the value for the property with name `name`
    async fn property(&self, name: &str) -> Option<String> {
        self.vv.properties().get(name).map(|v| v.to_string())
    }

    /// Returns the history as a vector of updates for the property with name `name`
    async fn property_history(&self, name: String) -> Vec<PropertyUpdate> {
        self.vv
            .properties()
            .temporal()
            .get(&name)
            .into_iter()
            .flat_map(|p| {
                p.iter()
                    .map(|(time, prop)| PropertyUpdate::new(time, prop.to_string()))
            })
            .collect()
    }

    /// Returns the history as a vectory of updates for any properties which are included in param names
    async fn properties_history(&self, names: Vec<String>) -> Vec<PropertyUpdateGroup> {
        names
            .iter()
            .filter_map(|name| match self.vv.properties().temporal().get(name) {
                Some(prop) => Option::Some(PropertyUpdateGroup::new(
                    name.to_string(),
                    prop.iter()
                        .map(|(time, prop)| PropertyUpdate::new(time, prop.to_string()))
                        .collect_vec(),
                )),
                None => None,
            })
            .collect_vec()
    }

    ////////////////////////
    //// EDGE GETTERS //////
    ////////////////////////
    /// Returns the number of edges connected to this node
    async fn degree(&self) -> usize {
        self.vv.degree()
    }

    /// Returns the number edges with this node as the source
    async fn out_degree(&self) -> usize {
        self.vv.out_degree()
    }

    /// Returns the number edges with this node as the destination
    async fn in_degree(&self) -> usize {
        self.vv.in_degree()
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
    async fn out_edges(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .vv
                .out_edges()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.vv.edges().map(|ee| ee.into()).collect(),
        }
    }

    async fn in_edges(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .vv
                .edges()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.vv.in_edges().map(|ee| ee.into()).collect(),
        }
    }

    async fn neighbours<'a>(&self) -> Vec<Node> {
        self.vv.neighbours().iter().map(|vv| vv.into()).collect()
    }

    async fn in_neighbours<'a>(&self) -> Vec<Node> {
        self.vv.in_neighbours().iter().map(|vv| vv.into()).collect()
    }

    async fn out_neighbours(&self) -> Vec<Node> {
        self.vv.out_neighbours().iter().map(|vv| vv.into()).collect()
    }

    ////////////////////////
    // GRAPHQL SPECIFIC ////
    ////////////////////////

    async fn expanded_edges(
        &self,
        graph_nodes: Vec<String>,
        filter: Option<EdgeFilter>,
    ) -> Vec<Edge> {
        let all_graph_nodes: HashSet<String> = graph_nodes.into_iter().collect();

        match filter {
            Some(edge_filter) => {
                let maybe_layers = edge_filter.clone().layer_names.map(|l| l.contains);
                let fetched_edges =
                    get_expanded_edges(all_graph_nodes, self.vv.clone(), maybe_layers)
                        .iter()
                        .map(|ee| ee.clone().into())
                        .collect_vec();
                fetched_edges
                    .into_iter()
                    .filter(|ev| edge_filter.matches(ev))
                    .collect()
            }
            None => get_expanded_edges(all_graph_nodes, self.vv.clone(), None)
                .iter()
                .map(|ee| ee.clone().into())
                .collect_vec(),
        }
    }


}
