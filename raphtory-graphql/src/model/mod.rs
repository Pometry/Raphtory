use crate::{
    data::Data,
    model::graph::graph::{GqlGraph, GraphMeta},
};
use async_graphql::Context;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use dynamic_graphql::{
    App, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields, Result,
    Upload,
};
use itertools::Itertools;
use raphtory::{
    core::Prop,
    db::api::view::internal::{CoreGraphOps, IntoDynamic, MaterializedGraph},
    prelude::{Graph, GraphViewOps, PropertyAdditionOps},
    search::IndexedGraph,
};
use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    io::BufReader,
    ops::Deref,
};
use uuid::Uuid;

pub(crate) mod algorithm;
pub(crate) mod filters;
pub(crate) mod graph;
pub(crate) mod schema;

#[derive(Debug)]
pub struct MissingGraph;

impl Display for MissingGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Graph does not exist")
    }
}

impl Error for MissingGraph {}

#[derive(ResolvedObject)]
#[graphql(root)]
pub(crate) struct QueryRoot;

#[ResolvedObjectFields]
impl QueryRoot {
    async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Returns a graph
    async fn graph<'a>(ctx: &Context<'a>, name: &str) -> Option<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.graphs.read().get(name).cloned()?;
        Some(GqlGraph::new(g.into_dynamic_indexed()))
    }

    async fn subgraph<'a>(ctx: &Context<'a>, name: &str) -> Option<GraphMeta> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.graphs.read().get(name).cloned()?;
        Some(GraphMeta::new(
            name.to_string(),
            g.deref().clone().into_dynamic(),
        ))
    }

    async fn subgraphs<'a>(ctx: &Context<'a>) -> Vec<GraphMeta> {
        let data = ctx.data_unchecked::<Data>();
        data.graphs
            .read()
            .iter()
            .map(|(name, g)| GraphMeta::new(name.clone(), g.deref().clone().into_dynamic()))
            .collect_vec()
    }

    async fn receive_graph<'a>(ctx: &Context<'a>, name: &str) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let g = data
            .graphs
            .read()
            .get(name)
            .cloned()
            .ok_or(MissingGraph)?
            .materialize()?;
        let bincode = bincode::serialize(&g)?;
        Ok(URL_SAFE_NO_PAD.encode(bincode))
    }

    async fn similarity_search<'a>(
        ctx: &Context<'a>,
        graph: &str,
        query: &str,
        init: Option<usize>,
        min_nodes: Option<usize>,
        min_edges: Option<usize>,
        limit: Option<usize>,
        window_start: Option<i64>,
        window_end: Option<i64>,
    ) -> Option<Vec<String>> {
        let init = init.unwrap_or(1);
        let min_nodes = min_nodes.unwrap_or(0);
        let min_edges = min_edges.unwrap_or(0);
        let limit = limit.unwrap_or(1);
        let data = ctx.data_unchecked::<Data>();
        let binding = data.vector_stores.read();
        let vec_store = binding.get(graph)?;
        println!("running similarity search for {query}");
        Some(
            vec_store
                .similarity_search(
                    query,
                    init,
                    min_nodes,
                    min_edges,
                    limit,
                    window_start,
                    window_end,
                )
                .await,
        )
    }
}

#[derive(MutationRoot)]
pub(crate) struct MutRoot;

#[derive(Mutation)]
pub(crate) struct Mut(MutRoot);

#[MutationFields]
impl Mut {
    /// Load graphs from a directory of bincode files (existing graphs with the same name are overwritten)
    ///
    /// # Returns:
    ///   list of names for newly added graphs
    async fn load_graphs_from_path<'a>(ctx: &Context<'a>, path: String) -> Vec<String> {
        let new_graphs = Data::load_from_file(&path);
        let keys: Vec<_> = new_graphs.keys().cloned().collect();
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        data.extend(new_graphs);
        keys
    }

    async fn save_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_name: String,
        graph_name: String,
        new_graph_name: String,
        props: String,
        graph_nodes: Vec<String>,
    ) -> Result<bool> {
        let mut data = ctx.data_unchecked::<Data>().graphs.write();

        let subgraph = data.get(&graph_name).ok_or("Graph not found")?;
        let mut path = subgraph
            .constant_prop(&"path".to_string())
            .expect("Path is missing")
            .to_string();

        if new_graph_name.ne(&graph_name) {
            fn path_prefix(path: String) -> Result<String> {
                let elements: Vec<&str> = path.split('/').collect();
                let size = elements.len();
                return if size > 2 {
                    let delimiter = "/";
                    let joined_string = elements
                        .iter()
                        .take(size - 1)
                        .map(|s| *s)
                        .collect::<Vec<_>>()
                        .join(delimiter);
                    Ok(joined_string)
                } else {
                    Err("Invalid graph path".into())
                };
            }

            path = path_prefix(path)? + "/" + &Uuid::new_v4().hyphenated().to_string();
        }

        let parent_graph = data.get(&parent_graph_name).ok_or("Graph not found")?;
        let new_subgraph = parent_graph
            .subgraph(graph_nodes)
            .materialize()
            .expect("Failed to materialize graph");
        let static_props_without_name = subgraph
            .properties()
            .into_iter()
            .filter(|(a, _)| a != "name");
        new_subgraph
            .add_constant_properties(static_props_without_name)
            .expect("Failed to add static properties");
        new_subgraph
            .add_constant_properties([("name".to_string(), Prop::str(new_graph_name.clone()))])
            .expect("Failed to add static property");
        new_subgraph
            .add_constant_properties([("uiProps".to_string(), Prop::str(props))])
            .expect("Failed to add static property");

        new_subgraph
            .save_to_file(path)
            .expect("Failed to save graph");

        let gi: IndexedGraph<Graph> = new_subgraph
            .into_events()
            .ok_or("Graph with deletions not supported")?
            .into();

        data.insert(new_graph_name, gi.clone());

        Ok(true)
    }

    /// Load new graphs from a directory of bincode files (existing graphs will not been overwritten)
    ///
    /// # Returns:
    ///   list of names for newly added graphs
    async fn load_new_graphs_from_path<'a>(ctx: &Context<'a>, path: String) -> Vec<String> {
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        let new_graphs: HashMap<_, _> = Data::load_from_file(&path)
            .into_iter()
            .filter(|(key, _)| !data.contains_key(key))
            .collect();
        let keys: Vec<_> = new_graphs.keys().cloned().collect();
        data.extend(new_graphs);
        keys
    }

    /// Use GQL multipart upload to send new graphs to server
    ///
    /// # Returns:
    ///    name of the new graph
    async fn upload_graph<'a>(ctx: &Context<'a>, name: String, graph: Upload) -> Result<String> {
        let g: MaterializedGraph =
            bincode::deserialize_from(BufReader::new(graph.value(ctx)?.content))?;
        let gi: IndexedGraph<Graph> = g
            .into_events()
            .ok_or("Graph with deletions not supported")?
            .into();
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        data.insert(name.clone(), gi.clone());
        Ok(name)
    }

    /// Send graph bincode as base64 encoded string
    ///
    /// # Returns:
    ///    name of the new graph
    async fn send_graph<'a>(ctx: &Context<'a>, name: String, graph: String) -> Result<String> {
        let g: MaterializedGraph = bincode::deserialize(&URL_SAFE_NO_PAD.decode(graph)?)?;
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        data.insert(
            name.clone(),
            g.into_events()
                .ok_or("Graph with deletions not supported")?
                .into(),
        );
        Ok(name)
    }
}

#[derive(App)]
pub(crate) struct App(QueryRoot, MutRoot, Mut);
