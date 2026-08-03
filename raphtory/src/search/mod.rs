use crate::{
    db::{
        api::view::{
            filter_ops::{Filter, NodeSelect},
            StaticGraphViewOps,
        },
        graph::{edge::EdgeView, node::NodeView, views::filter::CreateFilter},
    },
    errors::GraphError,
    prelude::{EdgeViewOps, GraphViewOps},
    search::property_index::PropertyIndex,
};
use ahash::HashSet;
use parking_lot::RwLockReadGuard;
use raphtory_api::core::entities::properties::{
    meta::PropMapper,
    prop::{Prop, PropType},
};
use std::{fs::create_dir_all, path::PathBuf, sync::Arc};
use tantivy::{
    schema::Schema,
    tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer},
    Index, IndexReader, IndexSettings,
};

pub mod graph_index;
pub mod searcher;

mod collectors;
mod edge_filter_executor;
pub mod edge_index;
pub mod entity_index;
mod exploded_edge_filter_executor;
mod node_filter_executor;
pub mod node_index;
pub mod property_index;
mod query_builder;

pub(in crate::search) mod fields {
    pub const TIME: &str = "time";
    pub const EVENT_ID: &str = "event_id";
    pub const NODE_ID: &str = "node_id";
    pub const NODE_NAME: &str = "node_name";
    pub const NODE_NAME_TOKENIZED: &str = "node_name_tokenized";
    pub const NODE_TYPE: &str = "node_type";
    pub const NODE_TYPE_TOKENIZED: &str = "node_type_tokenized";
    pub const EDGE_ID: &str = "edge_id";
    pub const SOURCE: &str = "src";
    pub const SOURCE_TOKENIZED: &str = "src_tokenized";
    pub const DESTINATION: &str = "dst";
    pub const DESTINATION_TOKENIZED: &str = "dst_tokenized";
    pub const LAYER_ID: &str = "layer_id";
}

pub(crate) const TOKENIZER: &str = "custom_default";

pub fn register_default_tokenizers(index: &Index) {
    let tokenizer = TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .build();
    index.tokenizers().register(TOKENIZER, tokenizer);
}

pub(crate) fn new_index(schema: Schema, path: &Option<PathBuf>) -> Result<Index, GraphError> {
    let index_builder = Index::builder()
        .settings(IndexSettings::default())
        .schema(schema);

    let index = if let Some(path) = path {
        create_dir_all(path).map_err(|e| {
            GraphError::IOErrorMsg(format!(
                "Failed to create index directory {}: {}",
                path.display(),
                e
            ))
        })?;

        index_builder.create_in_dir(path).map_err(|e| {
            GraphError::IndexErrorMsg(format!("Failed to create index in directory: {}", e))
        })?
    } else {
        index_builder.create_in_ram().map_err(|e| {
            GraphError::IndexErrorMsg(format!("Failed to create in-memory index: {}", e))
        })?
    };

    register_default_tokenizers(&index);

    Ok(index)
}

fn resolve_props(props: &Vec<Option<PropertyIndex>>) -> HashSet<usize> {
    props
        .iter()
        .enumerate()
        .filter_map(|(idx, opt)| opt.as_ref().map(|_| idx))
        .collect()
}

fn get_props<'a>(
    props: &'a HashSet<usize>,
    meta: &'a PropMapper,
) -> impl Iterator<Item = (String, usize, PropType)> + 'a {
    props.iter().filter_map(|prop_id| {
        let prop_name = meta.get_name(*prop_id).to_string();
        meta.get_dtype(*prop_id)
            .map(|prop_type| (prop_name, *prop_id, prop_type))
    })
}

// Filter props for which there already is a property index
pub(crate) fn indexed_props(
    props: &[(usize, Prop)],
    indexes: &RwLockReadGuard<Vec<Option<PropertyIndex>>>,
) -> Vec<(usize, Prop)> {
    props
        .iter()
        .cloned()
        .filter(|(id, _)| indexes.get(*id).is_some_and(|entry| entry.is_some()))
        .collect()
}

pub(crate) fn get_reader(index: &Arc<Index>) -> Result<IndexReader, GraphError> {
    let reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()?;
    Ok(reader)
}

pub(crate) fn fallback_filter_nodes<G: StaticGraphViewOps>(
    graph: &G,
    filter: &(impl CreateFilter + Clone + 'static),
    limit: usize,
    offset: usize,
) -> Result<Vec<NodeView<'static, G>>, GraphError> {
    let filtered_nodes = graph
        .nodes()
        .select(filter.clone())?
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();
    Ok(filtered_nodes)
}

pub(crate) fn fallback_filter_edges<G: StaticGraphViewOps>(
    graph: &G,
    filter: &(impl CreateFilter + Clone),
    limit: usize,
    offset: usize,
) -> Result<Vec<EdgeView<G>>, GraphError> {
    let filtered_edges = graph
        .filter(filter.clone())?
        .edges()
        .iter()
        .map(|e| EdgeView::new(graph.clone(), e.edge))
        .skip(offset)
        .take(limit)
        .collect();
    Ok(filtered_edges)
}

pub(crate) fn fallback_filter_exploded_edges<G: StaticGraphViewOps>(
    graph: &G,
    filter: &(impl CreateFilter + Clone),
    limit: usize,
    offset: usize,
) -> Result<Vec<EdgeView<G>>, GraphError> {
    let filtered_edges = graph
        .filter(filter.clone())?
        .edges()
        .explode()
        .iter()
        .map(|e| EdgeView::new(graph.clone(), e.edge))
        .skip(offset)
        .take(limit)
        .collect();
    Ok(filtered_edges)
}

#[cfg(test)]
mod test_index {}
