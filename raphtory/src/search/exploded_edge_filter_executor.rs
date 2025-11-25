use crate::{
    db::{
        api::view::{internal::FilterOps, BoxableGraphView, Filter, StaticGraphViewOps},
        graph::{
            edge::EdgeView,
            views::filter::{
                internal::CreateFilter,
                model::{
                    exploded_edge_filter::{CompositeExplodedEdgeFilter, ExplodedEdgeFilter},
                    property_filter::PropertyRef,
                },
            },
        },
    },
    errors::GraphError,
    prelude::{EdgeViewOps, PropertyFilter, TimeOps},
    search::{
        collectors::{
            exploded_edge_property_filter_collector::ExplodedEdgePropertyFilterCollector,
            unique_entity_filter_collector::UniqueEntityFilterCollector,
        },
        fallback_filter_exploded_edges, fields, get_reader,
        graph_index::Index,
        property_index::PropertyIndex,
        query_builder::QueryBuilder,
    },
};
use itertools::Itertools;
use raphtory_api::core::{
    entities::EID,
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use raphtory_storage::graph::edges::edge_storage_ops::EdgeStorageOps;
use std::{collections::HashSet, sync::Arc};
use tantivy::{collector::Collector, query::Query, IndexReader};

#[derive(Clone, Copy)]
pub struct ExplodedEdgeFilterExecutor<'a> {
    index: &'a Index,
    query_builder: QueryBuilder<'a>,
}

impl<'a> ExplodedEdgeFilterExecutor<'a> {
    pub fn new(index: &'a Index) -> Self {
        Self {
            index,
            query_builder: QueryBuilder::new(index),
        }
    }

    fn execute_filter_query<G: StaticGraphViewOps>(
        &self,
        filter: impl CreateFilter,
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let searcher = reader.searcher();
        let collector = UniqueEntityFilterCollector::new(fields::EDGE_ID.to_string());
        let edge_ids = searcher.search(&query, &collector)?;
        let edges = self.resolve_exploded_edges_from_edge_ids(filter, graph, edge_ids)?;

        if offset == 0 && limit >= edges.len() {
            Ok(edges)
        } else {
            Ok(edges.into_iter().skip(offset).take(limit).collect())
        }
    }

    fn execute_filter_property_query<G, C>(
        &self,
        filter: impl CreateFilter,
        graph: &G,
        query: Box<dyn Query>,
        reader: &IndexReader,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        G: StaticGraphViewOps,
        C: Collector<Fruit = HashSet<(TimeIndexEntry, EID, usize)>>,
    {
        let searcher = reader.searcher();
        let collector = collector_fn(fields::EDGE_ID.to_string());
        let edge_ids = searcher.search(&query, &collector)?;
        let edges = self.resolve_exploded_edges_from_exploded_edge_ids(filter, graph, edge_ids)?;

        if offset == 0 && limit >= edges.len() {
            Ok(edges)
        } else {
            Ok(edges.into_iter().skip(offset).take(limit).collect())
        }
    }

    fn execute_or_fallback<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter<ExplodedEdgeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let query = self.query_builder.build_property_query(pi, filter)?;
        let reader = get_reader(&pi.index)?;
        match query {
            Some(query) => {
                self.execute_filter_query(filter.clone(), graph, query, &reader, limit, offset)
            }
            // Fallback to raphtory apis
            None => fallback_filter_exploded_edges(graph, filter, limit, offset),
        }
    }

    fn execute_or_fallback_temporal<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        pi: &Arc<PropertyIndex>,
        filter: &PropertyFilter<ExplodedEdgeFilter>,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<(TimeIndexEntry, EID, usize)>>,
    {
        let query = self.query_builder.build_property_query(pi, filter)?;
        let reader = get_reader(&pi.index)?;
        match query {
            Some(query) => self.execute_filter_property_query(
                filter.clone(),
                graph,
                query,
                &reader,
                limit,
                offset,
                collector_fn,
            ),
            // Fallback to raphtory apis
            None => fallback_filter_exploded_edges(graph, filter, limit, offset),
        }
    }

    fn apply_metadata_filter<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter<ExplodedEdgeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        if let Some((cpi, _)) = self
            .index
            .edge_index
            .entity_index
            .get_metadata_index(graph.edge_meta(), prop_name)?
        {
            self.execute_or_fallback(graph, &cpi, filter, limit, offset)
        } else {
            fallback_filter_exploded_edges(graph, filter, limit, offset)
        }
    }

    fn apply_temporal_property_filter<G: StaticGraphViewOps, C>(
        &self,
        graph: &G,
        prop_name: &str,
        filter: &PropertyFilter<ExplodedEdgeFilter>,
        limit: usize,
        offset: usize,
        collector_fn: impl Fn(String) -> C,
    ) -> Result<Vec<EdgeView<G>>, GraphError>
    where
        C: Collector<Fruit = HashSet<(TimeIndexEntry, EID, usize)>>,
    {
        if let Some((tpi, _)) = self
            .index
            .edge_index
            .entity_index
            .get_temporal_property_index(graph.edge_meta(), prop_name)?
        {
            self.execute_or_fallback_temporal(graph, &tpi, filter, limit, offset, collector_fn)
        } else {
            fallback_filter_exploded_edges(graph, filter, limit, offset)
        }
    }

    fn filter_property_index<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &PropertyFilter<ExplodedEdgeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        if filter.ops.is_empty() {
            return fallback_filter_exploded_edges(graph, filter, limit, offset);
        }

        match &filter.prop_ref {
            PropertyRef::Metadata(prop_name) => {
                self.apply_metadata_filter(graph, prop_name, filter, limit, offset)
            }
            PropertyRef::TemporalProperty(prop_name) | PropertyRef::Property(prop_name) => self
                .apply_temporal_property_filter(
                    graph,
                    prop_name,
                    filter,
                    limit,
                    offset,
                    ExplodedEdgePropertyFilterCollector::new,
                ),
        }
    }

    pub fn filter_exploded_edges_internal<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeExplodedEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        match filter {
            CompositeExplodedEdgeFilter::Src(_) | CompositeExplodedEdgeFilter::Dst(_) => {
                // TODO: Can we use an index here to speed up search?
                fallback_filter_exploded_edges(graph, filter, limit, offset)
            }
            CompositeExplodedEdgeFilter::Property(filter) => {
                self.filter_property_index(graph, filter, limit, offset)
            }
            CompositeExplodedEdgeFilter::Windowed(filter) => {
                let start = filter.start.t();
                let end = filter.end.t();

                let dyn_graph: Arc<dyn BoxableGraphView> = Arc::new((*graph).clone());
                let dyn_graph = dyn_graph.window(start, end);
                let res = self.filter_exploded_edges(&dyn_graph, &filter.inner, limit, offset)?;
                Ok(res
                    .into_iter()
                    .map(|x| EdgeView::new(graph.clone(), x.edge))
                    .collect())
            }
            CompositeExplodedEdgeFilter::And(left, right) => {
                let left_result = self.filter_exploded_edges(graph, left, limit, offset)?;
                let right_result = self.filter_exploded_edges(graph, right, limit, offset)?;

                // Intersect results
                let left_set: HashSet<_> = left_result.into_iter().collect();
                let intersection = right_result
                    .into_iter()
                    .filter(|e| left_set.contains(e))
                    .collect::<Vec<_>>();

                Ok(intersection)
            }
            CompositeExplodedEdgeFilter::Or(left, right) => {
                let left_result = self.filter_exploded_edges(graph, left, limit, offset)?;
                let right_result = self.filter_exploded_edges(graph, right, limit, offset)?;

                // Union results
                let mut combined = HashSet::new();
                combined.extend(left_result);
                combined.extend(right_result);

                Ok(combined.into_iter().collect())
            }
            CompositeExplodedEdgeFilter::Not(_) => {
                fallback_filter_exploded_edges(graph, filter, limit, offset)
            }
        }
    }

    pub fn filter_exploded_edges<G: StaticGraphViewOps>(
        &self,
        graph: &G,
        filter: &CompositeExplodedEdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        self.filter_exploded_edges_internal(graph, filter, limit, offset)
    }

    fn resolve_exploded_edges_from_exploded_edge_ids<G: StaticGraphViewOps>(
        &self,
        filter: impl CreateFilter,
        graph: &G,
        exploded_edge_ids: HashSet<(TimeIndexEntry, EID, usize)>,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let filtered_graph = graph.filter(filter)?;
        let edges = exploded_edge_ids
            .into_iter()
            .filter_map(|(tie, eid, layer_id)| {
                if filtered_graph.filter_exploded_edge(eid.with_layer(layer_id), tie) {
                    let e_ref = graph.core_edge(eid).out_ref().at(tie).at_layer(layer_id);
                    Some(EdgeView::new(graph.clone(), e_ref))
                } else {
                    None
                }
            })
            .collect_vec();
        Ok(edges)
    }

    fn resolve_exploded_edges_from_edge_ids<G: StaticGraphViewOps>(
        &self,
        filter: impl CreateFilter,
        graph: &G,
        edge_ids: HashSet<u64>,
    ) -> Result<Vec<EdgeView<G>>, GraphError> {
        let filtered_graph = graph.filter(filter)?;
        let edges = edge_ids
            .into_iter()
            .filter_map(|id| {
                let e_ref = graph.core_edge(EID(id as usize));
                filtered_graph
                    .filter_edge(e_ref.as_ref())
                    .then(|| EdgeView::new(graph.clone(), e_ref.out_ref()))
            })
            .flat_map(|e| e.explode())
            .collect_vec();
        Ok(edges)
    }
}
