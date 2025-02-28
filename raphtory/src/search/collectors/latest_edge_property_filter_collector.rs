use crate::{db::api::view::StaticGraphViewOps, prelude::TimeOps, search::fields};
use itertools::Itertools;
use raphtory_api::core::{
    entities::EID,
    storage::timeindex::{AsTime, TimeIndexEntry},
};
use std::collections::HashSet;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    DocAddress, Document, IndexReader, Score, SegmentReader, TantivyDocument,
};

pub struct LatestEdgePropertyFilterCollector<G> {
    prop_id: usize,
    field: String,
    reader: IndexReader,
    graph: G,
}

impl<G> LatestEdgePropertyFilterCollector<G>
where
    G: StaticGraphViewOps,
{
    pub fn new(field: String, prop_id: usize, reader: IndexReader, graph: G) -> Self {
        Self {
            field,
            prop_id,
            reader,
            graph,
        }
    }
}

impl<G> Collector for LatestEdgePropertyFilterCollector<G>
where
    G: StaticGraphViewOps,
{
    type Fruit = HashSet<u64>;
    type Child = LatestEdgePropertyFilterSegmentCollector<G>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt_time = segment_reader.fast_fields().column_opt(fields::TIME)?;
        let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;
        let column_opt_layer_id = segment_reader.fast_fields().column_opt(fields::LAYER_ID)?;
        let column_opt_secondary_time: Option<Column<u64>> = segment_reader
            .fast_fields()
            .column_opt(fields::SECONDARY_TIME)?;

        Ok(LatestEdgePropertyFilterSegmentCollector {
            prop_id: self.prop_id,
            column_opt_time,
            column_opt_entity_id,
            column_opt_layer_id,
            column_opt_secondary_time,
            segment_ord: segment_local_id,
            unique_entity_ids: HashSet::new(),
            graph: self.graph.clone(),
            reader: self.reader.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<HashSet<u64>>) -> tantivy::Result<HashSet<u64>> {
        let mut global_unique_entity_ids: HashSet<u64> = HashSet::new();

        for entity_id in segment_fruits.into_iter().flatten() {
            global_unique_entity_ids.insert(entity_id);
        }

        Ok(global_unique_entity_ids)
    }
}

pub struct LatestEdgePropertyFilterSegmentCollector<G> {
    prop_id: usize,
    column_opt_time: Option<Column<i64>>,
    column_opt_entity_id: Option<Column<u64>>,
    column_opt_layer_id: Option<Column<u64>>,
    column_opt_secondary_time: Option<Column<u64>>,
    segment_ord: u32,
    unique_entity_ids: HashSet<u64>,
    graph: G,
    reader: IndexReader,
}

impl<G> SegmentCollector for LatestEdgePropertyFilterSegmentCollector<G>
where
    G: StaticGraphViewOps,
{
    type Fruit = HashSet<u64>;

    fn collect(&mut self, doc_id: u32, _score: Score) {
        let opt_time = self
            .column_opt_time
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());
        let opt_entity_id = self
            .column_opt_entity_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());
        let opt_layer_id = self
            .column_opt_layer_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());
        let opt_secondary_time = self
            .column_opt_secondary_time
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());

        // let searcher = self.reader.searcher();
        // let schema = searcher.schema();
        // let doc = searcher
        //     .doc::<TantivyDocument>(DocAddress::new(self.segment_ord, doc_id))
        //     .unwrap();
        // println!("doc = {:?}", doc.to_json(schema));

        if let (Some(time), Some(entity_id), Some(layer_id), Some(secondary_time)) =
            (opt_time, opt_entity_id, opt_layer_id, opt_secondary_time)
        {
            // If is_node_prop_update_latest check is true for a doc, we can ignore validating all other docs
            // against expensive is_node_prop_update_latest check for a given node id.
            if !self.unique_entity_ids.contains(&entity_id) {
                if self.graph.is_edge_prop_update_latest(
                    self.graph.layer_ids(),
                    layer_id as usize,
                    self.prop_id,
                    EID(entity_id as usize),
                    TimeIndexEntry::new(time, secondary_time as usize),
                ) {
                    self.unique_entity_ids.insert(entity_id);
                }
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.unique_entity_ids
    }
}
