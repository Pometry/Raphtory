use itertools::Itertools;
use std::collections::HashSet;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    DocId, Score, SegmentReader,
};

use crate::{db::api::view::StaticGraphViewOps, search::fields};

pub struct UniqueNodeFilterCollector<TCollector>
where
    TCollector: Collector + Send + Sync,
{
    field: String,
    collector: TCollector,
}

impl<TCollector> UniqueNodeFilterCollector<TCollector>
where
    TCollector: Collector + Send + Sync,
{
    pub fn new(field: String, collector: TCollector) -> Self {
        Self { field, collector }
    }
}

impl<TCollector> Collector for UniqueNodeFilterCollector<TCollector>
where
    TCollector: Collector + Send + Sync,
{
    type Fruit = Vec<u64>;
    type Child = UniqueNodeFilterSegmentCollector<TCollector::Child>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt_node_id = segment_reader.fast_fields().column_opt(&self.field)?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(UniqueNodeFilterSegmentCollector {
            column_opt_node_id,
            segment_collector,
            seen_nodes: HashSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut global_seen_nodes: HashSet<u64> = HashSet::new();

        for segment in segment_fruits {
            for node_id in segment {
                global_seen_nodes.insert(node_id);
            }
        }

        Ok(global_seen_nodes.into_iter().collect_vec())
    }
}

pub struct UniqueNodeFilterSegmentCollector<TSegmentCollector> {
    column_opt_node_id: Option<Column<u64>>,
    segment_collector: TSegmentCollector,
    seen_nodes: HashSet<u64>,
}

impl<TSegmentCollector> SegmentCollector for UniqueNodeFilterSegmentCollector<TSegmentCollector>
where
    TSegmentCollector: SegmentCollector,
{
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: u32, _score: Score) {
        if let Some(node_id) = self
            .column_opt_node_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc).next())
        {
            self.seen_nodes.insert(node_id);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.seen_nodes.into_iter().collect()
    }
}
