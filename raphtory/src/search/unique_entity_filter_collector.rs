use itertools::Itertools;
use std::collections::HashSet;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    Score, SegmentReader,
};

pub struct UniqueEntityFilterCollector<TCollector>
where
    TCollector: Collector + Send + Sync,
{
    field: String,
    collector: TCollector,
}

impl<TCollector> UniqueEntityFilterCollector<TCollector>
where
    TCollector: Collector + Send + Sync,
{
    pub fn new(field: String, collector: TCollector) -> Self {
        Self { field, collector }
    }
}

impl<TCollector> Collector for UniqueEntityFilterCollector<TCollector>
where
    TCollector: Collector + Send + Sync,
{
    type Fruit = Vec<u64>;
    type Child = UniqueEntityFilterSegmentCollector<TCollector::Child>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(UniqueEntityFilterSegmentCollector {
            column_opt_entity_id,
            segment_collector,
            seen_entities: HashSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut global_seen_entity: HashSet<u64> = HashSet::new();

        for segment in segment_fruits {
            for entity_id in segment {
                global_seen_entity.insert(entity_id);
            }
        }

        Ok(global_seen_entity.into_iter().collect_vec())
    }
}

pub struct UniqueEntityFilterSegmentCollector<TSegmentCollector> {
    column_opt_entity_id: Option<Column<u64>>,
    segment_collector: TSegmentCollector,
    seen_entities: HashSet<u64>,
}

impl<TSegmentCollector> SegmentCollector for UniqueEntityFilterSegmentCollector<TSegmentCollector>
where
    TSegmentCollector: SegmentCollector,
{
    type Fruit = Vec<u64>;

    fn collect(&mut self, doc: u32, _score: Score) {
        if let Some(entity_id) = self
            .column_opt_entity_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc).next())
        {
            self.seen_entities.insert(entity_id);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.seen_entities.into_iter().collect()
    }
}
