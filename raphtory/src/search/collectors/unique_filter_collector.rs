use crate::{
    core::entities::nodes::node_ref::NodeRef, db::api::view::StaticGraphViewOps,
    prelude::NodeViewOps, search::fields,
};
use itertools::Itertools;
use raphtory_api::core::entities::VID;
use std::collections::HashSet;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    schema::Value,
    DocAddress, Document, IndexReader, Score, Searcher, SegmentReader, TantivyDocument,
};

pub struct UniqueFilterCollector<TCollector, G>
where
    TCollector: Collector<Fruit = Vec<(Score, DocAddress)>> + Send + Sync,
    G: StaticGraphViewOps,
{
    field: String,
    collector: TCollector,
    reader: IndexReader,
    graph: G,
}

impl<TCollector, G> UniqueFilterCollector<TCollector, G>
where
    TCollector: Collector<Fruit = Vec<(Score, DocAddress)>> + Send + Sync,
    G: StaticGraphViewOps,
{
    pub fn new(field: String, collector: TCollector, reader: IndexReader, graph: G) -> Self {
        Self {
            field,
            collector,
            reader,
            graph,
        }
    }
}

impl<TCollector, G> Collector for UniqueFilterCollector<TCollector, G>
where
    TCollector: Collector<Fruit = Vec<(Score, DocAddress)>> + Send + Sync,
    TCollector::Child: SegmentCollector<Fruit = Vec<(Score, DocAddress)>>,
    G: StaticGraphViewOps,
{
    type Fruit = Vec<(Score, DocAddress)>;
    type Child = UniqueFilterSegmentCollector<TCollector::Child, G>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let column_opt_time = segment_reader.fast_fields().column_opt(fields::TIME)?;
        let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;

        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;

        Ok(UniqueFilterSegmentCollector {
            column_opt_time,
            column_opt_entity_id,
            segment_collector,
            seen_entities: HashSet::new(),
            segment_ord: segment_local_id,
            graph: self.graph.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut global_seen_entities: HashSet<u64> = HashSet::new();
        let mut unique_docs = Vec::new();

        let searcher = self.reader.searcher();
        let schema = searcher.schema();
        let node_id_field = schema.get_field(fields::NODE_ID)?;

        let mut all_docs = segment_fruits
            .into_iter()
            .flatten()
            .collect::<Vec<(Score, DocAddress)>>();

        // all_docs.sort_by(|a, b| {
        //     let a_time = extract_time(&searcher, a.1).unwrap_or(i64::MIN);
        //     let b_time = extract_time(&searcher, b.1).unwrap_or(i64::MIN);
        //
        //     b_time.cmp(&a_time).then_with(|| b.0.total_cmp(&a.0))
        // });

        for (score, doc_address) in all_docs {
            let segment_reader = searcher.segment_reader(doc_address.segment_ord);
            let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;
            if let Some(entity_id) = column_opt_entity_id
                .as_ref()
                .and_then(|col| col.values_for_doc(doc_address.doc_id).next())
            {
                // let doc = searcher.doc::<TantivyDocument>(doc_address)?;
                if global_seen_entities.insert(entity_id) {
                    unique_docs.push((score, doc_address));
                }
            }
        }

        Ok(unique_docs)
    }
}

pub struct UniqueFilterSegmentCollector<TSegmentCollector, G> {
    column_opt_time: Option<Column<i64>>,
    column_opt_entity_id: Option<Column<u64>>,
    segment_collector: TSegmentCollector,
    seen_entities: HashSet<u64>,
    segment_ord: u32,
    graph: G,
}

impl<TSegmentCollector, G> SegmentCollector for UniqueFilterSegmentCollector<TSegmentCollector, G>
where
    TSegmentCollector: SegmentCollector<Fruit = Vec<(Score, DocAddress)>>,
    G: StaticGraphViewOps,
{
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc_id: u32, score: Score) {
        let opt_entity_id = self
            .column_opt_entity_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());
        let opt_time = self
            .column_opt_time
            .as_ref()
            .and_then(|col| col.values_for_doc(doc_id).next());

        if let (Some(time), Some(entity_id)) = (opt_time, opt_entity_id) {
            if self.seen_entities.insert(entity_id) {
                self.segment_collector.collect(doc_id, score);
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.segment_collector.harvest()
    }
}

fn extract_time(searcher: &Searcher, doc_address: DocAddress) -> Option<i64> {
    searcher
        .segment_reader(doc_address.segment_ord)
        .fast_fields()
        .column_opt(fields::TIME)
        .ok()
        .flatten()
        .as_ref()
        .and_then(|col| col.values_for_doc(doc_address.doc_id).next())
}
