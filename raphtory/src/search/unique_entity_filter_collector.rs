use std::collections::HashSet;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    DocAddress, Document, IndexReader, Score, SegmentReader, TantivyDocument,
};

pub struct UniqueEntityFilterCollector<TCollector>
where
    TCollector: Collector<Fruit = Vec<(Score, DocAddress)>> + Send + Sync,
{
    field: String,
    collector: TCollector,
    reader: IndexReader,
}

impl<TCollector> UniqueEntityFilterCollector<TCollector>
where
    TCollector: Collector<Fruit = Vec<(Score, DocAddress)>> + Send + Sync,
{
    pub fn new(field: String, collector: TCollector, reader: IndexReader) -> Self {
        Self {
            field,
            collector,
            reader,
        }
    }
}

impl<TCollector> Collector for UniqueEntityFilterCollector<TCollector>
where
    TCollector: Collector<Fruit = Vec<(Score, DocAddress)>> + Send + Sync,
    TCollector::Child: SegmentCollector<Fruit = Vec<(Score, DocAddress)>>,
{
    type Fruit = Vec<(Score, DocAddress)>;
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
            segment_ord: segment_local_id,
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

        for segment in segment_fruits {
            for (score, doc_address) in segment {
                let segment_reader = searcher.segment_reader(doc_address.segment_ord);
                // let schema = searcher.schema();

                let column_opt_entity_id = segment_reader.fast_fields().column_opt(&self.field)?;
                if let Some(entity_id) = column_opt_entity_id
                    .as_ref()
                    .and_then(|col| col.values_for_doc(doc_address.doc_id).next())
                {
                    if global_seen_entities.insert(entity_id) {
                        // let doc = searcher.doc::<TantivyDocument>(doc_address)?;
                        // println!("Unique Document: {}", doc.to_json(&schema));
                        unique_docs.push((score, doc_address));
                    }
                }
            }
        }

        Ok(unique_docs)
    }
}

pub struct UniqueEntityFilterSegmentCollector<TSegmentCollector> {
    column_opt_entity_id: Option<Column<u64>>,
    segment_collector: TSegmentCollector,
    seen_entities: HashSet<u64>,
    segment_ord: u32,
}

impl<TSegmentCollector> SegmentCollector for UniqueEntityFilterSegmentCollector<TSegmentCollector>
where
    TSegmentCollector: SegmentCollector<Fruit = Vec<(Score, DocAddress)>>,
{
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc: u32, score: Score) {
        if let Some(entity_id) = self
            .column_opt_entity_id
            .as_ref()
            .and_then(|col| col.values_for_doc(doc).next())
        {
            if self.seen_entities.insert(entity_id) {
                self.segment_collector.collect(doc, score);
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.segment_collector.harvest()
    }
}
