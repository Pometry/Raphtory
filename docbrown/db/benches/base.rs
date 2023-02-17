use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Bencher, Criterion, Throughput,
};
use csv::StringRecord;
use csv_sniffer::Type;
use docbrown_core::Direction;
use docbrown_db::graphdb::GraphDB;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::Path;
use docbrown_core::graphview::GraphView;

use docbrown_it::data;

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn load_csv(graph: &mut GraphDB, path: &Path, source: usize, target: usize, time: Option<usize>) {
    let mut times: Range<i64> = (0..i64::MAX);
    let mut metadata = csv_sniffer::Sniffer::new().sniff_path(path).unwrap();
    metadata.dialect.header = csv_sniffer::metadata::Header {
        has_header_row: false,
        num_preamble_rows: 0,
    };
    let ids_are_numbers =
        metadata.types[source] == Type::Unsigned && metadata.types[target] == Type::Unsigned;
    let mut reader = metadata
        .dialect
        .open_reader(File::open(path).unwrap())
        .unwrap();

    let mut parse_record = |rec: &StringRecord| {
        let source_str = rec.get(source).ok_or("No source id")?;
        let target_str = rec.get(target).ok_or("No target id")?;
        let (source_value, target_value): (u64, u64) = if ids_are_numbers {
            (source_str.parse::<u64>()?, target_str.parse::<u64>()?)
        } else {
            (hash(&source_str), hash(&target_str))
        };
        let time_value: i64 = match time {
            Some(time) => rec.get(time).ok_or("No time value")?.parse()?,
            None => times.next().ok_or("Max time reached")?,
        };
        Ok::<(u64, u64, i64), Box<dyn Error>>((source_value, target_value, time_value))
    };

    for record in reader.records() {
        let record_ok = record.unwrap();
        let (source_id, target_id, time) =
            parse_record(&record_ok).expect(&format!("Unable to parse record: {:?}", record_ok));
        graph.add_vertex(source_id, time, &vec![]);
        graph.add_vertex(target_id, time, &vec![]);
        graph.add_edge(source_id, target_id, time, &vec![]);
    }
}

pub fn additions(c: &mut Criterion) {
    let mut graph = GraphDB::new(4);
    graph.add_vertex(0, 0, &vec![]);

    let mut g = c.benchmark_group("additions");
    g.throughput(Throughput::Elements(1));

    let mut times: Range<i64> = 0..i64::MAX;
    let mut indexes: Range<u64> = 0..u64::MAX;

    g.bench_function("existing vertex constant time", |b| {
        b.iter(|| graph.add_vertex(0, 0, &vec![]))
    });
    g.bench_function("existing vertex varying time", |b: &mut Bencher| {
        b.iter_batched(
            || times.next().unwrap(),
            |t| graph.add_vertex(0, t, &vec![]),
            BatchSize::SmallInput,
        )
    });
    g.bench_function("new vertex constant time", |b: &mut Bencher| {
        b.iter_batched(
            || indexes.next().unwrap(),
            |vid| graph.add_vertex(vid, 0, &vec![]),
            BatchSize::SmallInput,
        )
    });

    g.bench_function("existing edge constant time", |b| {
        b.iter(|| graph.add_edge(0, 1, 0, &vec![]))
    });
    g.bench_function("existing edge varying time", |b: &mut Bencher| {
        b.iter_batched(
            || times.next().unwrap(),
            |t| graph.add_edge(0, 0, t, &vec![]),
            BatchSize::SmallInput,
        )
    });
    g.bench_function("new edge constant time", |b: &mut Bencher| {
        b.iter_batched(
            || (indexes.next().unwrap(), indexes.next().unwrap()),
            |(v1, v2)| graph.add_edge(v1, v2, 0, &vec![]),
            BatchSize::SmallInput,
        )
    });

    g.finish();
}

pub fn ingestion(c: &mut Criterion) {
    let mut g = c.benchmark_group("ingestion");

    g.throughput(Throughput::Elements(2649));
    let lotr = data::lotr().unwrap();
    g.bench_function("lotr.csv", |b: &mut Bencher| {
        b.iter(|| {
            let mut graph = GraphDB::new(3);
            load_csv(&mut graph, &lotr, 0, 1, Some(2));
        })
    });

    g.throughput(Throughput::Elements(1400000));
    g.sample_size(20);
    let twitter = data::twitter().unwrap();
    g.bench_function("twitter.csv", |b: &mut Bencher| {
        b.iter(|| {
            let mut graph = GraphDB::new(3);
            load_csv(&mut graph, &twitter, 0, 1, None);
        })
    });

    g.finish();
}

pub fn analysis(c: &mut Criterion) {
    let mut g = c.benchmark_group("analysis");
    let lotr = data::lotr().unwrap();
    let mut graph = GraphDB::new(3);
    load_csv(&mut graph, &lotr, 0, 1, Some(2));
    g.bench_function("n_edges", |b| b.iter(|| graph.n_edges()));
    g.finish();
}

criterion_group!(benches, additions, ingestion, analysis);
criterion_main!(benches);
