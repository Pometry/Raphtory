//! `load_rdf` and `retract_rdf` on generated FOAF-like documents of about 200k and 2M triples.
//!
//! Run with `cargo bench -p raphtory-rdf-bench --features rdf --bench load` (or `make bench-load`
//! in `rdf-bench/`).
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use raphtory::{
    prelude::*,
    rdf::{RdfFormat, RdfMutationOps},
};
use std::{fmt::Write, time::Duration};

/// About 10.2 triples per person: IRIs, plain, typed and language-tagged literals, `knows` links
/// and, for every tenth person, a blank-node address.
fn people(n: usize) -> String {
    let mut doc = String::with_capacity(n * 1100);
    let mut r: u64 = 0x9E3779B97F4A7C15;
    let mut next = || {
        r ^= r << 13;
        r ^= r >> 7;
        r ^= r << 17;
        r
    };
    for i in 0..n {
        let s = format!("<http://example.org/person/{i}>");
        let _ = writeln!(
            doc,
            "{s} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> ."
        );
        let _ = writeln!(
            doc,
            "{s} <http://xmlns.com/foaf/0.1/name> \"Person number {i}\" ."
        );
        let _ = writeln!(
            doc,
            "{s} <http://xmlns.com/foaf/0.1/age> \"{}\"^^<http://www.w3.org/2001/XMLSchema#integer> .",
            18 + next() % 60
        );
        let _ = writeln!(
            doc,
            "{s} <http://example.org/birthDate> \"19{}-0{}-1{}\"^^<http://www.w3.org/2001/XMLSchema#date> .",
            50 + next() % 50,
            1 + next() % 9,
            next() % 10
        );
        for _ in 0..3 {
            let _ = writeln!(
                doc,
                "{s} <http://xmlns.com/foaf/0.1/knows> <http://example.org/person/{}> .",
                next() % n as u64
            );
        }
        let _ = writeln!(
            doc,
            "{s} <http://example.org/worksFor> <http://example.org/org/{}> .",
            next() % 2000
        );
        let _ = writeln!(
            doc,
            "{s} <http://www.w3.org/2000/01/rdf-schema#label> \"Person {i}\"@en ."
        );
        let _ = writeln!(
            doc,
            "{s} <http://xmlns.com/foaf/0.1/mbox> <mailto:p{i}@example.org> ."
        );
        if i % 10 == 0 {
            let _ = writeln!(doc, "{s} <http://example.org/address> _:addr{i} .");
            let _ = writeln!(
                doc,
                "_:addr{i} <http://example.org/city> \"City {}\" .",
                next() % 500
            );
        }
    }
    doc
}

/// `load_rdf` of a document about `people_count` people in each of `formats` and, if `retract`,
/// `retract_rdf` of it as N-Triples, each on a new `PersistentGraph`.
fn bench_load(
    c: &mut Criterion,
    people_count: usize,
    label: &str,
    formats: &[RdfFormat],
    retract: bool,
) {
    let doc = people(people_count);
    let triples = doc.lines().count() as u64;
    let mut group = c.benchmark_group("rdf_load");
    group.sample_size(10);
    group.throughput(Throughput::Elements(triples));
    if people_count > 100_000 {
        group.measurement_time(Duration::from_secs(40));
    }
    for &format in formats {
        group.bench_function(format!("{format}_{label}"), |b| {
            b.iter_batched(
                PersistentGraph::new,
                |g| {
                    g.load_rdf(1, doc.as_bytes(), format, None).unwrap();
                    g
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();

    if retract {
        let mut group = c.benchmark_group("rdf_retract");
        group.sample_size(10);
        group.throughput(Throughput::Elements(triples));
        group.bench_function(format!("N-Triples_{label}"), |b| {
            b.iter_batched(
                PersistentGraph::new,
                |g| {
                    g.retract_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
                        .unwrap();
                    g
                },
                BatchSize::PerIteration,
            )
        });
        group.finish();
    }
}

fn rdf(c: &mut Criterion) {
    bench_load(
        c,
        20_000,
        "200k",
        &[RdfFormat::NTriples, RdfFormat::Turtle],
        true,
    );
    bench_load(c, 200_000, "2M", &[RdfFormat::NTriples], false);
}

criterion_group!(benches, rdf);
criterion_main!(benches);
