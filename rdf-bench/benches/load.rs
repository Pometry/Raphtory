//! `load_rdf` of a generated FOAF-like document of about 200k triples, as N-Triples and as Turtle.
//!
//! Run with `cargo bench -p raphtory-rdf-bench --features rdf --bench load` (or `make bench-load`
//! in `rdf-bench/`).
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use raphtory::{prelude::*, rdf::RdfFormat};
use std::fmt::Write;

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

/// `load_rdf` of a document of 20,000 people in each format, each on a new `RdfGraph`.
fn rdf(c: &mut Criterion) {
    let doc = people(20_000);
    let triples = doc.lines().count() as u64;
    let mut group = c.benchmark_group("rdf_load");
    group.sample_size(10);
    group.throughput(Throughput::Elements(triples));
    for format in [RdfFormat::NTriples, RdfFormat::Turtle] {
        group.bench_function(format!("{format}_200k"), |b| {
            b.iter_batched(
                RdfGraph::new,
                |g| {
                    g.load_rdf(1, doc.as_bytes(), format, None).unwrap();
                    g
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

criterion_group!(benches, rdf);
criterion_main!(benches);
