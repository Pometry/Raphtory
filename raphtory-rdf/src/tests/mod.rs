mod bounds;
mod functions;
mod import_export;
mod limits;
mod loading;
mod mapping;
mod oracle;
mod results;
mod scan_equivalence;
#[cfg(feature = "shacl")]
mod shacl;
mod temporal;
mod time_graph;

use crate::{RdfFormat, RdfView, SparqlOptions, SparqlResults, Term, Triple};

use oxigraph::{io::RdfParser, model::Graph};
use raphtory::db::api::view::{IntoDynamic, StaticGraphViewOps};

const FOAF: &str = include_str!("fixtures/foaf.ttl");
const DATATYPES: &str = include_str!("fixtures/datatypes.nt");
const BLANK_NODES: &str = include_str!("fixtures/blank_nodes.ttl");
const SOCIAL: &str = include_str!("fixtures/social.ttl");

fn sorted_rows(rows: &[Vec<Option<Term>>]) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map_or_else(|| "UNDEF".to_owned(), ToString::to_string)
                })
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

fn select<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, query: &str) -> Vec<Vec<String>> {
    match view
        .sparql(query, &SparqlOptions::default())
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        SparqlResults::Solutions { rows, .. } => sorted_rows(&rows),
        other => panic!("{query}: not a SELECT result: {other:?}"),
    }
}

fn ask<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, query: &str) -> bool {
    match view
        .sparql(query, &SparqlOptions::default())
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        SparqlResults::Boolean(value) => value,
        other => panic!("{query}: not an ASK result: {other:?}"),
    }
}

fn exported<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>) -> Vec<String> {
    let mut out = Vec::new();
    view.to_rdf(&mut out, RdfFormat::NTriples).unwrap();
    let mut lines: Vec<String> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

fn parse_graph(doc: &str, format: RdfFormat) -> Graph {
    RdfParser::from_format(format.to_oxigraph())
        .for_reader(doc.as_bytes())
        .map(|quad| Triple::from(quad.unwrap_or_else(|e| panic!("{e}: {doc}"))))
        .collect()
}
