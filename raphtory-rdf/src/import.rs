//! Writing RDF triples to a graph: one `add_edge` or `delete_edge` event per triple.
use crate::{
    mapping::{is_rdf11_term, layer_name_of, name_of},
    RdfError, RdfFormat,
};
use oxigraph::{
    io::RdfParser,
    model::{TermRef, TripleRef},
};
use raphtory::{errors::GraphError, prelude::*};
use raphtory_api::core::utils::time::InputTime;
use std::{fs::File, io::Read, path::Path};

fn unstorable(t: TermRef<'_>) -> RdfError {
    if is_rdf11_term(t) {
        RdfError::NonCanonicalTerm(t.to_string())
    } else {
        RdfError::Rdf12Term(t.to_string())
    }
}

fn name(t: TermRef<'_>) -> Result<String, RdfError> {
    name_of(t).ok_or_else(|| unstorable(t))
}

fn layer(t: TermRef<'_>) -> Result<String, RdfError> {
    layer_name_of(t).ok_or_else(|| unstorable(t))
}

pub(crate) fn write_triple<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    triple: TripleRef<'_>,
    retract: bool,
) -> Result<(), RdfError> {
    let s = name(triple.subject.into())?;
    let p = layer(triple.predicate.into())?;
    let o = name(triple.object)?;
    if retract {
        g.delete_edge(time, &s, &o, Some(&p))?;
    } else {
        g.add_edge(time, &s, &o, NO_PROPS, Some(&p))?;
    }
    Ok(())
}

pub(crate) fn read_document<G: AdditionOps + DeletionOps>(
    g: &G,
    time: InputTime,
    data: impl Read,
    format: RdfFormat,
    base_iri: Option<&str>,
    retract: bool,
) -> Result<usize, RdfError> {
    let mut parser = RdfParser::from_format(format.to_oxigraph()).without_named_graphs();
    if !retract {
        parser = parser.rename_blank_nodes();
    }
    if let Some(base_iri) = base_iri {
        parser = parser.with_base_iri(base_iri).map_err(RdfError::from)?;
    }
    let mut count = 0;
    // the parsers buffer their input themselves
    for quad in parser.for_reader(data) {
        let quad = quad.map_err(RdfError::from)?;
        let triple = quad.as_ref().into();
        write_triple(g, time, triple, retract)?;
        count += 1;
    }
    Ok(count)
}

pub(crate) fn open_document(
    path: &Path,
    format: Option<RdfFormat>,
) -> Result<(File, RdfFormat), RdfError> {
    let format = format.map_or_else(|| RdfFormat::from_path(path), Ok)?;
    // `File::open` opens a directory on Unix, and reading it fails later with a vaguer error
    if path.is_dir() {
        return Err(GraphError::IOErrorMsg(format!(
            "cannot open '{}': it is a directory",
            path.display()
        ))
        .into());
    }
    let file = File::open(path).map_err(|error| {
        GraphError::IOErrorMsg(format!("cannot open '{}': {error}", path.display()))
    })?;
    Ok((file, format))
}
