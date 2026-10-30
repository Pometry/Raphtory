//! SHACL validation of graph views: the SHACL Core engine of rudof's `shacl` crate reads the
//! triples [`to_rdf`](crate::RdfView::to_rdf) writes straight from the view, one lookup at a time.
use crate::{
    graph::{layer, name, named, open_document},
    mapping::{iri_of, term_name, term_of},
    query::on_stack,
    scan::matching,
    RdfError, RdfFormat,
};
use oxigraph::{
    io::{RdfParser, RdfSerializer},
    model::{
        BlankNode, Graph, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, Term, TermRef, Triple,
        TripleRef,
    },
};
use prefixmap::{PrefixMap, PrefixMapError};
use raphtory::{
    db::{
        api::view::{internal::CoreGraphOps, DynamicGraph},
        graph::edge::EdgeView,
    },
    prelude::*,
};
use raphtory_api::core::entities::VID;
use rudof_iri::IriS;
use rudof_rdf::{
    rdf_core::{
        term::Object,
        vocabs::{RdfVocab, ShaclNodeExprVocab, ShaclVocab},
        Any, Matcher, NeighsRDF, Rdf, SHACLPath,
    },
    rdf_impl::OxigraphInMemory,
};
use rustc_hash::FxHashMap;
use shacl::{
    error::IRError,
    ir::{dg::ShapeRecursionKind, IRSchema},
    rdf::ShaclParser,
    validator::{
        engine::{Engine, NativeEngine},
        processor::ShaclProcessor,
        report::{ValidationReport, ValidationResult},
        ShaclConfig, ShaclValidationMode,
    },
};
use std::{
    fmt,
    io::{Read, Write},
    mem,
    path::Path,
};

/// A shapes graph the validator does not accept (see [`RdfError::ShaclLimit`]).
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShaclLimit {
    #[error("recursive SHACL shapes (a shape that refers to itself) are not accepted")]
    RecursiveShapes,
    #[error("a SHACL path that contains itself is not accepted")]
    CyclicPath,
    #[error(
        "a SHACL path that expands to more steps than the shapes graph has triples is not accepted"
    )]
    PathTooLarge,
}

/// Options for [`RdfView::validate_shacl`](crate::RdfView::validate_shacl).
#[derive(Clone, Debug, Default)]
pub struct ShaclOptions {
    reject_recursive_shapes: bool,
}

impl ShaclOptions {
    /// Fail with [`ShaclLimit::RecursiveShapes`] rather than validate with recursive shapes.
    /// The validator recurses once per node along them, so a long chain aborts the process.
    pub fn with_reject_recursive_shapes(mut self, reject: bool) -> Self {
        self.reject_recursive_shapes = reject;
        self
    }
}

/// A parsed and compiled SHACL shapes graph, which can validate many views from many threads.
#[derive(Debug)]
pub struct ShaclShapes {
    schema: IRSchema,
    recursive: bool,
    // sizes the stack of all rudof work, which recurses along the shapes graph's nesting
    pub(crate) depth: usize,
}

/// One `sh:ValidationResult`, with its terms as Raphtory names ([`term_name`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "python", derive(pyo3::IntoPyObject))]
#[non_exhaustive]
pub struct ShaclResult {
    pub focus_node: String,
    pub constraint_component: String,
    /// The predicate's name for a single predicate, otherwise the path in rudof's syntax.
    pub path: Option<String>,
    pub value: Option<String>,
    pub source_shape: Option<String>,
    pub severity: String,
    pub messages: Vec<String>,
}

/// The `sh:ValidationReport` of [`RdfView::validate_shacl`](crate::RdfView::validate_shacl).
/// Its paths nest as deep as the shapes, so it is read and dropped on a stack sized like theirs.
#[derive(Debug)]
pub struct ShaclReport(pub(crate) ValidationReport, usize);

impl ShaclShapes {
    /// Parses and compiles a shapes graph read in `format`, or Turtle if it is `None`.
    pub fn parse(
        data: impl Read,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<Self, RdfError> {
        let format = format.into().unwrap_or(RdfFormat::Turtle);
        let mut parser = RdfParser::from_format(format.to_oxigraph()).without_named_graphs();
        if let Some(base_iri) = base_iri {
            parser = parser.with_base_iri(base_iri).map_err(RdfError::from)?;
        }
        let graph = parser
            .for_reader(data)
            .map(|quad| quad.map(Triple::from))
            .collect::<Result<Graph, _>>()
            .map_err(RdfError::from)?;
        Self::compile(graph)
    }

    /// [`parse`](Self::parse) of a file, in `format` or the one its extension names.
    pub fn parse_path(
        path: impl AsRef<Path>,
        format: impl Into<Option<RdfFormat>>,
        base_iri: Option<&str>,
    ) -> Result<Self, RdfError> {
        let (file, format) = open_document(path.as_ref(), format.into())?;
        Self::parse(file, format, base_iri)
    }

    /// Compiles a shapes graph from its triples.
    pub fn from_triples<'a>(
        triples: impl IntoIterator<Item = TripleRef<'a>>,
    ) -> Result<Self, RdfError> {
        Self::compile(triples.into_iter().collect())
    }

    fn compile(graph: Graph) -> Result<Self, RdfError> {
        let depth = nesting(&graph)?;
        let (schema, recursive) = on_stack("the SHACL shapes compiler", depth, || {
            let mut store = OxigraphInMemory::new();
            for triple in graph.iter() {
                store
                    .add_triple_ref(triple.subject, triple.predicate, triple.object)
                    .map_err(RdfError::shacl)?;
            }
            let ast = ShaclParser::new(store)
                .parse()
                .map_err(|error| RdfError::shacl(IRError::from(error)))?;
            let schema = IRSchema::try_from(ast).map_err(RdfError::shacl)?;
            let recursive = schema
                .recursion_kinds()
                .values()
                .any(|kind| *kind != ShapeRecursionKind::NonRecursive);
            Ok((schema, recursive))
        })?;
        Ok(Self {
            schema,
            recursive,
            depth,
        })
    }

    pub(crate) fn validate(
        &self,
        view: DynamicGraph,
        options: &ShaclOptions,
    ) -> Result<ShaclReport, RdfError> {
        if self.recursive && options.reject_recursive_shapes {
            return Err(RdfError::ShaclLimit(ShaclLimit::RecursiveShapes));
        }
        let report = on_stack("the SHACL validator", self.depth, || {
            Store::new(view)
                .validate(
                    &self.schema,
                    &ShaclValidationMode::Native,
                    &ShaclConfig::default(),
                )
                .map_err(RdfError::shacl)
        })?;
        Ok(ShaclReport(report, self.depth))
    }
}

// The longest chain of links rudof recurses along: any link between shapes, but under a path only
// path constructs, where an IRI is a predicate unless it is a list cell. Rejects paths rudof would
// expand without end, or to more steps than the graph has triples, as it expands a shared sub-path
// once per use.
fn nesting(graph: &Graph) -> Result<usize, RdfError> {
    const PATH_LINKS: [&str; 7] = [
        RdfVocab::RDF_FIRST,
        RdfVocab::RDF_REST,
        ShaclVocab::SH_ALTERNATIVE_PATH,
        ShaclVocab::SH_ZERO_OR_MORE_PATH,
        ShaclVocab::SH_ONE_OR_MORE_PATH,
        ShaclVocab::SH_ZERO_OR_ONE_PATH,
        ShaclVocab::SH_INVERSE_PATH,
    ];
    // by (node, whether in a path): `None` while open, then (depth, path steps)
    let mut done: FxHashMap<(NamedOrBlankNodeRef<'_>, bool), Option<(usize, usize)>> =
        FxHashMap::default();
    let mut deepest = 0;
    for root in graph.iter().map(|triple| (triple.subject, false)) {
        if done.contains_key(&root) {
            continue;
        }
        done.insert(root, None);
        // a link is read again once the node it leads to is done
        let mut open = vec![(root, graph.triples_for_subject(root.0).peekable(), 0, 0)];
        while let Some((node, links, depth, steps)) = open.last_mut() {
            let Some(&link) = links.peek() else {
                let (node, depth, steps) = (*node, *depth, *steps);
                open.pop();
                if node.1 && steps > graph.len() {
                    return Err(RdfError::ShaclLimit(ShaclLimit::PathTooLarge));
                }
                deepest = deepest.max(depth);
                done.insert(node, Some((depth, steps)));
                continue;
            };
            let predicate = link.predicate.as_str();
            // the objects rudof parses as a path
            let path = node.1
                || [
                    ShaclVocab::SH_PATH,
                    ShaclVocab::SH_SUBSET_OF,
                    ShaclNodeExprVocab::SHNEX_PATH_VALUES,
                ]
                .contains(&predicate);
            let child = match link.object {
                TermRef::BlankNode(child) if !node.1 || PATH_LINKS.contains(&predicate) => {
                    Some((NamedOrBlankNodeRef::from(child), path))
                }
                // rudof reads an IRI list in a path as a list, every other IRI there as a predicate
                TermRef::NamedNode(child)
                    if !path
                        || [RdfVocab::RDF_REST, ShaclVocab::SH_ALTERNATIVE_PATH]
                            .contains(&predicate) =>
                {
                    Some((child.into(), path))
                }
                _ => None,
            };
            if let Some(child) = child {
                match done.get(&child) {
                    None => {
                        done.insert(child, None);
                        let links = graph.triples_for_subject(child.0).peekable();
                        open.push((child, links, 0, 0));
                        continue;
                    }
                    Some(None) if path => return Err(RdfError::ShaclLimit(ShaclLimit::CyclicPath)),
                    // shortcut: a memoised walk under-counts chains through a cycle of shapes, so
                    // these size by the triple count; find strongly connected components if that shows
                    Some(None) => deepest = deepest.max(graph.len()),
                    Some(Some((inner, inner_steps))) => {
                        *depth = (*depth).max(inner + 1);
                        if path {
                            *steps = steps.saturating_add(inner_steps + 1);
                        }
                    }
                }
            }
            links.next();
        }
    }
    Ok(deepest)
}

// rudof's schemas and reports nest as deep as the shapes, and so does dropping them
impl Drop for ShaclShapes {
    fn drop(&mut self) {
        let schema = mem::replace(&mut self.schema, IRSchema::new(PrefixMap::new()));
        let _ = on_stack("dropping SHACL shapes", self.depth, || {
            drop(schema);
            Ok(())
        });
    }
}

impl Drop for ShaclReport {
    fn drop(&mut self) {
        let report = mem::take(&mut self.0);
        let _ = on_stack("dropping a SHACL report", self.1, || {
            drop(report);
            Ok(())
        });
    }
}

/// The triples of a view as rudof reads them: each lookup is one [`matching`] scan.
struct Store {
    view: DynamicGraph,
    // the predicate of each layer, by `LayerId`
    predicates: Vec<NamedNode>,
}

impl Store {
    fn new(view: DynamicGraph) -> Self {
        let layers = view.edge_meta().layer_meta().all_keys();
        let predicates = layers.iter().map(|layer| iri_of(layer)).collect();
        Self { view, predicates }
    }

    fn node(&self, term: TermRef<'_>) -> Option<VID> {
        let name = named(term, true, name).ok()?;
        self.view.node(&*name).map(|node| node.node)
    }

    // `None` for a literal subject, which `to_rdf` skips too
    fn triple(&self, edge: EdgeView<DynamicGraph>) -> Option<Triple> {
        // shortcut: parses both node names per triple read, cache terms by VID if that shows in profiles
        let subject = NamedOrBlankNode::try_from(term_of(&edge.src().name())).ok()?;
        let predicate = match edge.edge.layer().and_then(|id| self.predicates.get(id.0)) {
            Some(predicate) => predicate.clone(),
            // a layer added during the validation
            None => iri_of(&edge.layer_name().ok()?),
        };
        Some(Triple::new(subject, predicate, term_of(&edge.dst().name())))
    }
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Store")
    }
}

impl Rdf for Store {
    type IRI = NamedNode;
    type BNode = BlankNode;
    type Literal = oxigraph::model::Literal;
    type Subject = NamedOrBlankNode;
    type Term = Term;
    type Triple = Triple;
    type Err = RdfError;

    fn qualify_iri(&self, iri: &NamedNode) -> String {
        iri.to_string()
    }

    fn qualify_subject(&self, subject: &NamedOrBlankNode) -> String {
        subject.to_string()
    }

    fn qualify_term(&self, term: &Term) -> String {
        term.to_string()
    }

    fn prefixmap(&self) -> Option<PrefixMap> {
        None
    }

    fn resolve_prefix_local(&self, prefix: &str, local: &str) -> Result<IriS, PrefixMapError> {
        PrefixMap::new().resolve_prefix_local(prefix, local)
    }
}

impl NeighsRDF for Store {
    fn triples(&self) -> Result<impl Iterator<Item = Triple>, RdfError> {
        self.triples_matching(&Any, &Any, &Any)
    }

    fn triples_matching<S, P, O>(
        &self,
        subject: &S,
        predicate: &P,
        object: &O,
    ) -> Result<impl Iterator<Item = Triple> + '_, RdfError>
    where
        S: Matcher<NamedOrBlankNode>,
        P: Matcher<NamedNode>,
        O: Matcher<Term>,
    {
        let (subject, predicate, object) = (subject.value(), predicate.value(), object.value());
        // with the class index off only SHACL 1.2 node expressions (`shnex:nodesMatching`) get here,
        // and they can recurse on each node without end
        if subject.is_none() && predicate.is_none() && object.is_none() {
            return Err(RdfError::shacl(
                "SHACL node expressions that read every triple are not supported",
            ));
        }
        // `None` if a bound term is not in the view, so nothing matches
        let bound = || {
            let subject = match subject {
                Some(subject) => Some(self.node(subject.into())?),
                None => None,
            };
            let layer = match predicate {
                Some(predicate) => Some(named(predicate.into(), true, layer).ok()?),
                None => None,
            };
            let object = match object {
                Some(object) => Some(self.node(object.as_ref())?),
                None => None,
            };
            Some(matching(&self.view, subject, layer.as_deref(), object))
        };
        Ok(bound()
            .into_iter()
            .flatten()
            .filter_map(move |edge| self.triple(edge)))
    }
}

impl ShaclProcessor<Store> for Store {
    fn store(&self) -> &Store {
        self
    }

    fn runner(_mode: &ShaclValidationMode, config: &ShaclConfig) -> Box<dyn Engine<Store>> {
        Box::new(NativeEngine::new(config.recursion_semantics()))
    }

    // the index reads every triple; without it `sh:targetClass` reads the `rdf:type` layer
    fn use_class_index(&self) -> bool {
        false
    }
}

impl ShaclReport {
    /// `sh:conforms`: whether there are no results, of any severity.
    pub fn conforms(&self) -> bool {
        self.0.conforms()
    }

    /// The results (`sh:result`), sorted.
    pub fn results(&self) -> Vec<ShaclResult> {
        on_stack("the SHACL report", self.1, || {
            let name = |object: &Object| term_name(Term::from(object.clone()).as_ref());
            let mut results: Vec<_> = self
                .0
                .results()
                .iter()
                .map(|result| {
                    let mut messages: Vec<String> = result
                        .message()
                        .iter()
                        .map(|(_, message)| message.clone())
                        .collect();
                    messages.sort();
                    ShaclResult {
                        focus_node: name(result.focus_node()),
                        constraint_component: name(result.constraint_component()),
                        path: result.path().map(|path| match path {
                            SHACLPath::Predicate { pred } => {
                                term_name(NamedNode::from(pred.clone()).as_ref().into())
                            }
                            path => path.to_string(),
                        }),
                        value: result.value().map(name),
                        source_shape: result.source().map(name),
                        severity: IriS::from(result.severity()).to_string(),
                        messages,
                    }
                })
                .collect();
            results.sort();
            Ok(results)
        })
        .unwrap_or_else(|error| panic!("{error}"))
    }

    /// Writes the report as an `sh:ValidationReport` graph and returns the number of triples.
    pub fn write<W: Write>(&self, writer: W, format: RdfFormat) -> Result<usize, RdfError> {
        // rudof labels its own blank nodes 1, 2, .. in hex, which can be data or shapes labels too
        let mark = |object: &Object| match object {
            Object::BlankNode(id) => Object::BlankNode(format!("r{id}")),
            object => object.clone(),
        };
        let graph = on_stack("the SHACL report", self.1, || {
            let results = self.0.results().iter().map(|result| {
                ValidationResult::new(
                    mark(result.focus_node()),
                    mark(result.constraint_component()),
                    result.severity().clone(),
                )
                .with_path(result.path().cloned())
                .with_value(result.value().map(mark))
                .with_source(result.source().map(mark))
                .with_message(result.message().clone())
            });
            let mut graph = OxigraphInMemory::new();
            ValidationReport::new()
                .with_conforms(self.conforms())
                .with_results(results.collect())
                .to_rdf(&mut graph)
                .map_err(RdfError::shacl)?;
            Ok(graph)
        })?;
        let mut own = FxHashMap::default();
        let mut unmark = |node: BlankNode| match node.as_str().strip_prefix('r') {
            Some(id) => BlankNode::new_unchecked(id),
            None => own.entry(node).or_insert_with(BlankNode::default).clone(),
        };
        let mut serializer = RdfSerializer::from_format(format.to_oxigraph()).for_writer(writer);
        for quad in graph.quads() {
            let subject = match quad.subject {
                NamedOrBlankNode::BlankNode(node) => unmark(node).into(),
                subject => subject,
            };
            let object = match quad.object {
                Term::BlankNode(node) => unmark(node).into(),
                object => object,
            };
            serializer.serialize_triple(TripleRef::new(&subject, &quad.predicate, &object))?;
        }
        serializer.finish()?;
        Ok(graph.len())
    }

    /// The report as a document, as [`write`](Self::write) writes it.
    pub fn to_rdf_string(&self, format: RdfFormat) -> Result<String, RdfError> {
        let mut document = Vec::new();
        self.write(&mut document, format)?;
        Ok(String::from_utf8(document)?)
    }
}
