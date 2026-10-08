//! SHACL validation of graph views (feature `shacl`), with the native engine of rudof's
//! [`shacl`](https://crates.io/crates/shacl) crate.
//!
//! A [`ShaclShapes`] is a compiled SHACL shapes graph. [`ShaclShapes::validate`] (or
//! [`RdfViewOps::validate_shacl`]) checks the triples of a view against it and returns a
//! [`ShaclReport`]: the `sh:ValidationReport` of the W3C SHACL recommendation, as Rust values,
//! which [`ShaclReport::write`] serialises as RDF.
//!
//! # What SHACL sees
//!
//! The data graph is exactly the triples [`to_rdf`](crate::rdf::RdfViewOps::to_rdf) writes and
//! [`sparql`](crate::rdf::RdfViewOps::sparql) matches: one triple per `(edge, layer)` pair
//! visible in `view.valid()`, with names mapped to terms by [`term_of`]. Generalized triples (a
//! literal subject, or a layer whose term is not an IRI) are left out. It is read from the graph
//! as the validator asks for it, without a copy.
//!
//! So time travel is picking the view: `pg.snapshot_at(t)` validates the state as of `t`, and
//! [`ShaclShapes::validate_at`] validates several times. On an event [`Graph`] retractions are
//! ignored (use `persistent_graph()`). As with `sparql`, validation does not see a snapshot of a
//! graph that other threads write meanwhile: validate a `read_only()` view for a consistent
//! result.
//!
//! # Supported SHACL
//!
//! - All SHACL Core constraint components, `sh:targetNode`, `sh:targetClass`,
//!   `sh:targetSubjectsOf`, `sh:targetObjectsOf` and implicit class targets, `sh:closed` and
//!   `sh:ignoredProperties`, `sh:deactivated`, `sh:severity` and `sh:message`, and recursive
//!   shapes (with the least-fixpoint semantics of the validator, see below).
//! - Shapes are checked when they are compiled, and fail with [`RdfError::ShaclUnsupported`]
//!   instead of being silently ignored or misread by the validator, if they use:
//!   - SHACL-SPARQL (`sh:sparql`, `sh:select`, `sh:ask`, `sh:SPARQLConstraint`,
//!     `sh:ConstraintComponent`, ...), SHACL Advanced Features (`sh:rule`, `sh:target`,
//!     `sh:expression`, `sh:values`, `sh:filterShape`, ...), SHACL-JS or `sh:entailment`;
//!   - SHACL 1.2 features other than reifier shapes, such as `sh:singleLine`,
//!     `sh:minListLength`, `sh:memberShape`, `sh:rootClass`, `sh:targetWhere`,
//!     `sh:closed sh:ByTypes` or `sh:ShapeClass`. Other unknown terms of the `sh:` namespace
//!     are ignored, as SHACL requires (so a misspelt term is ignored too);
//!   - an `sh:inversePath` of anything but a predicate, such as `^(ex:p / ex:q)`, which the
//!     validator cannot evaluate. Write it with inverses of predicates instead:
//!     `^(ex:p / ex:q)` is `(^ex:q / ^ex:p)`, `^(ex:p | ex:q)` is `(^ex:p | ^ex:q)` and
//!     `^(ex:p*)` is `(^ex:p)*`;
//!   - a literal of `sh:hasValue` or `sh:in` that the validator rewrites (see below).
//! - Shapes fail with [`RdfError::Shacl`] if an RDF list they use has more than
//!   [`MAX_LIST_LENGTH`] members or is cyclic, if shapes and paths nest more than
//!   [`MAX_NESTING`] deep, or if a path contains itself, since the validator reads them
//!   recursively and a stack overflow would abort the process.
//! - `owl:imports` and `sh:shapesGraph` are not followed: the shapes graph is the document given.
//!
//! The W3C SHACL test suite (`raphtory-rdf-tests/tests/shacl_w3c.rs`) runs with
//! `make w3c-tests-init && make rust-test-shacl-w3c`.
//!
//! # Deviations of the validator
//!
//! The rudof validator does not follow `rdfs:subClassOf` as SHACL requires:
//! 1. `sh:targetClass C` targets only the nodes whose `rdf:type` is `C` itself, not those of a
//!    subclass of `C`;
//! 2. an implicit class target (a shape that is also an `rdfs:Class`) follows one
//!    `rdfs:subClassOf` step;
//! 3. `sh:class C` accepts a value typed `C` or a direct subclass of `C`, so a value two
//!    subclass steps below `C` is reported.
//!
//! When the shapes use class targets or `sh:class` and the view has an `rdfs:subClassOf`
//! triple, the report has a warning ([`SUBCLASS_WARNING`]).
//!
//! Recursive shapes (a shape that refers to itself through `sh:node`, `sh:property`, `sh:not`,
//! `sh:and`, `sh:or`, `sh:xone` or `sh:qualifiedValueShape`) have the least-fixpoint semantics
//! of the validator: a node whose conformance depends on itself through a cycle of the data does
//! not conform (e.g. two people who `ex:knows` each other under a self-referencing shape),
//! where other processors accept it (SHACL 1.0 leaves recursion undefined).
//! A report of recursive shapes that has results has a warning ([`RECURSION_WARNING`]). The
//! validator recurses once per node, so a very long chain of mutually dependent nodes overflows
//! the stack and aborts the process.
//!
//! The validator reads literals of some datatypes as values, and writes them back in a canonical
//! form: booleans (`"1"^^xsd:boolean` is `"true"`), date-times (`"2020-01-01T00:00:00.000Z"` and
//! `"2020-01-01T00:00:00+00:00"` are `"2020-01-01T00:00:00Z"`), the integer types `xsd:long`,
//! `xsd:short`, `xsd:byte`, their unsigned types and `xsd:nonNegativeInteger`,
//! `xsd:positiveInteger`, `xsd:negativeInteger` and `xsd:nonPositiveInteger`
//! (`"042"^^xsd:long` is `"42"^^xsd:long`, and `"05"^^xsd:short` even becomes
//! `"5"^^xsd:integer`), and language tags with a region (`"colour"@en-gb` is
//! `"colour"@en-GB`). Literals of `xsd:integer`, `xsd:int`, `xsd:decimal`, `xsd:double`,
//! `xsd:float`, strings, and of other datatypes keep their form. So:
//! - the focus nodes and values of results are given back as the literals of the view when that
//!   is unambiguous: when one literal of the view has the canonical form, and the canonical form
//!   is not itself a node. Otherwise they are the canonical form, which [`name_of`] may not map
//!   to a node;
//! - `sh:hasValue` and `sh:in` compare terms, so a literal of theirs that is not in canonical
//!   form would never match the data: such shapes are rejected (see above);
//! - a literal of `sh:targetNode` is not rejected: one that is not in canonical form is looked
//!   up as written (unless its canonical form is itself a node, or another `sh:targetNode`
//!   literal has the same canonical form), and is given back as written in results;
//! - `sh:disjoint` compares canonical forms, so it takes `"1"^^xsd:boolean` and
//!   `"true"^^xsd:boolean` to be the same value.
//!
//! Apart from these, the results are those of SHACL.
//!
//! # Example
//!
//! ```
//! use raphtory::{
//!     prelude::*,
//!     rdf::{shacl::ShaclShapes, RdfFormat},
//! };
//!
//! let shapes = ShaclShapes::parse(
//!     r#"
//!     @prefix sh: <http://www.w3.org/ns/shacl#> .
//!     @prefix ex: <http://ex/> .
//!     ex:PersonShape a sh:NodeShape ;
//!         sh:targetClass ex:Person ;
//!         sh:property [ sh:path ex:name ; sh:minCount 1 ] .
//!     "#
//!     .as_bytes(),
//!     RdfFormat::Turtle,
//!     None,
//! )
//! .unwrap();
//!
//! let pg = PersistentGraph::new();
//! let doc = r#"
//!     @prefix ex: <http://ex/> .
//!     ex:alice a ex:Person ; ex:name "Alice" .
//! "#;
//! pg.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None).unwrap();
//! pg.retract_rdf(
//!     5,
//!     r#"<http://ex/alice> <http://ex/name> "Alice" ."#.as_bytes(),
//!     RdfFormat::NTriples,
//!     None,
//! )
//! .unwrap();
//!
//! assert!(pg.validate_shacl(&shapes).unwrap().results.len() == 1);
//! // as of t = 3 alice had a name
//! assert!(pg.snapshot_at(3).validate_shacl(&shapes).unwrap().conforms);
//!
//! // when was the data valid?
//! let reports = shapes.validate_at(&pg, [1, 4, 5, 9]).unwrap();
//! let conforms: Vec<(i64, bool)> = reports.iter().map(|(t, r)| (*t, r.conforms)).collect();
//! assert_eq!(conforms, [(1, true), (4, true), (5, false), (9, false)]);
//!
//! let result = &pg.validate_shacl(&shapes).unwrap().results[0];
//! assert_eq!(result.focus_node.to_string(), "<http://ex/alice>");
//! assert_eq!(
//!     result.constraint_component.as_str(),
//!     "http://www.w3.org/ns/shacl#MinCountConstraintComponent"
//! );
//! assert_eq!(result.path.as_ref().unwrap().to_string(), "<http://ex/name>");
//! ```
use crate::{
    db::api::view::{internal::CoreGraphOps, DynamicGraph, IntoDynamic, StaticGraphViewOps},
    errors::GraphError,
    prelude::*,
    rdf::{
        dataset::lookup_node,
        export::{xml_text_safe, TripleSink},
        mapping::{layer_predicate, name_of, term_of},
        scan::{view_layers, Dir, EdgeScan},
        RdfError, RdfExportStats,
    },
};
use dashmap::{mapref::entry::Entry, DashMap};
use indexmap::IndexMap;
use oxigraph::{
    io::{RdfFormat, RdfParser, RdfSerializer},
    model::{
        vocab::{rdf, rdfs, xsd},
        BlankNode, BlankNodeRef, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode,
        NamedOrBlankNodeRef, Term, TermRef, Triple, TripleRef,
    },
};
use prefixmap::{PrefixMap, PrefixMapError};
use raphtory_api::core::{
    entities::{LayerId, VID},
    storage::timeindex::AsTime,
    utils::time::TryIntoTime,
};
use rayon::{ThreadPool, ThreadPoolBuilder};
use rudof_iri::IriS;
use rudof_rdf::{
    rdf_core::{term::Object, Matcher, NeighsRDF, RDFFormat, Rdf, SHACLPath},
    rdf_impl::ReaderMode,
};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet, FxHasher};
use shacl::{
    ir::IRSchema,
    validator::{
        engine::{Engine, NativeEngine},
        processor::ShaclProcessor,
        report::ValidationReport,
        ShaclConfig, ShaclValidationMode,
    },
};
use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    io::{Read, Write},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, OnceLock},
};

/// The SHACL namespace.
const SH: &str = "http://www.w3.org/ns/shacl#";

/// `owl:Class`, which makes a shape an implicit class target as `rdfs:Class` does.
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";

/// The stack size of the threads that compile shapes and validate (see [`run_guarded`]).
const STACK_SIZE: usize = 64 << 20;

/// The most node terms a validation keeps in memory (see [`ShaclStore::node_term`]), and the
/// most rewritten literals it remembers (see [`ShaclStore::originals`]).
const TERM_CACHE_CAP: usize = 1 << 20;

/// The most members an RDF list of a shapes graph can have, such as the values of `sh:in`.
///
/// The validator reads lists recursively, so shapes with a longer list fail with
/// [`RdfError::Shacl`] rather than risk a stack overflow. Check membership in a longer list of
/// values with `sh:pattern` or a SPARQL query.
pub const MAX_LIST_LENGTH: usize = 10_000;

/// How deep the shapes and paths of a shapes graph can nest: a shape is one deeper than the
/// shapes it refers to (by `sh:node`, `sh:property`, `sh:not`, `sh:and`, `sh:or`, `sh:xone`,
/// `sh:qualifiedValueShape` or `sh:reifierShape`), and as deep as its paths; a path node is one
/// deeper than the paths it is made of. A shape that refers to itself (a recursive shape) is not
/// counted again.
///
/// The validator reads them recursively, so shapes that nest deeper fail with
/// [`RdfError::Shacl`] rather than risk a stack overflow.
pub const MAX_NESTING: usize = 256;

/// The warning of a report on a view with `rdfs:subClassOf` triples, validated with shapes that
/// use class targets or `sh:class` (see the [module documentation](self)).
pub const SUBCLASS_WARNING: &str = "the data has rdfs:subClassOf triples, but the validator \
    follows them only partly: sh:targetClass targets direct rdf:type instances only, and implicit \
    class targets and sh:class follow a single rdfs:subClassOf step, so results can differ from \
    those of a SHACL processor with full subclass reasoning";

/// The warning of a report that has results, validated with recursive shapes (see the
/// [module documentation](self)).
pub const RECURSION_WARNING: &str = "the shapes are recursive, and the validator gives \
    recursion least-fixpoint semantics (which SHACL 1.0 leaves undefined): a node whose \
    conformance depends on itself through a cycle in the data is reported as not conforming, \
    where other SHACL processors may accept it";

const SHACL_SPARQL: &str = "SHACL-SPARQL is not supported";
const SHACL_AF: &str = "SHACL Advanced Features are not supported";
const SHACL_JS: &str = "SHACL-JS is not supported";
const SHACL_12: &str = "SHACL 1.2 features are not supported";
const COMPLEX_INVERSE: &str = "the validator only supports the inverse of a predicate: write \
    ^(p / q) as (^q / ^p), ^(p | q) as (^p | ^q) and ^(p*) as (^p)*";

/// Why a predicate of the SHACL namespace is rejected, or `None`. `local` is the part after
/// [`SH`].
///
/// The validator silently ignores terms outside SHACL Core and 1.2 reifier shapes, so known
/// terms of other SHACL parts are rejected; unknown terms are ignored, as SHACL requires.
fn unsupported_predicate(local: &str) -> Option<&'static str> {
    match local {
        "sparql" | "select" | "ask" | "construct" | "update" | "validator" | "nodeValidator"
        | "propertyValidator" | "parameter" | "optional" | "labelTemplate" => Some(SHACL_SPARQL),
        // rules, custom targets, functions and node expressions
        "rule" | "target" | "expression" | "filterShape" | "values" | "condition" | "subject"
        | "predicate" | "object" | "nodes" | "intersection" | "union" | "returnType" | "count"
        | "min" | "max" | "sum" | "distinct" | "exists" | "if" | "then" | "else" | "orderBy"
        | "desc" | "limit" | "offset" | "groupConcat" | "separator" | "minus" | "flatMap"
        | "findFirst" | "matchAll" => Some(SHACL_AF),
        "entailment" => {
            Some("entailment regimes are not supported: validation sees the stored triples only")
        }
        "singleLine" | "rootClass" | "memberShape" | "minListLength" | "maxListLength"
        | "uniqueMembers" | "nodeByExpression" | "targetWhere" | "someValue" => Some(SHACL_12),
        _ if local.starts_with("js") => Some(SHACL_JS),
        _ => None,
    }
}

/// Why a class of the SHACL namespace (an `rdf:type` object in the shapes graph) is rejected,
/// or `None` (see [`unsupported_predicate`]).
fn unsupported_class(local: &str) -> Option<&'static str> {
    match local {
        "SPARQLConstraint"
        | "SPARQLConstraintComponent"
        | "SPARQLTarget"
        | "SPARQLTargetType"
        | "ConstraintComponent"
        | "Parameter"
        | "Parameterizable"
        | "Function"
        | "Validator"
        | "SPARQLFunction"
        | "SPARQLAskValidator"
        | "SPARQLSelectValidator"
        | "SPARQLExecutable"
        | "SPARQLAskExecutable"
        | "SPARQLSelectExecutable"
        | "SPARQLConstructExecutable"
        | "SPARQLUpdateExecutable" => Some(SHACL_SPARQL),
        "SPARQLRule" | "TripleRule" | "Rule" | "Target" | "TargetType" => Some(SHACL_AF),
        "ShapeClass" => Some(SHACL_12),
        _ if local.starts_with("JS") => Some(SHACL_JS),
        _ => None,
    }
}

/// A parsed and compiled SHACL shapes graph. Cheap to clone, and `Send + Sync`, so one
/// [`ShaclShapes`] can validate many views, from many threads.
#[derive(Clone)]
pub struct ShaclShapes {
    schema: Arc<IRSchema>,
    /// Whether the shapes use class targets or `sh:class`, which follow `rdfs:subClassOf` only
    /// partly (see [`SUBCLASS_WARNING`]).
    uses_classes: bool,
    /// Whether a shape refers to itself (see [`RECURSION_WARNING`]).
    recursive: bool,
    /// The literals of `sh:targetNode` the validator rewrites (see
    /// [`ShapesGraph::rewritten_targets`]).
    targets: Arc<FxHashMap<Term, Term>>,
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ShaclShapes>();
};

impl fmt::Debug for ShaclShapes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShaclShapes")
            .field("uses_classes", &self.uses_classes)
            .field("recursive", &self.recursive)
            .finish_non_exhaustive()
    }
}

/// A SHACL property path, as in `sh:path` and `sh:resultPath`.
///
/// It displays in SPARQL 1.1 property path syntax, such as `^<http://ex/knows>` or
/// `(<http://ex/worksFor> / <http://ex/name>)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShaclPath {
    /// A predicate IRI.
    Predicate(NamedNode),
    /// A sequence path: the paths one after the other.
    Sequence(Vec<ShaclPath>),
    /// An alternative path (`sh:alternativePath`): any one of the paths.
    Alternative(Vec<ShaclPath>),
    /// An inverse path (`sh:inversePath`).
    Inverse(Box<ShaclPath>),
    /// `sh:zeroOrMorePath`.
    ZeroOrMore(Box<ShaclPath>),
    /// `sh:oneOrMorePath`.
    OneOrMore(Box<ShaclPath>),
    /// `sh:zeroOrOnePath`.
    ZeroOrOne(Box<ShaclPath>),
}

impl From<&SHACLPath> for ShaclPath {
    fn from(path: &SHACLPath) -> Self {
        let inner = |path: &SHACLPath| Box::new(ShaclPath::from(path));
        match path {
            SHACLPath::Predicate { pred } => Self::Predicate(NamedNode::from(pred.clone())),
            SHACLPath::Sequence { paths } => Self::Sequence(paths.iter().map(Into::into).collect()),
            SHACLPath::Alternative { paths } => {
                Self::Alternative(paths.iter().map(Into::into).collect())
            }
            SHACLPath::Inverse { path } => Self::Inverse(inner(path)),
            SHACLPath::ZeroOrMore { path } => Self::ZeroOrMore(inner(path)),
            SHACLPath::OneOrMore { path } => Self::OneOrMore(inner(path)),
            SHACLPath::ZeroOrOne { path } => Self::ZeroOrOne(inner(path)),
        }
    }
}

impl ShaclPath {
    /// Writes the path as an operand of a unary operator: in brackets unless it is a predicate
    /// or already bracketed.
    fn fmt_operand(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Predicate(_) | Self::Sequence(_) | Self::Alternative(_) => write!(f, "{self}"),
            _ => write!(f, "({self})"),
        }
    }
}

impl fmt::Display for ShaclPath {
    /// SPARQL 1.1 property path syntax.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let join = |f: &mut fmt::Formatter<'_>, paths: &[ShaclPath], separator: &str| {
            f.write_str("(")?;
            for (i, path) in paths.iter().enumerate() {
                if i > 0 {
                    f.write_str(separator)?;
                }
                write!(f, "{path}")?;
            }
            f.write_str(")")
        };
        match self {
            Self::Predicate(p) => write!(f, "{p}"),
            Self::Sequence(paths) => join(f, paths, " / "),
            Self::Alternative(paths) => join(f, paths, " | "),
            Self::Inverse(path) => {
                f.write_str("^")?;
                path.fmt_operand(f)
            }
            Self::ZeroOrMore(path) => {
                path.fmt_operand(f)?;
                f.write_str("*")
            }
            Self::OneOrMore(path) => {
                path.fmt_operand(f)?;
                f.write_str("+")
            }
            Self::ZeroOrOne(path) => {
                path.fmt_operand(f)?;
                f.write_str("?")
            }
        }
    }
}

/// One `sh:ValidationResult` of a [`ShaclReport`]. Terms are RDF terms, as in
/// [`to_rdf`](crate::rdf::RdfViewOps::to_rdf): use [`name_of`] to get the Raphtory name of a
/// node.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShaclResult {
    /// `sh:focusNode`: the node that does not conform.
    pub focus_node: Term,
    /// `sh:resultPath`: the path of the property shape, if the result comes from one.
    pub path: Option<ShaclPath>,
    /// `sh:value`: the value that violates the constraint, if there is one.
    pub value: Option<Term>,
    /// `sh:sourceShape`: the shape whose constraint is violated.
    pub source_shape: Option<Term>,
    /// `sh:sourceConstraintComponent`, such as `sh:MinCountConstraintComponent`.
    pub constraint_component: NamedNode,
    /// `sh:resultSeverity`: `sh:Violation` unless the shape sets `sh:severity`.
    pub severity: NamedNode,
    /// `sh:resultMessage`: the messages of the shape (`sh:message`) or, if it has none, a
    /// message of the validator (such as `MinCount(1) not satisfied`), sorted.
    pub messages: Vec<Literal>,
}

/// A SHACL `sh:ValidationReport`: the result of [`ShaclShapes::validate`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShaclReport {
    /// `sh:conforms`: whether there are no results, of any severity.
    pub conforms: bool,
    /// `sh:result`: the validation results, in a deterministic order (by focus node, then
    /// constraint component, path, value and source shape).
    pub results: Vec<ShaclResult>,
    /// Why the results may not be those of SHACL (not part of the W3C report):
    /// [`SUBCLASS_WARNING`], when the shapes use class targets or `sh:class` and the view has an
    /// `rdfs:subClassOf` triple, and [`RECURSION_WARNING`], when the shapes are recursive and
    /// there are results.
    pub warnings: Vec<String>,
}

impl ShaclShapes {
    /// Parses and compiles a shapes graph from an RDF document.
    ///
    /// The blank nodes of the document get labels that depend only on the document (by order
    /// of first appearance), so the results of the same document, such as a blank-node
    /// `sh:sourceShape`, are the same each time it is parsed.
    ///
    /// Fails with [`RdfError::Parse`] if the document does not parse (named graphs are
    /// rejected), [`RdfError::ShaclUnsupported`] if it uses a SHACL feature the validator does
    /// not evaluate (see the [module documentation](self)), and [`RdfError::Shacl`] if it is not a
    /// valid shapes graph or exceeds the limits of the validator ([`MAX_LIST_LENGTH`],
    /// [`MAX_NESTING`]).
    pub fn parse(
        data: impl Read,
        format: RdfFormat,
        base_iri: Option<&str>,
    ) -> Result<Self, GraphError> {
        let mut parser = RdfParser::from_format(format).without_named_graphs();
        if let Some(base_iri) = base_iri {
            parser = parser.with_base_iri(base_iri).map_err(RdfError::from)?;
        }
        let triples = parser
            .for_reader(data)
            .map(|quad| quad.map(Triple::from))
            .collect::<Result<Vec<_>, _>>()
            .map_err(RdfError::from)?;
        let triples = relabel_blank_nodes(triples);
        Self::from_triples(triples.iter().map(Triple::as_ref))
    }

    /// Compiles a shapes graph from its triples. Blank-node labels are kept as given. Fails like
    /// [`parse`](Self::parse).
    pub fn from_triples<'a>(
        triples: impl IntoIterator<Item = TripleRef<'a>>,
    ) -> Result<Self, GraphError> {
        let triples: Vec<TripleRef<'a>> = triples.into_iter().collect();
        check_supported(&triples)?;
        let graph = ShapesGraph::new(&triples);
        let recursive = graph.check_structure()?;
        graph.check_literals()?;
        let targets = Arc::new(graph.rewritten_targets());
        let uses_classes = uses_classes(&triples);
        // the validator reads its shapes graph from a document: N-Triples keeps the blank-node
        // labels as given
        let mut document = Vec::new();
        let mut serializer =
            RdfSerializer::from_format(RdfFormat::NTriples).for_writer(&mut document);
        for triple in &triples {
            serializer
                .serialize_triple(*triple)
                .map_err(|error| RdfError::Shacl(error.to_string()))?;
        }
        serializer
            .finish()
            .map_err(|error| RdfError::Shacl(error.to_string()))?;
        let schema = run_guarded("the SHACL shapes compiler", move || {
            IRSchema::from_reader(
                &mut document.as_slice(),
                "shapes",
                &RDFFormat::NTriples,
                None,
                &ReaderMode::Strict,
            )
            .map_err(|error| RdfError::Shacl(error.to_string()))
        })?;
        Ok(Self {
            schema: Arc::new(schema),
            uses_classes,
            recursive,
            targets,
        })
    }

    /// Validates the triples of `view` (the triples [`to_rdf`](crate::rdf::RdfViewOps::to_rdf)
    /// writes; see the [module documentation](self)).
    ///
    /// Runs on a shared pool of large-stack threads while the caller waits; a validator panic
    /// becomes [`RdfError::Shacl`].
    pub fn validate<G: StaticGraphViewOps + IntoDynamic>(
        &self,
        view: &G,
    ) -> Result<ShaclReport, GraphError> {
        let view = view.clone().into_dynamic();
        let schema = self.schema.clone();
        let (uses_classes, recursive) = (self.uses_classes, self.recursive);
        let targets = self.targets.clone();
        let report = run_guarded("the SHACL validator", move || {
            let mut processor = Proc(ShaclStore::new(view).with_targets(targets));
            let mut warnings = Vec::new();
            if uses_classes && processor.0.has_subclass_triples() {
                warnings.push(SUBCLASS_WARNING.to_owned());
            }
            let report = processor
                .validate(
                    &schema,
                    &ShaclValidationMode::Native,
                    &ShaclConfig::default(),
                )
                .map_err(|error| RdfError::Shacl(error.to_string()))?;
            if recursive && !report.conforms() {
                warnings.push(RECURSION_WARNING.to_owned());
            }
            convert(&report, warnings, &processor.0)
        })?;
        Ok(report)
    }

    /// Validates `view.snapshot_at(t)` for every time `t` of `times`, in order, and returns each
    /// time (in milliseconds) with its report.
    ///
    /// On a [`PersistentGraph`] that is the state as of each time, so this tells when the data
    /// started or stopped conforming. On an event [`Graph`] a snapshot holds every triple
    /// asserted up to the time.
    pub fn validate_at<G: StaticGraphViewOps + IntoDynamic, T: TryIntoTime>(
        &self,
        view: &G,
        times: impl IntoIterator<Item = T>,
    ) -> Result<Vec<(i64, ShaclReport)>, GraphError> {
        times
            .into_iter()
            .map(|time| {
                let t = time.try_into_time()?.t();
                Ok((t, self.validate(&view.snapshot_at(t))?))
            })
            .collect()
    }
}

/// Relabels blank nodes deterministically (document hash plus order of first appearance), as
/// the parser gives anonymous blank nodes random labels.
fn relabel_blank_nodes(triples: Vec<Triple>) -> Vec<Triple> {
    let mut indexes: FxHashMap<BlankNode, usize> = FxHashMap::default();
    let mut hasher = FxHasher::default();
    for triple in &triples {
        let subject: TermRef<'_> = triple.subject.as_ref().into();
        for term in [
            subject,
            triple.predicate.as_ref().into(),
            triple.object.as_ref(),
        ] {
            match term {
                TermRef::BlankNode(node) => {
                    let next = indexes.len();
                    let index = *indexes.entry(node.into_owned()).or_insert(next);
                    (0u8, index).hash(&mut hasher);
                }
                term => (1u8, term).hash(&mut hasher),
            }
        }
    }
    if indexes.is_empty() {
        return triples;
    }
    let document = hasher.finish() >> 32;
    let relabel =
        |node: &BlankNode| BlankNode::new_unchecked(format!("s{document:08x}b{}", indexes[node]));
    triples
        .into_iter()
        .map(|triple| {
            let subject = match triple.subject {
                NamedOrBlankNode::BlankNode(node) => relabel(&node).into(),
                subject => subject,
            };
            let object = match triple.object {
                Term::BlankNode(node) => relabel(&node).into(),
                object => object,
            };
            Triple::new(subject, triple.predicate, object)
        })
        .collect()
}

/// Fails with [`RdfError::ShaclUnsupported`] if a shapes graph uses a feature the validator
/// would silently ignore (see [`unsupported_predicate`]).
fn check_supported(triples: &[TripleRef<'_>]) -> Result<(), RdfError> {
    let unsupported = |local: &str, reason: &str| {
        Err(RdfError::ShaclUnsupported(format!("sh:{local} ({reason})")))
    };
    for triple in triples {
        if let Some(local) = triple.predicate.as_str().strip_prefix(SH) {
            if let Some(reason) = unsupported_predicate(local) {
                return unsupported(local, reason);
            }
            // SHACL 1.2 closes a shape by the properties of its types with `sh:ByTypes`
            let by_types = matches!(triple.object, TermRef::NamedNode(object)
                if object.as_str().strip_prefix(SH) == Some("ByTypes"));
            if local == "closed" && by_types {
                return unsupported(local, SHACL_12);
            }
        }
        if triple.predicate == rdf::TYPE {
            if let TermRef::NamedNode(class) = triple.object {
                if let Some(local) = class.as_str().strip_prefix(SH) {
                    if let Some(reason) = unsupported_class(local) {
                        return unsupported(local, reason);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Whether a shapes graph uses `sh:targetClass`, `sh:class` or implicit class targets (a shape
/// that is also an `rdfs:Class` or `owl:Class`).
fn uses_classes(triples: &[TripleRef<'_>]) -> bool {
    let sh = |local: &str| format!("{SH}{local}");
    let (target_class, class) = (sh("targetClass"), sh("class"));
    let (node_shape, property_shape) = (sh("NodeShape"), sh("PropertyShape"));
    let mut shapes = FxHashSet::default();
    let mut classes = FxHashSet::default();
    for triple in triples {
        let predicate = triple.predicate.as_str();
        if predicate == target_class || predicate == class {
            return true;
        }
        if triple.predicate == rdf::TYPE {
            if let TermRef::NamedNode(object) = triple.object {
                let object = object.as_str();
                if object == node_shape || object == property_shape {
                    shapes.insert(triple.subject);
                } else if object == rdfs::CLASS.as_str() || object == OWL_CLASS {
                    classes.insert(triple.subject);
                }
            }
        }
    }
    !shapes.is_disjoint(&classes)
}

/// The literal the validator makes of a term, if it is not the term itself: the validator reads
/// literals of some datatypes as values and writes them back in canonical form (see the
/// [module documentation](self)).
fn canonical_form(term: &Term) -> Option<Term> {
    let Term::Literal(literal) = term else {
        return None;
    };
    // the validator keeps simple literals as they are
    if literal.datatype() == xsd::STRING {
        return None;
    }
    let canonical = Term::from(Object::try_from(term.clone()).ok()?);
    (canonical != *term).then_some(canonical)
}

/// The node of a term that can be a subject.
fn as_node(term: TermRef<'_>) -> Option<NamedOrBlankNodeRef<'_>> {
    match term {
        TermRef::NamedNode(node) => Some(node.into()),
        TermRef::BlankNode(node) => Some(node.into()),
        _ => None,
    }
}

/// The triples of a shapes graph by subject, to check what the validator reads of it
/// recursively before it compiles them: the shapes, the shapes they refer to, their paths, and
/// the RDF lists of these. Every check is iterative, and reads them as the validator does.
struct ShapesGraph<'a> {
    properties: FxHashMap<NamedOrBlankNodeRef<'a>, Vec<(NamedNodeRef<'a>, TermRef<'a>)>>,
}

fn cyclic_path() -> RdfError {
    RdfError::Shacl("the shapes graph has a path that contains itself".to_owned())
}

fn too_deep() -> RdfError {
    RdfError::Shacl(format!(
        "the shapes graph nests shapes and paths more than {MAX_NESTING} deep, the most the \
         validator accepts"
    ))
}

impl<'a> ShapesGraph<'a> {
    fn new(triples: &[TripleRef<'a>]) -> Self {
        let mut properties: FxHashMap<_, Vec<_>> = FxHashMap::default();
        for triple in triples {
            properties
                .entry(triple.subject)
                .or_default()
                .push((triple.predicate, triple.object));
        }
        Self { properties }
    }

    /// The predicates and objects of the triples of `node`.
    fn properties(&self, node: NamedOrBlankNodeRef<'a>) -> &[(NamedNodeRef<'a>, TermRef<'a>)] {
        self.properties.get(&node).map_or(&[], Vec::as_slice)
    }

    /// The objects of `predicate` for `node`.
    fn objects(
        &self,
        node: NamedOrBlankNodeRef<'a>,
        predicate: NamedNodeRef<'a>,
    ) -> impl Iterator<Item = TermRef<'a>> + '_ {
        self.properties(node)
            .iter()
            .filter(move |(p, _)| *p == predicate)
            .map(|(_, object)| *object)
    }

    /// The object of `sh:{local}` for `node`, if it has exactly one, as the validator reads
    /// the parts of a path.
    fn single(&self, node: NamedOrBlankNodeRef<'a>, local: &str) -> Option<TermRef<'a>> {
        let mut objects = self
            .properties(node)
            .iter()
            .filter(|(predicate, _)| predicate.as_str().strip_prefix(SH) == Some(local))
            .map(|(_, object)| *object);
        let object = objects.next()?;
        objects.next().is_none().then_some(object)
    }

    /// The members (`rdf:first`) of the RDF list at `head`. Fails with [`RdfError::Shacl`] if
    /// its `rdf:rest` links lead back to it, or it has more than [`MAX_LIST_LENGTH`] members.
    fn list(&self, head: TermRef<'a>) -> Result<Vec<TermRef<'a>>, RdfError> {
        let mut members = Vec::new();
        let mut seen = FxHashSet::default();
        let mut node = as_node(head);
        while let Some(current) = node {
            if !seen.insert(current) {
                return Err(RdfError::Shacl(
                    "the shapes graph has a cyclic RDF list (its rdf:rest links lead back to it)"
                        .to_owned(),
                ));
            }
            members.extend(self.objects(current, rdf::FIRST).take(1));
            if members.len() > MAX_LIST_LENGTH {
                return Err(RdfError::Shacl(format!(
                    "the shapes graph has an RDF list of more than {MAX_LIST_LENGTH} members, \
                     the most the validator accepts"
                )));
            }
            node = self.objects(current, rdf::REST).next().and_then(as_node);
        }
        Ok(members)
    }

    /// The paths a path node (a blank node) is made of, read as the validator does: an RDF list
    /// (a sequence path), or else the path of `sh:alternativePath` (a list),
    /// `sh:zeroOrMorePath`, `sh:oneOrMorePath`, `sh:zeroOrOnePath` or `sh:inversePath`, the
    /// first that it has exactly one of. Fails with [`RdfError::ShaclUnsupported`] for the
    /// inverse of anything but a predicate, on which the validator panics.
    fn path_parts(&self, node: BlankNodeRef<'a>) -> Result<Vec<TermRef<'a>>, RdfError> {
        let node = NamedOrBlankNodeRef::from(node);
        if self.objects(node, rdf::FIRST).next().is_some() {
            return self.list(node.into());
        }
        if let Some(list) = self.single(node, "alternativePath") {
            return self.list(list);
        }
        for local in ["zeroOrMorePath", "oneOrMorePath", "zeroOrOnePath"] {
            if let Some(path) = self.single(node, local) {
                return Ok(vec![path]);
            }
        }
        match self.single(node, "inversePath") {
            Some(path) if !path.is_named_node() => Err(RdfError::ShaclUnsupported(format!(
                "sh:inversePath ({COMPLEX_INVERSE})"
            ))),
            _ => Ok(Vec::new()),
        }
    }

    /// How deep the path `path` nests: 0 for a predicate, and one more than its deepest part
    /// for a path node. `depths` holds the depths of the path nodes checked so far.
    fn path_depth(
        &self,
        path: TermRef<'a>,
        depths: &mut FxHashMap<BlankNodeRef<'a>, usize>,
    ) -> Result<usize, RdfError> {
        struct Frame<'a> {
            node: BlankNodeRef<'a>,
            parts: Vec<TermRef<'a>>,
            next: usize,
            depth: usize,
        }
        let TermRef::BlankNode(root) = path else {
            return Ok(0);
        };
        if let Some(&depth) = depths.get(&root) {
            return Ok(depth);
        }
        // the path nodes on the stack
        let mut open = FxHashSet::default();
        open.insert(root);
        let mut stack = vec![Frame {
            node: root,
            parts: self.path_parts(root)?,
            next: 0,
            depth: 1,
        }];
        while let Some(top) = stack.last_mut() {
            if let Some(&part) = top.parts.get(top.next) {
                top.next += 1;
                let TermRef::BlankNode(child) = part else {
                    continue;
                };
                if open.contains(&child) {
                    return Err(cyclic_path());
                }
                if let Some(&depth) = depths.get(&child) {
                    top.depth = top.depth.max(depth + 1);
                    continue;
                }
                open.insert(child);
                let parts = self.path_parts(child)?;
                stack.push(Frame {
                    node: child,
                    parts,
                    next: 0,
                    depth: 1,
                });
                continue;
            }
            let Some(frame) = stack.pop() else {
                break;
            };
            if frame.depth > MAX_NESTING {
                return Err(too_deep());
            }
            open.remove(&frame.node);
            depths.insert(frame.node, frame.depth);
            match stack.last_mut() {
                Some(parent) => parent.depth = parent.depth.max(frame.depth + 1),
                None => return Ok(frame.depth),
            }
        }
        Ok(0)
    }

    /// What the validator reads of a node as a shape: the shapes it refers to (by `sh:node`,
    /// `sh:property`, `sh:not`, `sh:qualifiedValueShape` and `sh:reifierShape`, and in the lists
    /// of `sh:and`, `sh:or` and `sh:xone`), and the depth of its deepest path. Checks its lists
    /// of values (`sh:in`, `sh:ignoredProperties`, `sh:languageIn`) too.
    fn shape_parts(
        &self,
        node: NamedOrBlankNodeRef<'a>,
        path_depths: &mut FxHashMap<BlankNodeRef<'a>, usize>,
        value_lists: &mut FxHashSet<NamedOrBlankNodeRef<'a>>,
    ) -> Result<(Vec<NamedOrBlankNodeRef<'a>>, usize), RdfError> {
        let mut shapes = Vec::new();
        let mut depth = 0;
        for &(predicate, object) in self.properties(node) {
            let Some(local) = predicate.as_str().strip_prefix(SH) else {
                continue;
            };
            match local {
                "and" | "or" | "xone" => {
                    shapes.extend(self.list(object)?.into_iter().filter_map(as_node))
                }
                "node" | "property" | "not" | "qualifiedValueShape" | "reifierShape" => {
                    shapes.extend(as_node(object))
                }
                "path" => depth = depth.max(self.path_depth(object, path_depths)?),
                "in" | "ignoredProperties" | "languageIn" => {
                    // each list once
                    let unchecked = as_node(object).is_some_and(|head| value_lists.insert(head));
                    if unchecked {
                        self.list(object)?;
                    }
                }
                _ => {}
            }
        }
        Ok((shapes, depth))
    }

    /// Checks what the validator reads recursively, taking every node as a possible shape.
    ///
    /// Fails with [`RdfError::Shacl`] if one of its RDF lists is cyclic or has more than
    /// [`MAX_LIST_LENGTH`] members, if a path contains itself, or if shapes and paths nest more
    /// than [`MAX_NESTING`] deep (a shape is one deeper than the shapes it refers to, and at
    /// least as deep as its paths), and with [`RdfError::ShaclUnsupported`] for the inverse of
    /// a path that is not a predicate. A shape that refers to itself is a recursive shape, not
    /// an error: returns whether there is one.
    fn check_structure(&self) -> Result<bool, RdfError> {
        struct Frame<'a> {
            node: NamedOrBlankNodeRef<'a>,
            shapes: Vec<NamedOrBlankNodeRef<'a>>,
            next: usize,
            depth: usize,
        }
        let mut path_depths = FxHashMap::default();
        let mut value_lists = FxHashSet::default();
        // the depth of every shape checked, and `None` for those on the stack
        let mut depths: FxHashMap<NamedOrBlankNodeRef<'a>, Option<usize>> = FxHashMap::default();
        let mut recursive = false;
        let mut stack: Vec<Frame<'a>> = Vec::new();
        for &root in self.properties.keys() {
            if depths.contains_key(&root) {
                continue;
            }
            depths.insert(root, None);
            let (shapes, depth) = self.shape_parts(root, &mut path_depths, &mut value_lists)?;
            stack.push(Frame {
                node: root,
                shapes,
                next: 0,
                depth,
            });
            while let Some(top) = stack.last_mut() {
                if let Some(&shape) = top.shapes.get(top.next) {
                    top.next += 1;
                    match depths.get(&shape).copied() {
                        Some(None) => recursive = true,
                        Some(Some(depth)) => top.depth = top.depth.max(depth + 1),
                        None => {
                            depths.insert(shape, None);
                            let (shapes, depth) =
                                self.shape_parts(shape, &mut path_depths, &mut value_lists)?;
                            stack.push(Frame {
                                node: shape,
                                shapes,
                                next: 0,
                                depth,
                            });
                        }
                    }
                    continue;
                }
                let Some(frame) = stack.pop() else {
                    break;
                };
                if frame.depth > MAX_NESTING {
                    return Err(too_deep());
                }
                depths.insert(frame.node, Some(frame.depth));
                if let Some(parent) = stack.last_mut() {
                    parent.depth = parent.depth.max(frame.depth + 1);
                }
            }
        }
        Ok(recursive)
    }

    /// Fails with [`RdfError::ShaclUnsupported`] if a literal of `sh:hasValue` or `sh:in` is
    /// one the validator rewrites (see [`canonical_form`]): it compares the rewritten literal
    /// with the terms of the data, so it would never match the literal as written. Lists are
    /// checked first (see [`check_structure`](Self::check_structure)).
    fn check_literals(&self) -> Result<(), RdfError> {
        for properties in self.properties.values() {
            for &(predicate, object) in properties {
                let local = match predicate.as_str().strip_prefix(SH) {
                    Some(local @ ("hasValue" | "in")) => local,
                    _ => continue,
                };
                let values = if local == "in" {
                    self.list(object)?
                } else {
                    vec![object]
                };
                for value in values {
                    let TermRef::Literal(literal) = value else {
                        continue;
                    };
                    let literal = Term::from(literal.into_owned());
                    if let Some(canonical) = canonical_form(&literal) {
                        return Err(RdfError::ShaclUnsupported(format!(
                            "sh:{local} {literal} (the validator rewrites this literal as \
                             {canonical}, a different term, so it would never match the data)"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// The literals of `sh:targetNode` that the validator rewrites, by the form it gives them
    /// (see [`canonical_form`]), when only one literal has that form and it is not itself a
    /// target: the store looks them up as written (see [`ShaclStore::node`]).
    fn rewritten_targets(&self) -> FxHashMap<Term, Term> {
        let mut targets: FxHashMap<Term, Option<Term>> = FxHashMap::default();
        let mut written = FxHashSet::default();
        for properties in self.properties.values() {
            for &(predicate, object) in properties {
                if predicate.as_str().strip_prefix(SH) != Some("targetNode") {
                    continue;
                }
                let target = object.into_owned();
                if let Some(canonical) = canonical_form(&target) {
                    targets
                        .entry(canonical)
                        .and_modify(|original| {
                            if original.as_ref() != Some(&target) {
                                *original = None
                            }
                        })
                        .or_insert_with(|| Some(target.clone()));
                }
                written.insert(target);
            }
        }
        targets
            .into_iter()
            .filter(|(canonical, _)| !written.contains(canonical))
            .filter_map(|(canonical, original)| Some((canonical, original?)))
            .collect()
    }
}

/// The pool that compiles shapes and validates, built on first use: threads with a stack of
/// [`STACK_SIZE`] bytes, as a stack overflow cannot be caught and would abort the process.
fn pool() -> Result<&'static ThreadPool, RdfError> {
    static POOL: OnceLock<Result<ThreadPool, String>> = OnceLock::new();
    POOL.get_or_init(|| {
        ThreadPoolBuilder::new()
            .thread_name(|i| format!("raphtory-shacl-{i}"))
            .stack_size(STACK_SIZE)
            .build()
            .map_err(|error| error.to_string())
    })
    .as_ref()
    .map_err(|error| RdfError::Shacl(format!("cannot start the SHACL threads: {error}")))
}

/// Runs `f` on [`pool`] (so the parallel work of the validator runs there too), turning a panic
/// of `what` into [`RdfError::Shacl`].
pub(crate) fn run_guarded<R: Send>(
    what: &str,
    f: impl FnOnce() -> Result<R, RdfError> + Send,
) -> Result<R, RdfError> {
    pool()?.install(|| {
        catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|panic| {
            Err(RdfError::Shacl(format!(
                "{what} panicked: {}",
                panic_message(&*panic)
            )))
        })
    })
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| message.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_owned())
}

/// The IRI of a term of a report, which the validator always makes an IRI.
fn iri(term: Term, what: &str) -> Result<NamedNode, RdfError> {
    match term {
        Term::NamedNode(iri) => Ok(iri),
        other => Err(RdfError::Shacl(format!("{what} {other} is not an IRI"))),
    }
}

/// Converts a report of the validator on `store`, with the literals of the store as written
/// (see [`ShaclStore::original`]).
fn convert(
    report: &ValidationReport,
    warnings: Vec<String>,
    store: &ShaclStore,
) -> Result<ShaclReport, RdfError> {
    let mut results = report
        .results()
        .iter()
        .map(|result| {
            let mut messages: Vec<Literal> = result
                .message()
                .iter()
                .map(|(language, message)| {
                    language
                        .as_ref()
                        .and_then(|language| {
                            Literal::new_language_tagged_literal(message, language.as_str()).ok()
                        })
                        .unwrap_or_else(|| Literal::new_simple_literal(message))
                })
                .collect();
            messages.sort_by_cached_key(Literal::to_string);
            Ok(ShaclResult {
                focus_node: store.original(result.focus_node().clone().into()),
                path: result.path().map(ShaclPath::from),
                value: result
                    .value()
                    .cloned()
                    .map(|value| store.original(value.into())),
                source_shape: result.source().cloned().map(Into::into),
                constraint_component: iri(
                    result.constraint_component().clone().into(),
                    "constraint component",
                )?,
                severity: NamedNode::from(IriS::from(result.severity())),
                messages,
            })
        })
        .collect::<Result<Vec<_>, RdfError>>()?;
    results.sort_by_cached_key(|result| {
        (
            result.focus_node.to_string(),
            result.constraint_component.to_string(),
            result.path.as_ref().map(ShaclPath::to_string),
            result.value.as_ref().map(Term::to_string),
            result.source_shape.as_ref().map(Term::to_string),
            result.severity.to_string(),
            result
                .messages
                .iter()
                .map(Literal::to_string)
                .collect::<Vec<_>>(),
        )
    });
    Ok(ShaclReport {
        conforms: report.conforms(),
        results,
        warnings,
    })
}

impl ShaclReport {
    /// Writes the report as a W3C `sh:ValidationReport` graph: a blank node of type
    /// `sh:ValidationReport` with `sh:conforms` and one `sh:result` per result (a blank node of
    /// type `sh:ValidationResult` with `sh:focusNode`, `sh:resultSeverity`,
    /// `sh:sourceConstraintComponent`, and, when they are set, `sh:sourceShape`, `sh:value`,
    /// `sh:resultPath` and `sh:resultMessage`). Paths other than a predicate are written as
    /// SHACL paths (RDF lists and blank nodes). [`warnings`](Self::warnings) are not written.
    ///
    /// `serializer` is an [`RdfFormat`] or an [`RdfSerializer`] (for example with the `sh:`
    /// prefix from [`serializer_with_prefixes`](crate::rdf::serializer_with_prefixes)). The
    /// triples of each subject are written together.
    ///
    /// In RDF/XML, a report whose focus node, value or message is a literal RDF/XML cannot hold
    /// (control characters other than tab and line feed, U+FFFE, U+FFFF) fails with
    /// [`RdfError::XmlUnsafeReport`] before anything is written. [`RdfExportStats::skipped`] is
    /// always 0.
    pub fn write<W: Write>(
        &self,
        writer: W,
        serializer: impl Into<RdfSerializer>,
    ) -> Result<RdfExportStats, GraphError> {
        let serializer = serializer.into();
        let mut graph = ReportGraph::default();
        let root: NamedOrBlankNode = BlankNode::default().into();
        graph.add(&root, rdf::TYPE, sh("ValidationReport"));
        graph.add(&root, sh("conforms").as_ref(), Literal::from(self.conforms));
        for result in &self.results {
            let node: NamedOrBlankNode = BlankNode::default().into();
            graph.add(&root, sh("result").as_ref(), node.clone());
            graph.add(&node, rdf::TYPE, sh("ValidationResult"));
            graph.add(&node, sh("focusNode").as_ref(), result.focus_node.clone());
            graph.add(
                &node,
                sh("resultSeverity").as_ref(),
                result.severity.clone(),
            );
            graph.add(
                &node,
                sh("sourceConstraintComponent").as_ref(),
                result.constraint_component.clone(),
            );
            if let Some(shape) = &result.source_shape {
                graph.add(&node, sh("sourceShape").as_ref(), shape.clone());
            }
            if let Some(value) = &result.value {
                graph.add(&node, sh("value").as_ref(), value.clone());
            }
            if let Some(path) = &result.path {
                let path = graph.path(path);
                graph.add(&node, sh("resultPath").as_ref(), path);
            }
            for message in &result.messages {
                graph.add(&node, sh("resultMessage").as_ref(), message.clone());
            }
        }
        if serializer.format() == RdfFormat::RdfXml {
            if let Some(object) = graph
                .0
                .values()
                .flatten()
                .map(|(_, object)| object)
                .find(|object| !xml_text_safe(object.as_ref()))
            {
                return Err(RdfError::XmlUnsafeReport(object.to_string()).into());
            }
        }
        let mut sink = TripleSink::new(serializer, writer);
        for (subject, properties) in &graph.0 {
            for (predicate, object) in properties {
                sink.push(subject.as_ref(), predicate.as_ref(), object.as_ref())?;
            }
        }
        Ok(sink.finish()?)
    }
}

fn sh(local: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("{SH}{local}"))
}

/// The triples of a report, grouped by subject (in the order of their first triple).
#[derive(Default)]
struct ReportGraph(IndexMap<NamedOrBlankNode, Vec<(NamedNode, Term)>, FxBuildHasher>);

impl ReportGraph {
    fn add(
        &mut self,
        subject: &NamedOrBlankNode,
        predicate: NamedNodeRef<'_>,
        object: impl Into<Term>,
    ) {
        self.0
            .entry(subject.clone())
            .or_default()
            .push((predicate.into_owned(), object.into()));
    }

    /// Adds the triples of a SHACL path and returns its node.
    fn path(&mut self, path: &ShaclPath) -> Term {
        let wrap = |graph: &mut Self, predicate: &str, inner: &ShaclPath| -> Term {
            let node: NamedOrBlankNode = BlankNode::default().into();
            let inner = graph.path(inner);
            graph.add(&node, sh(predicate).as_ref(), inner);
            node.into()
        };
        match path {
            ShaclPath::Predicate(predicate) => predicate.clone().into(),
            ShaclPath::Sequence(paths) => self.list(paths),
            ShaclPath::Alternative(paths) => {
                let node: NamedOrBlankNode = BlankNode::default().into();
                let list = self.list(paths);
                self.add(&node, sh("alternativePath").as_ref(), list);
                node.into()
            }
            ShaclPath::Inverse(path) => wrap(self, "inversePath", path),
            ShaclPath::ZeroOrMore(path) => wrap(self, "zeroOrMorePath", path),
            ShaclPath::OneOrMore(path) => wrap(self, "oneOrMorePath", path),
            ShaclPath::ZeroOrOne(path) => wrap(self, "zeroOrOnePath", path),
        }
    }

    /// Adds an RDF list of paths and returns its head.
    fn list(&mut self, paths: &[ShaclPath]) -> Term {
        let nodes: Vec<NamedOrBlankNode> =
            paths.iter().map(|_| BlankNode::default().into()).collect();
        for (i, (node, path)) in nodes.iter().zip(paths).enumerate() {
            let first = self.path(path);
            self.add(node, rdf::FIRST, first);
            let rest: Term = match nodes.get(i + 1) {
                Some(next) => next.clone().into(),
                None => rdf::NIL.into_owned().into(),
            };
            self.add(node, rdf::REST, rest);
        }
        match nodes.into_iter().next() {
            Some(head) => head.into(),
            None => rdf::NIL.into_owned().into(),
        }
    }
}

/// The triples of a view, as the validator reads them: one triple per `(edge, layer)` pair
/// visible in `view.valid()`, as [`to_rdf`](crate::rdf::RdfViewOps::to_rdf) writes them. Every
/// pattern is answered by an [`EdgeScan`], which holds no lock between triples, so writers are
/// never blocked and the validator's own reads cannot deadlock.
pub(crate) struct ShaclStore {
    /// The view (not `valid()`: the scans only produce valid pairs).
    view: DynamicGraph,
    /// The predicate IRI of every layer whose term is an IRI.
    predicates: FxHashMap<LayerId, NamedNode>,
    /// The layer of every predicate IRI.
    layers: FxHashMap<NamedNode, LayerId>,
    /// The terms of the nodes read so far (at most [`TERM_CACHE_CAP`]).
    terms: DashMap<VID, Term, FxBuildHasher>,
    /// The literals read so far that the validator rewrites, by the form it gives them (see
    /// [`canonical_form`]): `None` if several literals have that form. At most
    /// [`TERM_CACHE_CAP`] forms are remembered; the literals of the others stay rewritten in
    /// results.
    originals: DashMap<Term, Option<Term>, FxBuildHasher>,
    /// The literals of `sh:targetNode` the validator rewrites, by the form it gives them.
    targets: Arc<FxHashMap<Term, Term>>,
}

impl fmt::Debug for ShaclStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ShaclStore")
    }
}

impl ShaclStore {
    pub(crate) fn new(view: DynamicGraph) -> Self {
        let mut predicates = FxHashMap::default();
        let mut layers = FxHashMap::default();
        for (layer, name) in view_layers(&view) {
            if let Some(iri) = layer_predicate(&name) {
                layers.insert(iri.clone(), layer);
                predicates.insert(layer, iri);
            }
        }
        Self {
            view,
            predicates,
            layers,
            terms: DashMap::default(),
            originals: DashMap::default(),
            targets: Arc::default(),
        }
    }

    /// The store, with the literals of `sh:targetNode` that the validator rewrites.
    fn with_targets(self, targets: Arc<FxHashMap<Term, Term>>) -> Self {
        Self { targets, ..self }
    }

    /// Whether the view has a visible `rdfs:subClassOf` triple.
    fn has_subclass_triples(&self) -> bool {
        self.layers
            .get(&rdfs::SUB_CLASS_OF.into_owned())
            .is_some_and(|&layer| {
                EdgeScan::all(self.view.clone(), Some(layer))
                    .only_valid()
                    .next()
                    .is_some()
            })
    }

    /// The term of node `v`.
    fn node_term(&self, v: VID) -> Term {
        if let Some(term) = self.terms.get(&v) {
            return term.clone();
        }
        let term = term_of(&self.view.node_name(v));
        self.note_original(&term);
        if self.terms.len() < TERM_CACHE_CAP {
            self.terms.insert(v, term.clone());
        }
        term
    }

    /// Remembers `term` if it is a literal the validator rewrites (see [`Self::originals`]).
    fn note_original(&self, term: &Term) {
        let Some(canonical) = canonical_form(term) else {
            return;
        };
        // before taking the entry: `len` locks every shard
        let full = self.originals.len() >= TERM_CACHE_CAP;
        match self.originals.entry(canonical) {
            Entry::Occupied(mut entry) => {
                if entry.get().as_ref() != Some(term) {
                    entry.insert(None);
                }
            }
            Entry::Vacant(entry) => {
                if !full {
                    entry.insert(Some(term.clone()));
                }
            }
        }
    }

    /// The term of the store that a term of a report stands for: a literal the validator
    /// rewrote is given back as written in the graph (if only one literal read during the
    /// validation has that form) or in `sh:targetNode`, unless that form is itself a node of
    /// the graph.
    fn original(&self, term: Term) -> Term {
        if !term.is_literal() {
            return term;
        }
        let original = match self.originals.get(&term) {
            Some(entry) => entry.value().clone(),
            None => self.targets.get(&term).cloned(),
        };
        match original {
            Some(original)
                if name_of(term.as_ref())
                    .and_then(|name| lookup_node(&self.view, &name))
                    .is_none() =>
            {
                original
            }
            _ => term,
        }
    }

    /// The node of a term, if the graph has one (whether or not the view shows it: the scans
    /// check that). A literal of `sh:targetNode` that the validator rewrote, and that is not a
    /// node as rewritten, is looked up as written.
    fn node(&self, term: TermRef<'_>) -> Option<VID> {
        lookup_node(&self.view, &name_of(term)?).or_else(|| {
            let TermRef::Literal(literal) = term else {
                return None;
            };
            if self.targets.is_empty() {
                return None;
            }
            let target = self.targets.get(&Term::from(literal.into_owned()))?;
            lookup_node(&self.view, &name_of(target.as_ref())?)
        })
    }

    /// The triple of a visible `(edge, layer)` pair, or `None` for a generalized triple (a
    /// literal subject, or a layer whose term is not an IRI), which `to_rdf` skips too.
    fn triple(&self, (s, layer, o): (VID, LayerId, VID)) -> Option<Triple> {
        let predicate = self.predicates.get(&layer)?;
        let subject = NamedOrBlankNode::try_from(self.node_term(s)).ok()?;
        Some(Triple::new(subject, predicate.clone(), self.node_term(o)))
    }

    /// The scan of the triples matching a pattern, or `None` if a bound term is not in the
    /// graph (so nothing matches).
    fn scan(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
    ) -> Option<EdgeScan> {
        let layer = match predicate {
            Some(predicate) => Some(*self.layers.get(predicate)?),
            None => None,
        };
        let subject = match subject {
            Some(subject) => Some(self.node(subject.as_ref().into())?),
            None => None,
        };
        let object = match object {
            Some(object) => Some(self.node(object.as_ref())?),
            None => None,
        };
        let view = self.view.clone();
        let scan = match (subject, object) {
            (Some(s), Some(o)) => EdgeScan::between(view, s, o, layer),
            (Some(s), None) => EdgeScan::around(view, s, Dir::Out, layer),
            (None, Some(o)) => EdgeScan::around(view, o, Dir::In, layer),
            (None, None) => EdgeScan::all(view, layer),
        };
        Some(scan.only_valid())
    }
}

impl Rdf for ShaclStore {
    type IRI = NamedNode;
    type BNode = BlankNode;
    type Literal = Literal;
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

impl ShaclStore {
    /// Every triple of the view (see [`NeighsRDF::triples`]).
    #[cfg(test)]
    pub(crate) fn all_triples(&self) -> impl Iterator<Item = Triple> + '_ {
        EdgeScan::all(self.view.clone(), None)
            .only_valid()
            .filter_map(move |item| self.triple(item))
    }
}

impl NeighsRDF for ShaclStore {
    /// The `rdf:type` and `rdfs:subClassOf` triples of the view only, not all of them.
    ///
    /// The validator calls this only to build its class index, which reads only these triples;
    /// all other reads go through [`triples_matching`](Self::triples_matching). This holds for
    /// the pinned rudof version: recheck it when upgrading.
    fn triples(&self) -> Result<impl Iterator<Item = Triple>, RdfError> {
        let layers: Vec<LayerId> = [rdf::TYPE, rdfs::SUB_CLASS_OF]
            .into_iter()
            .filter_map(|predicate| self.layers.get(&predicate.into_owned()).copied())
            .collect();
        let view = self.view.clone();
        Ok(layers
            .into_iter()
            .flat_map(move |layer| EdgeScan::all(view.clone(), Some(layer)).only_valid())
            .filter_map(move |item| self.triple(item)))
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
        let scan = self.scan(subject.value(), predicate.value(), object.value());
        Ok(scan
            .into_iter()
            .flatten()
            .filter_map(move |item| self.triple(item)))
    }
}

/// Runs rudof's native engine on a [`ShaclStore`].
struct Proc(ShaclStore);

impl ShaclProcessor<ShaclStore> for Proc {
    fn store(&self) -> &ShaclStore {
        &self.0
    }

    fn runner(_mode: &ShaclValidationMode, config: &ShaclConfig) -> Box<dyn Engine<ShaclStore>> {
        Box::new(NativeEngine::new(config.recursion_semantics()))
    }
}
