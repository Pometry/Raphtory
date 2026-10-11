//! SHACL validation of graph views (feature `shacl`), with the native engine of rudof's
//! [`shacl`](https://crates.io/crates/shacl) crate.
//!
//! A [`ShaclShapes`] is a compiled SHACL shapes graph.
//! [`RdfView::validate_shacl`](crate::RdfView::validate_shacl) checks the triples of a view
//! against it and returns a [`ShaclReport`]: the `sh:ValidationReport` of the W3C SHACL
//! recommendation, as Rust values, which [`ShaclReport::write`] serialises as RDF.
//!
//! # What SHACL sees
//!
//! The data graph is exactly the triples [`to_rdf`](crate::RdfView::to_rdf) writes and
//! [`sparql`](crate::RdfView::sparql) matches: one triple per `(edge, layer)` pair
//! visible in the view, with node names mapped to terms by [`term_of`] and layers to their
//! predicate IRIs. Generalized triples (a literal subject, or a layer whose term is not an IRI)
//! are left out. It is read from the graph as the validator asks for it, without a copy.
//!
//! So time travel is picking the view: `rg.valid()` validates the current state, `rg.at(t)` the
//! state as of `t`, and `rg.snapshot_at(t)` for several `t` tells when the data stopped
//! conforming. As with `sparql`, validation does not see a snapshot of a graph that other
//! threads write meanwhile: validate a view of a `read_only()` graph for a consistent result.
//!
//! # Supported SHACL
//!
//! SHACL Core is supported. Shapes that use other parts of SHACL fail to compile with
//! [`RdfError::ShaclUnsupported`] rather than being ignored or misread by the validator, and
//! shapes that the validator could not read within its stack or memory ([`MAX_LIST_LENGTH`],
//! [`MAX_NESTING`], [`MAX_SHAPES_SIZE`]) fail with [`RdfError::ShaclLimit`].
//!
//! The validator deviates from SHACL in a few ways. [`ShaclReport::warnings`] flags a report
//! that subclass reasoning or recursive shapes may affect (see [`ShaclWarning`]); the user guide
//! page *Validating with SHACL* (`docs/user-guide/rdf/6_shacl.md`) describes every deviation.
//!
//! # Example
//!
//! ```
//! use raphtory::prelude::*;
//! use raphtory_rdf::{shacl::ShaclShapes, RdfFormat, RdfGraph};
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
//! let rg = RdfGraph::new();
//! let doc = r#"
//!     @prefix ex: <http://ex/> .
//!     ex:alice a ex:Person ; ex:name "Alice" .
//! "#;
//! rg.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None).unwrap();
//! rg.retract_rdf(
//!     5,
//!     r#"<http://ex/alice> <http://ex/name> "Alice" ."#.as_bytes(),
//!     RdfFormat::NTriples,
//!     None,
//! )
//! .unwrap();
//!
//! assert!(rg.valid().validate_shacl(&shapes).unwrap().results.len() == 1);
//! // as of t = 3 alice had a name
//! assert!(rg.at(3).validate_shacl(&shapes).unwrap().conforms);
//!
//! // when was the data valid?
//! let conforms: Vec<(i64, bool)> = [1, 4, 5, 9]
//!     .into_iter()
//!     .map(|t| (t, rg.snapshot_at(t).validate_shacl(&shapes).unwrap().conforms))
//!     .collect();
//! assert_eq!(conforms, [(1, true), (4, true), (5, false), (9, false)]);
//!
//! let result = &rg.valid().validate_shacl(&shapes).unwrap().results[0];
//! assert_eq!(result.focus_node.to_string(), "<http://ex/alice>");
//! ```
use crate::{
    dataset::lookup_node,
    export::{serializer_with_prefixes, xml_text_safe, TripleSink},
    mapping::{layer_predicate, name_of, term_of},
    scan::{view_layers, EdgeScan},
    RdfError, RdfFormat,
};
use dashmap::{mapref::entry::Entry, DashMap};
use indexmap::IndexMap;
use oxigraph::{
    io::RdfParser,
    model::{
        vocab::{rdf, rdfs, xsd},
        BlankNode, BlankNodeRef, Graph, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode,
        NamedOrBlankNodeRef, Term, TermRef, Triple, TripleRef,
    },
};
use prefixmap::{PrefixMap, PrefixMapError};
#[cfg(feature = "python")]
use pyo3::pyclass;
use raphtory::db::api::view::{internal::CoreGraphOps, DynamicGraph};
use raphtory_api::core::entities::{LayerId, VID};
use rayon::{ThreadPool, ThreadPoolBuilder};
use rudof_iri::IriS;
use rudof_rdf::{
    rdf_core::{term::Object, vocabs::ShaclVocab, Matcher, NeighsRDF, RDFError, Rdf, SHACLPath},
    rdf_impl::OxigraphInMemory,
};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet, FxHasher};
use shacl::{
    error::IRError,
    ir::{IRComponent, IRSchema},
    rdf::ShaclParser,
    types::Target,
    validator::{
        engine::{Engine, NativeEngine},
        processor::ShaclProcessor,
        report::ValidationReport,
        ShaclConfig, ShaclValidationMode,
    },
};
use std::{
    any::Any,
    collections::HashSet,
    fmt,
    hash::{BuildHasher, Hash, Hasher},
    io::{Read, Write},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering::Relaxed},
        Arc, OnceLock,
    },
};

/// The stack size of the threads that compile shapes and validate (see [`run_guarded`]).
const STACK_SIZE: usize = 64 << 20;

/// The most node terms a validation keeps in memory (see [`ShaclStore::node_term`]), and the
/// most rewritten literals it remembers (see [`ShaclStore::originals`]).
const TERM_CACHE_CAP: usize = 1 << 20;

/// The most members an RDF list of a shapes graph can have, such as the values of `sh:in`.
///
/// The validator reads lists recursively, so shapes with a longer list fail with
/// [`RdfError::ShaclLimit`] rather than risk a stack overflow. Check membership in a longer list
/// of values with `sh:pattern` or a SPARQL query.
pub const MAX_LIST_LENGTH: usize = 10_000;

/// How large a shapes graph can be as the validator reads it: the steps of every `sh:path`,
/// with a shared sub-path counted wherever it is used, plus the members of every `sh:in`,
/// `sh:languageIn`, `sh:ignoredProperties`, `sh:and`, `sh:or` and `sh:xone` list, counted once
/// per reference.
///
/// The validator reads a path again wherever it is used, so a few triples that share sub-paths
/// can make a path too large for memory: shapes that expand to more fail with
/// [`RdfError::ShaclLimit`].
pub const MAX_SHAPES_SIZE: usize = 1_000_000;

/// How deep the shapes and paths of a shapes graph can nest: a shape is one deeper than the
/// shapes it refers to (by `sh:node`, `sh:property`, `sh:not`, `sh:and`, `sh:or`, `sh:xone`,
/// `sh:qualifiedValueShape` or `sh:reifierShape`), and as deep as its paths; a path node is one
/// deeper than the paths it is made of, and a sequence path is as deep as its length plus its
/// deepest member. A shape that refers to itself (a recursive shape) is not counted again.
///
/// The validator reads them recursively, so shapes that nest deeper fail with
/// [`RdfError::ShaclLimit`] rather than risk a stack overflow.
pub const MAX_NESTING: usize = 256;

/// Why the results of a SHACL validation report may not be those of SHACL. Its string form is a
/// stable name: `partial_subclass_reasoning` or `recursive_shapes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "python",
    pyclass(eq, eq_int, frozen, str, skip_from_py_object, module = "raphtory")
)]
#[non_exhaustive]
pub enum ShaclWarning {
    /// The view has `rdfs:subClassOf` triples and the shapes use class targets or `sh:class`,
    /// but the validator follows subclasses only partly: `sh:targetClass` targets direct
    /// `rdf:type` instances only, and implicit class targets and `sh:class` follow a single
    /// `rdfs:subClassOf` step.
    PartialSubclassReasoning,
    /// The shapes are recursive and the report has results. The validator gives recursion
    /// least-fixpoint semantics, which SHACL 1.0 leaves undefined: a node whose conformance
    /// depends on itself through a cycle of the data does not conform, where other processors
    /// may accept it.
    RecursiveShapes,
}

impl fmt::Display for ShaclWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PartialSubclassReasoning => "partial_subclass_reasoning",
            Self::RecursiveShapes => "recursive_shapes",
        })
    }
}

/// A part of SHACL the validator does not evaluate (see [`RdfError::ShaclUnsupported`]).
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShaclFeature {
    #[error("SHACL-SPARQL is not supported")]
    Sparql,
    #[error("SHACL Advanced Features are not supported")]
    AdvancedFeatures,
    #[error("SHACL-JS is not supported")]
    Js,
    #[error("entailment regimes are not supported: validation sees the stored triples only")]
    Entailment,
    #[error("SHACL 1.2 features are not supported")]
    Shacl12,
    #[error(
        "the validator only supports the inverse of a predicate: write ^(p / q) as (^q / ^p), \
         ^(p | q) as (^p | ^q) and ^(p*) as (^p)*"
    )]
    ComplexInversePath,
    /// A literal of `sh:hasValue` or `sh:in` that the validator compares in another form.
    #[error(
        "the validator rewrites the literal {literal} as {canonical}, a different term, so it \
         would never match the data"
    )]
    RewrittenLiteral {
        // boxed, so that this does not make every `RdfError` larger
        literal: Box<Literal>,
        canonical: Box<Term>,
    },
}

/// A limit of the validator that a shapes graph exceeds (see [`RdfError::ShaclLimit`]).
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShaclLimit {
    #[error("the shapes graph has a cyclic RDF list (its rdf:rest links lead back to a node)")]
    CyclicList,
    #[error(
        "the shapes graph has an RDF list of more than {max} members (MAX_LIST_LENGTH), the most \
         the validator accepts",
        max = MAX_LIST_LENGTH
    )]
    ListTooLong,
    #[error("the shapes graph has a path that contains itself")]
    CyclicPath,
    #[error(
        "the shapes graph nests shapes and paths more than {max} deep (MAX_NESTING), the most \
         the validator accepts",
        max = MAX_NESTING
    )]
    TooDeep,
    #[error(
        "the shapes graph expands to more than {max} path steps and list members \
         (MAX_SHAPES_SIZE), the most the validator accepts",
        max = MAX_SHAPES_SIZE
    )]
    TooLarge,
}

/// The feature of a predicate of the SHACL namespace that is rejected, or `None`. `local` is
/// the part after the namespace.
///
/// The validator silently ignores terms outside SHACL Core and 1.2 reifier shapes, so known
/// terms of other SHACL parts are rejected; unknown terms are ignored, as SHACL requires.
fn unsupported_predicate(local: &str) -> Option<ShaclFeature> {
    match local {
        "sparql" | "select" | "ask" | "construct" | "update" | "validator" | "nodeValidator"
        | "propertyValidator" | "parameter" | "optional" | "labelTemplate" => {
            Some(ShaclFeature::Sparql)
        }
        // rules, custom targets, functions and node expressions
        "rule" | "target" | "expression" | "filterShape" | "values" | "condition" | "subject"
        | "predicate" | "object" | "nodes" | "intersection" | "union" | "returnType" | "count"
        | "min" | "max" | "sum" | "distinct" | "exists" | "if" | "then" | "else" | "orderBy"
        | "desc" | "limit" | "offset" | "groupConcat" | "separator" | "minus" | "flatMap"
        | "findFirst" | "matchAll" => Some(ShaclFeature::AdvancedFeatures),
        "entailment" => Some(ShaclFeature::Entailment),
        "singleLine" | "rootClass" | "memberShape" | "minListLength" | "maxListLength"
        | "uniqueMembers" | "nodeByExpression" | "targetWhere" | "someValue" => {
            Some(ShaclFeature::Shacl12)
        }
        _ if local.starts_with("js") => Some(ShaclFeature::Js),
        _ => None,
    }
}

/// The feature of a class of the SHACL namespace (an `rdf:type` object in the shapes graph)
/// that is rejected, or `None` (see [`unsupported_predicate`]).
fn unsupported_class(local: &str) -> Option<ShaclFeature> {
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
        | "SPARQLUpdateExecutable" => Some(ShaclFeature::Sparql),
        "SPARQLRule" | "TripleRule" | "Rule" | "Target" | "TargetType" => {
            Some(ShaclFeature::AdvancedFeatures)
        }
        "ShapeClass" => Some(ShaclFeature::Shacl12),
        _ if local.starts_with("JS") => Some(ShaclFeature::Js),
        _ => None,
    }
}

/// A parsed and compiled SHACL shapes graph. One [`ShaclShapes`] can validate many views, from
/// many threads.
#[derive(Clone)]
pub struct ShaclShapes {
    schema: IRSchema,
    /// Whether the shapes use class targets or `sh:class`, which follow `rdfs:subClassOf` only
    /// partly (see [`ShaclWarning::PartialSubclassReasoning`]).
    uses_classes: bool,
    /// Whether a shape refers to itself (see [`ShaclWarning::RecursiveShapes`]).
    recursive: bool,
    /// The literals of `sh:targetNode` the validator rewrites (see
    /// [`ShapesGraph::rewritten_targets`]).
    targets: Arc<FxHashMap<Term, Term>>,
}

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
/// [`to_rdf`](crate::RdfView::to_rdf): use [`name_of`] to get the Raphtory name of a
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

/// A SHACL `sh:ValidationReport`: the result of
/// [`RdfView::validate_shacl`](crate::RdfView::validate_shacl).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShaclReport {
    /// `sh:conforms`: whether there are no results, of any severity.
    pub conforms: bool,
    /// `sh:result`: the validation results, in a deterministic order (by focus node, then
    /// constraint component, path, value and source shape).
    pub results: Vec<ShaclResult>,
    /// Why the results may not be those of SHACL (not part of the W3C report).
    pub warnings: Vec<ShaclWarning>,
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
    /// not evaluate (see the [module documentation](self)), [`RdfError::ShaclLimit`] if it
    /// exceeds a limit of the validator ([`MAX_LIST_LENGTH`], [`MAX_NESTING`],
    /// [`MAX_SHAPES_SIZE`]), and [`RdfError::Shacl`] if it is not a valid shapes graph.
    pub fn parse(
        data: impl Read,
        format: RdfFormat,
        base_iri: Option<&str>,
    ) -> Result<Self, RdfError> {
        let mut parser = RdfParser::from_format(format.to_oxigraph()).without_named_graphs();
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
    ) -> Result<Self, RdfError> {
        let graph: Graph = triples.into_iter().collect();
        check_supported(&graph)?;
        let shapes = ShapesGraph(&graph);
        let recursive = shapes.check_structure()?;
        shapes.check_literals()?;
        let targets = Arc::new(shapes.rewritten_targets());
        let schema = run_guarded("the SHACL shapes compiler", || {
            let mut store = OxigraphInMemory::new();
            for triple in graph.iter() {
                store
                    .add_triple_ref(triple.subject, triple.predicate, triple.object)
                    .map_err(RdfError::shacl)?;
            }
            let ast = ShaclParser::new(store)
                .parse()
                .map_err(|error| RdfError::shacl(IRError::from(error)))?;
            IRSchema::try_from(ast).map_err(RdfError::shacl)
        })?;
        let class_target =
            |target: &Target| matches!(target, Target::Class(_) | Target::ImplicitClass(_));
        let class = |component: &IRComponent| matches!(component, IRComponent::Class(_));
        let uses_classes = schema.iter().any(|(_, shape)| {
            !shape.deactivated()
                && (shape.targets().iter().any(class_target)
                    || shape.components().iter().any(class))
        });
        Ok(Self {
            schema,
            uses_classes,
            recursive,
            targets,
        })
    }

    /// Whether a shape refers to itself. The validator follows such shapes through the data by
    /// recursing once per node, so on a long enough chain of nodes it overflows its stack.
    pub fn is_recursive(&self) -> bool {
        self.recursive
    }

    /// Body of [`RdfView::validate_shacl`](crate::RdfView::validate_shacl).
    pub(crate) fn validate(&self, view: DynamicGraph) -> Result<ShaclReport, RdfError> {
        let report = run_guarded("the SHACL validator", || {
            let store = ShaclStore::new(view, self.targets.clone(), self.uses_classes);
            let mut processor = Proc(store);
            let mut warnings = Vec::new();
            if self.uses_classes && processor.0.has_subclass_triples() {
                warnings.push(ShaclWarning::PartialSubclassReasoning);
            }
            let report = processor
                .validate(
                    &self.schema,
                    &ShaclValidationMode::Native,
                    &ShaclConfig::default(),
                )
                .map_err(RdfError::shacl)?;
            if self.recursive && !report.conforms() {
                warnings.push(ShaclWarning::RecursiveShapes);
            }
            convert(&report, warnings, &processor.0)
        })?;
        Ok(report)
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
fn check_supported(graph: &Graph) -> Result<(), RdfError> {
    for triple in graph {
        let rejected = match (triple.predicate.as_str(), triple.object) {
            // SHACL 1.2 closes a shape by the properties of its types with `sh:ByTypes`
            (ShaclVocab::SH_CLOSED, TermRef::NamedNode(object))
                if object.as_str().strip_prefix(ShaclVocab::SH) == Some("ByTypes") =>
            {
                Some((object, ShaclFeature::Shacl12))
            }
            (_, TermRef::NamedNode(class)) if triple.predicate == rdf::TYPE => class
                .as_str()
                .strip_prefix(ShaclVocab::SH)
                .and_then(unsupported_class)
                .map(|feature| (class, feature)),
            (predicate, _) => predicate
                .strip_prefix(ShaclVocab::SH)
                .and_then(unsupported_predicate)
                .map(|feature| (triple.predicate, feature)),
        };
        if let Some((term, feature)) = rejected {
            return Err(RdfError::ShaclUnsupported {
                term: term.into_owned(),
                feature,
            });
        }
    }
    Ok(())
}

/// The literal the validator makes of a term, if it is not the term itself: the validator reads
/// literals of some datatypes as values and writes them back in canonical form, such as
/// `"1"^^xsd:boolean` as `"true"^^xsd:boolean`.
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

/// A shapes graph, to check what the validator reads of it recursively before it compiles it:
/// the shapes, the shapes they refer to, their paths, and the RDF lists of these. The checks
/// over-approximate what the validator may read.
struct ShapesGraph<'a>(&'a Graph);

/// What [`ShapesGraph::check_structure`] has measured.
#[derive(Default)]
struct Walk<'a> {
    /// The depth and expanded size of every path node measured.
    paths: FxHashMap<BlankNodeRef<'a>, (usize, usize)>,
    /// The depth of every shape measured, and `None` for those being measured.
    shapes: FxHashMap<NamedOrBlankNodeRef<'a>, Option<usize>>,
    /// The path steps and list members counted so far (see [`MAX_SHAPES_SIZE`]).
    size: usize,
    /// Whether a shape refers to itself.
    recursive: bool,
}

impl Walk<'_> {
    /// Counts `n` more path steps or list members.
    fn count(&mut self, n: usize) -> Result<(), RdfError> {
        self.size = self.size.saturating_add(n);
        if self.size > MAX_SHAPES_SIZE {
            return Err(RdfError::ShaclLimit(ShaclLimit::TooLarge));
        }
        Ok(())
    }
}

impl<'a> ShapesGraph<'a> {
    /// The members (`rdf:first`) of the RDF list at `head`, following every `rdf:rest`. Fails
    /// with [`RdfError::ShaclLimit`] if a node is reached twice or there are more than
    /// [`MAX_LIST_LENGTH`] members.
    fn list(&self, head: TermRef<'a>) -> Result<Vec<TermRef<'a>>, RdfError> {
        let mut members = Vec::new();
        let mut seen = FxHashSet::default();
        let mut pending: Vec<_> = as_node(head).into_iter().collect();
        while let Some(node) = pending.pop() {
            if !seen.insert(node) {
                return Err(RdfError::ShaclLimit(ShaclLimit::CyclicList));
            }
            members.extend(self.0.objects_for_subject_predicate(node, rdf::FIRST));
            if members.len() > MAX_LIST_LENGTH {
                return Err(RdfError::ShaclLimit(ShaclLimit::ListTooLong));
            }
            pending.extend(
                self.0
                    .objects_for_subject_predicate(node, rdf::REST)
                    .filter_map(as_node),
            );
        }
        Ok(members)
    }

    /// The base depth of a path node (a blank node) and the paths it may be made of. The
    /// validator reads it as the first construct that parses, with any one value of each: an RDF
    /// list (a sequence path, as deep as its length), `sh:alternativePath` (a list),
    /// `sh:zeroOrMorePath`, `sh:oneOrMorePath`, `sh:zeroOrOnePath` or `sh:inversePath`. So the
    /// parts are every value of every construct. Fails with [`RdfError::ShaclUnsupported`] for
    /// the inverse of anything but a predicate on a node with no other construct, on which the
    /// validator panics.
    fn path_parts(&self, node: BlankNodeRef<'a>) -> Result<(usize, Vec<TermRef<'a>>), RdfError> {
        let mut parts = Vec::new();
        let mut base = 1;
        if self
            .0
            .object_for_subject_predicate(node, rdf::FIRST)
            .is_some()
        {
            parts = self.list(node.into())?;
            base = parts.len();
        }
        let mut inverses = Vec::new();
        for triple in self.0.triples_for_subject(node) {
            match triple.predicate.as_str() {
                ShaclVocab::SH_ALTERNATIVE_PATH => parts.extend(self.list(triple.object)?),
                ShaclVocab::SH_ZERO_OR_MORE_PATH
                | ShaclVocab::SH_ONE_OR_MORE_PATH
                | ShaclVocab::SH_ZERO_OR_ONE_PATH => parts.push(triple.object),
                ShaclVocab::SH_INVERSE_PATH => inverses.push(triple.object),
                _ => {}
            }
        }
        if parts.is_empty() && !inverses.iter().all(TermRef::is_named_node) {
            return Err(RdfError::ShaclUnsupported {
                term: NamedNode::new_unchecked(ShaclVocab::SH_INVERSE_PATH),
                feature: ShaclFeature::ComplexInversePath,
            });
        }
        parts.extend(inverses);
        Ok((base, parts))
    }

    /// How deep the path `path` nests (see [`MAX_NESTING`]) and how many steps it expands to
    /// (see [`MAX_SHAPES_SIZE`]): 0 deep and one step for a predicate; for a path node, its base
    /// depth plus the depth of its deepest part, and one step plus those of its parts (see
    /// [`path_parts`](Self::path_parts)). `level` is how deep the walk is, `open` holds the
    /// path nodes being measured and `paths` those measured before.
    fn path_depth(
        &self,
        path: TermRef<'a>,
        level: usize,
        open: &mut FxHashSet<BlankNodeRef<'a>>,
        paths: &mut FxHashMap<BlankNodeRef<'a>, (usize, usize)>,
    ) -> Result<(usize, usize), RdfError> {
        let TermRef::BlankNode(node) = path else {
            return Ok((0, 1));
        };
        if let Some(&measure) = paths.get(&node) {
            return Ok(measure);
        }
        if level > MAX_NESTING {
            return Err(RdfError::ShaclLimit(ShaclLimit::TooDeep));
        }
        if !open.insert(node) {
            return Err(RdfError::ShaclLimit(ShaclLimit::CyclicPath));
        }
        let (base, parts) = self.path_parts(node)?;
        let (mut deepest, mut size) = (0, 1_usize);
        for part in parts {
            let (depth, steps) = self.path_depth(part, level + 1, open, paths)?;
            deepest = deepest.max(depth);
            size = size.saturating_add(steps);
        }
        open.remove(&node);
        let depth = base + deepest;
        if depth > MAX_NESTING {
            return Err(RdfError::ShaclLimit(ShaclLimit::TooDeep));
        }
        paths.insert(node, (depth, size));
        Ok((depth, size))
    }

    /// What the validator reads of a node as a shape: the shapes it refers to (by `sh:node`,
    /// `sh:property`, `sh:not`, `sh:qualifiedValueShape` and `sh:reifierShape`, and in the lists
    /// of `sh:and`, `sh:or` and `sh:xone`), and the depth of its deepest path. Counts its paths
    /// and lists (`sh:in`, `sh:ignoredProperties` and `sh:languageIn` too) in `walk`.
    fn shape_parts(
        &self,
        node: NamedOrBlankNodeRef<'a>,
        walk: &mut Walk<'a>,
    ) -> Result<(Vec<NamedOrBlankNodeRef<'a>>, usize), RdfError> {
        let mut shapes = Vec::new();
        let mut depth = 0;
        for triple in self.0.triples_for_subject(node) {
            match triple.predicate.as_str() {
                ShaclVocab::SH_AND | ShaclVocab::SH_OR | ShaclVocab::SH_XONE => {
                    let members = self.list(triple.object)?;
                    walk.count(members.len())?;
                    shapes.extend(members.into_iter().filter_map(as_node));
                }
                ShaclVocab::SH_NODE
                | ShaclVocab::SH_PROPERTY
                | ShaclVocab::SH_NOT
                | ShaclVocab::SH_QUALIFIED_VALUE_SHAPE
                | ShaclVocab::SH_REIFIER_SHAPE => shapes.extend(as_node(triple.object)),
                ShaclVocab::SH_PATH => {
                    let mut open = FxHashSet::default();
                    let (path_depth, size) =
                        self.path_depth(triple.object, 0, &mut open, &mut walk.paths)?;
                    walk.count(size)?;
                    depth = depth.max(path_depth);
                }
                ShaclVocab::SH_IN
                | ShaclVocab::SH_IGNORED_PROPERTIES
                | ShaclVocab::SH_LANGUAGE_IN => walk.count(self.list(triple.object)?.len())?,
                _ => {}
            }
        }
        Ok((shapes, depth))
    }

    /// How deep the shape `node` nests (see [`MAX_NESTING`]), or `None` if it is being measured:
    /// a shape that refers back to itself, which adds no depth. `level` is how deep the walk is.
    fn shape_depth(
        &self,
        node: NamedOrBlankNodeRef<'a>,
        level: usize,
        walk: &mut Walk<'a>,
    ) -> Result<Option<usize>, RdfError> {
        match walk.shapes.get(&node) {
            Some(None) => {
                walk.recursive = true;
                return Ok(None);
            }
            Some(&Some(depth)) => return Ok(Some(depth)),
            None => {}
        }
        if level > MAX_NESTING {
            return Err(RdfError::ShaclLimit(ShaclLimit::TooDeep));
        }
        walk.shapes.insert(node, None);
        let (shapes, mut depth) = self.shape_parts(node, walk)?;
        for shape in shapes {
            if let Some(inner) = self.shape_depth(shape, level + 1, walk)? {
                depth = depth.max(inner + 1);
            }
        }
        if depth > MAX_NESTING {
            return Err(RdfError::ShaclLimit(ShaclLimit::TooDeep));
        }
        walk.shapes.insert(node, Some(depth));
        Ok(Some(depth))
    }

    /// Checks the limits, taking every subject as a shape; returns whether a shape is recursive.
    fn check_structure(&self) -> Result<bool, RdfError> {
        let mut walk = Walk::default();
        for triple in self.0 {
            self.shape_depth(triple.subject, 0, &mut walk)?;
        }
        Ok(walk.recursive)
    }

    /// Fails with [`RdfError::ShaclUnsupported`] if a literal of `sh:hasValue` or `sh:in` is
    /// one the validator rewrites (see [`canonical_form`]): it compares the rewritten literal
    /// with the terms of the data, so it would never match the literal as written. Lists are
    /// checked first (see [`check_structure`](Self::check_structure)).
    fn check_literals(&self) -> Result<(), RdfError> {
        for triple in self.0 {
            let values = match triple.predicate.as_str() {
                ShaclVocab::SH_IN => self.list(triple.object)?,
                ShaclVocab::SH_HAS_VALUE => vec![triple.object],
                _ => continue,
            };
            for value in values {
                let TermRef::Literal(literal) = value else {
                    continue;
                };
                let literal = literal.into_owned();
                if let Some(canonical) = canonical_form(&literal.clone().into()) {
                    return Err(RdfError::ShaclUnsupported {
                        term: triple.predicate.into_owned(),
                        feature: ShaclFeature::RewrittenLiteral {
                            literal: Box::new(literal),
                            canonical: Box::new(canonical),
                        },
                    });
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
        let target_node = NamedNodeRef::new_unchecked(ShaclVocab::SH_TARGET_NODE);
        for triple in self.0.triples_for_predicate(target_node) {
            let target = triple.object.into_owned();
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
    // Only a built pool is kept, so a failed build is retried by the next call.
    static POOL: OnceLock<ThreadPool> = OnceLock::new();
    if let Some(pool) = POOL.get() {
        return Ok(pool);
    }
    let pool = ThreadPoolBuilder::new()
        .thread_name(|i| format!("raphtory-shacl-{i}"))
        .stack_size(STACK_SIZE)
        .build()
        .map_err(RdfError::shacl)?;
    Ok(POOL.get_or_init(|| pool))
}

/// Runs `f` on [`pool`] (so the parallel work of the validator runs there too), turning a panic
/// of `what` into [`RdfError::Shacl`].
pub(crate) fn run_guarded<R: Send>(
    what: &str,
    f: impl FnOnce() -> Result<R, RdfError> + Send,
) -> Result<R, RdfError> {
    pool()?.install(|| {
        catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|panic| {
            Err(RdfError::shacl(format!(
                "{what} panicked: {}",
                panic_message(&*panic)
            )))
        })
    })
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic")
}

/// Converts a report of the validator on `store`, with the literals of the store as written
/// (see [`ShaclStore::original`]).
fn convert(
    report: &ValidationReport,
    warnings: Vec<ShaclWarning>,
    store: &ShaclStore,
) -> Result<ShaclReport, RdfError> {
    let mut results = report
        .results()
        .iter()
        .map(|result| {
            let mut messages = result
                .message()
                .iter()
                .map(|(language, message)| match language {
                    Some(language) => {
                        Literal::new_language_tagged_literal(message, language.as_str())
                    }
                    None => Ok(Literal::new_simple_literal(message)),
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(RdfError::shacl)?;
            messages.sort_by_cached_key(Literal::to_string);
            Ok(ShaclResult {
                focus_node: store.original(result.focus_node().clone().into()),
                path: result.path().map(ShaclPath::from),
                value: result
                    .value()
                    .cloned()
                    .map(|value| store.original(value.into())),
                source_shape: result.source().cloned().map(Into::into),
                constraint_component: NamedNode::try_from(Term::from(
                    result.constraint_component().clone(),
                ))
                .map_err(RdfError::shacl)?,
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
    /// `prefixes` are prefix names and IRIs, such as `("sh", "http://www.w3.org/ns/shacl#")`, as
    /// in [`to_rdf`](crate::RdfView::to_rdf). The triples of each subject are written
    /// together, and the number of triples written is returned.
    ///
    /// In RDF/XML, a report whose focus node, value or message is a literal RDF/XML cannot hold
    /// (control characters other than tab and line feed, U+FFFE, U+FFFF) fails with
    /// [`RdfError::XmlUnsafeReport`] before anything is written.
    pub fn write<W: Write>(
        &self,
        writer: W,
        format: RdfFormat,
        prefixes: &[(&str, &str)],
    ) -> Result<usize, RdfError> {
        let serializer = serializer_with_prefixes(format, prefixes.iter().copied())?;
        let mut graph = ReportGraph::default();
        let root: NamedOrBlankNode = BlankNode::default().into();
        graph.add(
            &root,
            rdf::TYPE,
            NamedNodeRef::new_unchecked(ShaclVocab::SH_VALIDATION_REPORT),
        );
        graph.add(
            &root,
            NamedNodeRef::new_unchecked(ShaclVocab::SH_CONFORMS),
            Literal::from(self.conforms),
        );
        for result in &self.results {
            let node: NamedOrBlankNode = BlankNode::default().into();
            graph.add(
                &root,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_RESULT),
                node.clone(),
            );
            graph.add(
                &node,
                rdf::TYPE,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_VALIDATION_RESULT),
            );
            graph.add(
                &node,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_FOCUS_NODE),
                result.focus_node.clone(),
            );
            graph.add(
                &node,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_RESULT_SEVERITY),
                result.severity.clone(),
            );
            graph.add(
                &node,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_SOURCE_CONSTRAINT_COMPONENT),
                result.constraint_component.clone(),
            );
            if let Some(shape) = &result.source_shape {
                graph.add(
                    &node,
                    NamedNodeRef::new_unchecked(ShaclVocab::SH_SOURCE_SHAPE),
                    shape.clone(),
                );
            }
            if let Some(value) = &result.value {
                graph.add(
                    &node,
                    NamedNodeRef::new_unchecked(ShaclVocab::SH_VALUE),
                    value.clone(),
                );
            }
            if let Some(path) = &result.path {
                let path = graph.path(path);
                graph.add(
                    &node,
                    NamedNodeRef::new_unchecked(ShaclVocab::SH_RESULT_PATH),
                    path,
                );
            }
            for message in &result.messages {
                graph.add(
                    &node,
                    NamedNodeRef::new_unchecked(ShaclVocab::SH_RESULT_MESSAGE),
                    message.clone(),
                );
            }
        }
        if format == RdfFormat::RdfXml {
            if let Some(object) = graph
                .0
                .values()
                .flatten()
                .map(|(_, object)| object)
                .find(|object| !xml_text_safe(object.as_ref()))
            {
                return Err(RdfError::XmlUnsafeReport(object.clone()));
            }
        }
        let mut sink = TripleSink::new(serializer, writer);
        for (subject, properties) in &graph.0 {
            for (predicate, object) in properties {
                sink.push(subject.as_ref(), predicate.as_ref(), object.as_ref())?;
            }
        }
        Ok(sink.finish()?.written)
    }
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
        let wrap =
            |graph: &mut Self, predicate: NamedNodeRef<'static>, inner: &ShaclPath| -> Term {
                let node: NamedOrBlankNode = BlankNode::default().into();
                let inner = graph.path(inner);
                graph.add(&node, predicate, inner);
                node.into()
            };
        match path {
            ShaclPath::Predicate(predicate) => predicate.clone().into(),
            ShaclPath::Sequence(paths) => self.list(paths),
            ShaclPath::Alternative(paths) => {
                let node: NamedOrBlankNode = BlankNode::default().into();
                let list = self.list(paths);
                self.add(
                    &node,
                    NamedNodeRef::new_unchecked(ShaclVocab::SH_ALTERNATIVE_PATH),
                    list,
                );
                node.into()
            }
            ShaclPath::Inverse(path) => wrap(
                self,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_INVERSE_PATH),
                path,
            ),
            ShaclPath::ZeroOrMore(path) => wrap(
                self,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_ZERO_OR_MORE_PATH),
                path,
            ),
            ShaclPath::OneOrMore(path) => wrap(
                self,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_ONE_OR_MORE_PATH),
                path,
            ),
            ShaclPath::ZeroOrOne(path) => wrap(
                self,
                NamedNodeRef::new_unchecked(ShaclVocab::SH_ZERO_OR_ONE_PATH),
                path,
            ),
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
/// visible in the view, as [`to_rdf`](crate::RdfView::to_rdf) writes them. Every
/// pattern is answered by an [`EdgeScan`], which holds no lock between triples, so writers are
/// never blocked and the validator's own reads cannot deadlock.
struct ShaclStore {
    view: DynamicGraph,
    /// The predicate IRI of every layer whose term is an IRI.
    predicates: FxHashMap<LayerId, NamedNode>,
    /// The layer of every predicate IRI.
    layers: FxHashMap<NamedNode, LayerId>,
    /// The terms of the nodes read so far (at most [`TERM_CACHE_CAP`]).
    terms: DashMap<VID, Term, FxBuildHasher>,
    /// The nodes of `terms`, by [`term_key`] of their term, so a borrowed term can be looked up;
    /// of two terms with the same key only the first is kept.
    nodes: DashMap<u64, VID, FxBuildHasher>,
    /// About the number of entries of `terms` (`DashMap::len` locks every shard).
    num_terms: AtomicUsize,
    /// The literals read so far that the validator rewrites, by the form it gives them (see
    /// [`canonical_form`]): `None` if several literals have that form. At most
    /// [`TERM_CACHE_CAP`] forms are remembered; the literals of the others stay rewritten in
    /// results.
    originals: DashMap<Term, Option<Term>, FxBuildHasher>,
    /// About the number of entries of `originals`.
    num_originals: AtomicUsize,
    /// The literals of `sh:targetNode` the validator rewrites, by the form it gives them.
    targets: Arc<FxHashMap<Term, Term>>,
    /// Whether the shapes use classes, so that the validator reads the class triples (see
    /// [`triples`](NeighsRDF::triples)).
    classes: bool,
}

impl fmt::Debug for ShaclStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ShaclStore")
    }
}

impl ShaclStore {
    /// The store of `view`, with the literals of `sh:targetNode` that the validator rewrites.
    fn new(view: DynamicGraph, targets: Arc<FxHashMap<Term, Term>>, classes: bool) -> Self {
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
            nodes: DashMap::default(),
            num_terms: AtomicUsize::new(0),
            originals: DashMap::default(),
            num_originals: AtomicUsize::new(0),
            targets,
            classes,
        }
    }

    /// A full scan of `layer`.
    fn scan_layer(&self, layer: LayerId) -> EdgeScan {
        EdgeScan::all(self.view.clone(), Some(layer))
    }

    /// Whether the view has a visible `rdfs:subClassOf` triple.
    fn has_subclass_triples(&self) -> bool {
        self.layers
            .get(&rdfs::SUB_CLASS_OF.into_owned())
            .is_some_and(|&layer| self.scan_layer(layer).next().is_some())
    }

    /// The term of node `v`.
    fn node_term(&self, v: VID) -> Term {
        if let Some(term) = self.terms.get(&v) {
            return term.clone();
        }
        let term = term_of(&self.view.node_name(v));
        self.note_original(&term);
        if self.num_terms.load(Relaxed) < TERM_CACHE_CAP
            && self.terms.insert(v, term.clone()).is_none()
        {
            self.nodes.entry(term_key(term.as_ref())).or_insert(v);
            self.num_terms.fetch_add(1, Relaxed);
        }
        term
    }

    /// Remembers `term` if it is a literal the validator rewrites (see [`Self::originals`]).
    fn note_original(&self, term: &Term) {
        let Some(canonical) = canonical_form(term) else {
            return;
        };
        let full = self.num_originals.load(Relaxed) >= TERM_CACHE_CAP;
        match self.originals.entry(canonical) {
            Entry::Occupied(mut entry) => {
                if entry.get().as_ref() != Some(term) {
                    entry.insert(None);
                }
            }
            Entry::Vacant(entry) => {
                if !full {
                    entry.insert(Some(term.clone()));
                    self.num_originals.fetch_add(1, Relaxed);
                }
            }
        }
    }

    /// The term of the store that a term of a report stands for: a literal the validator
    /// rewrote is given back as written in the graph (if only one literal read during the
    /// validation has that form) or in `sh:targetNode`, unless the validation read that form
    /// itself in the view. Once the cache is full it is incomplete, so then any node of the
    /// graph with that form keeps it.
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
                if self.read_node(term.as_ref()).is_none()
                    && (self.num_terms.load(Relaxed) < TERM_CACHE_CAP
                        || name_of(term.as_ref())
                            .and_then(|name| lookup_node(&self.view, &name))
                            .is_none()) =>
            {
                original
            }
            _ => term,
        }
    }

    /// The node of `term`, if the validation read it as a term of a visible triple and the
    /// cache kept it.
    fn read_node(&self, term: TermRef<'_>) -> Option<VID> {
        let v = *self.nodes.get(&term_key(term))?;
        self.terms
            .get(&v)
            .is_some_and(|known| known.as_ref() == term)
            .then_some(v)
    }

    /// The node of a term, if the graph has one (whether or not the view shows it: the scans
    /// check that). A literal of `sh:targetNode` that the validator rewrote is looked up as
    /// written, unless its rewritten form is the object of a triple of the view.
    fn node(&self, term: TermRef<'_>) -> Option<VID> {
        if let Some(v) = self.read_node(term) {
            return Some(v);
        }
        let v = lookup_node(&self.view, &name_of(term)?);
        let target = match term {
            TermRef::Literal(literal) if !self.targets.is_empty() => {
                self.targets.get(&Term::from(literal.into_owned()))
            }
            _ => None,
        };
        let Some(target) = target else {
            return v;
        };
        match v {
            Some(v)
                if EdgeScan::matching(self.view.clone(), None, None, Some(v))
                    .next()
                    .is_some() =>
            {
                Some(v)
            }
            _ => lookup_node(&self.view, &name_of(target.as_ref())?),
        }
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
        Some(EdgeScan::matching(
            self.view.clone(),
            subject,
            layer,
            object,
        ))
    }
}

/// The key of a term in [`ShaclStore::nodes`].
fn term_key(term: TermRef<'_>) -> u64 {
    FxBuildHasher.hash_one(term)
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

impl NeighsRDF for ShaclStore {
    /// The `rdf:type` and `rdfs:subClassOf` triples of the view only, not all of them, and none
    /// when the shapes use no classes.
    ///
    /// The validator calls this only to build its class index, which reads only these triples
    /// and only class targets use; all other reads go through
    /// [`triples_matching`](Self::triples_matching). This holds for the pinned rudof version:
    /// recheck it when upgrading.
    fn triples(&self) -> Result<impl Iterator<Item = Triple>, RdfError> {
        let classes: &[NamedNodeRef<'_>] = if self.classes {
            &[rdf::TYPE, rdfs::SUB_CLASS_OF]
        } else {
            &[]
        };
        let layers: Vec<LayerId> = classes
            .iter()
            .filter_map(|predicate| self.layers.get(&predicate.into_owned()).copied())
            .collect();
        Ok(layers
            .into_iter()
            .flat_map(move |layer| self.scan_layer(layer))
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

    // A literal has no outgoing triples; the default errors, and the validator drops the node.
    fn objects_for(
        &self,
        subject: &Term,
        predicate: &NamedNode,
    ) -> Result<HashSet<Term>, RDFError> {
        let Ok(subject) = NamedOrBlankNode::try_from(subject.clone()) else {
            return Ok(HashSet::new());
        };
        Ok(self
            .scan(Some(&subject), Some(predicate), None)
            .into_iter()
            .flatten()
            .filter_map(|item| self.triple(item))
            .map(|triple| triple.object)
            .collect())
    }
}

/// Runs rudof's native engine on a store.
pub(crate) struct Proc<S>(pub(crate) S);

impl<S: NeighsRDF + fmt::Debug + Send + Sync + 'static> ShaclProcessor<S> for Proc<S> {
    fn store(&self) -> &S {
        &self.0
    }

    fn runner(_mode: &ShaclValidationMode, config: &ShaclConfig) -> Box<dyn Engine<S>> {
        Box::new(NativeEngine::new(config.recursion_semantics()))
    }
}
