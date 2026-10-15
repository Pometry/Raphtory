//! Crash guards of the SHACL validator: a large-stack pool that turns panics into errors, and a
//! walk that rejects shapes graphs the validator would read too deep or too large.
use crate::RdfError;
use oxigraph::model::{vocab::rdf, BlankNodeRef, Graph, NamedOrBlankNodeRef, TermRef};
use rayon::{ThreadPool, ThreadPoolBuilder};
use rudof_rdf::rdf_core::vocabs::ShaclVocab;
use rustc_hash::{FxHashMap, FxHashSet};
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::OnceLock,
};

// a stack overflow cannot be caught and aborts the process
pub(crate) const STACK_SIZE: usize = 64 << 20;

// the validator reads lists, shapes and paths recursively and re-reads shared sub-paths
pub(crate) const MAX_LIST_LENGTH: usize = 10_000;
pub(crate) const MAX_SHAPES_SIZE: usize = 1_000_000;
pub(crate) const MAX_NESTING: usize = 256;

/// A limit of the validator that a shapes graph exceeds (see [`RdfError::ShaclLimit`]).
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShaclLimit {
    #[error("the shapes graph has a cyclic RDF list (its rdf:rest links lead back to a node)")]
    CyclicList,
    #[error("the shapes graph has an RDF list of more than {MAX_LIST_LENGTH} members")]
    ListTooLong,
    #[error("the shapes graph has a path that contains itself")]
    CyclicPath,
    #[error("the shapes graph nests shapes and paths more than {MAX_NESTING} deep")]
    TooDeep,
    #[error("the shapes graph expands to more than {MAX_SHAPES_SIZE} path steps and list members")]
    TooLarge,
    #[error("recursive SHACL shapes (a shape that refers to itself) are not accepted")]
    RecursiveShapes,
}

fn pool() -> Result<&'static ThreadPool, RdfError> {
    // only a built pool is kept, so a failed build is retried by the next call
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

// `install`, so the validator's rayon work runs on the large stacks too
pub(crate) fn run_guarded<R: Send>(
    what: &str,
    f: impl FnOnce() -> Result<R, RdfError> + Send,
) -> Result<R, RdfError> {
    pool()?.install(|| {
        catch_unwind(AssertUnwindSafe(f))
            .unwrap_or_else(|panic| Err(RdfError::panicked(what, panic)))
    })
}

// every subject is taken as a shape, over-approximating what the validator reads
pub(super) fn check_structure(shapes: &Graph) -> Result<(), RdfError> {
    let graph = ShapesGraph(shapes);
    let mut walk = Walk::default();
    for triple in shapes {
        graph.shape_depth(triple.subject, 0, &mut walk)?;
    }
    Ok(())
}

fn as_node(term: TermRef<'_>) -> Option<NamedOrBlankNodeRef<'_>> {
    match term {
        TermRef::NamedNode(node) => Some(node.into()),
        TermRef::BlankNode(node) => Some(node.into()),
        _ => None,
    }
}

struct ShapesGraph<'a>(&'a Graph);

#[derive(Default)]
struct Walk<'a> {
    paths: FxHashMap<BlankNodeRef<'a>, (usize, usize)>,
    // `None` while being measured: a shape that refers back to itself adds no depth
    shapes: FxHashMap<NamedOrBlankNodeRef<'a>, Option<usize>>,
    size: usize,
}

impl Walk<'_> {
    fn count(&mut self, n: usize) -> Result<(), RdfError> {
        self.size = self.size.saturating_add(n);
        if self.size > MAX_SHAPES_SIZE {
            return Err(RdfError::ShaclLimit(ShaclLimit::TooLarge));
        }
        Ok(())
    }
}

impl<'a> ShapesGraph<'a> {
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

    // every value of every construct: the validator reads the first that parses
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
        for triple in self.0.triples_for_subject(node) {
            match triple.predicate.as_str() {
                ShaclVocab::SH_ALTERNATIVE_PATH => parts.extend(self.list(triple.object)?),
                ShaclVocab::SH_ZERO_OR_MORE_PATH
                | ShaclVocab::SH_ONE_OR_MORE_PATH
                | ShaclVocab::SH_ZERO_OR_ONE_PATH
                | ShaclVocab::SH_INVERSE_PATH => parts.push(triple.object),
                _ => {}
            }
        }
        Ok((base, parts))
    }

    // (depth, expanded steps)
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

    // also counts its paths and lists towards `MAX_SHAPES_SIZE`
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

    fn shape_depth(
        &self,
        node: NamedOrBlankNodeRef<'a>,
        level: usize,
        walk: &mut Walk<'a>,
    ) -> Result<Option<usize>, RdfError> {
        if let Some(&depth) = walk.shapes.get(&node) {
            return Ok(depth);
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
}
