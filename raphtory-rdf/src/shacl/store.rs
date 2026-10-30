use crate::{
    graph::{layer, name, named},
    mapping::{iri_of, term_of},
    scan::matching,
    RdfError,
};
use oxigraph::model::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, TermRef, Triple};
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
use rudof_rdf::rdf_core::{Any, Matcher, NeighsRDF, Rdf};
use shacl::validator::{
    engine::{Engine, NativeEngine},
    processor::ShaclProcessor,
    ShaclConfig, ShaclValidationMode,
};
use std::fmt;

/// The triples of a view as rudof reads them: each lookup is one [`matching`] scan.
pub(super) struct Store {
    view: DynamicGraph,
    // the predicate of each layer, by `LayerId`
    predicates: Vec<NamedNode>,
}

impl Store {
    pub(super) fn new(view: DynamicGraph) -> Self {
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
        // with the class index off only shnex:nodesMatching gets here; it can recurse without end
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
