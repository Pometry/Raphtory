use super::ShaclLimit;
use crate::RdfError;
use oxigraph::model::{Graph, NamedOrBlankNodeRef, TermRef};
use rudof_rdf::rdf_core::vocabs::{RdfVocab, ShaclNodeExprVocab, ShaclVocab};
use rustc_hash::FxHashMap;

// longest chain rudof recurses along; errs on paths it would expand past the triple count
pub(super) fn nesting(graph: &Graph) -> Result<usize, RdfError> {
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
                    // shortcut: shape cycles under-count, so sized by triple count; use SCCs if hit
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
