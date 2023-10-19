use crate::{
    db::graph::{edge::EdgeView, vertex::VertexView},
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
    vectors::{entity_id::EntityId, EntityDocuments},
};
use itertools::Itertools;
use regex::Regex;
use tantivy::HasLen;

const DOCUMENT_MAX_SIZE: usize = 1000;

pub(crate) trait DocumentSource: Sized {
    // type Output: Iterator<Item=EntityDocument>;
    fn generate_docs<T, I>(&self, template: &T) -> EntityDocuments
    where
        T: Fn(&Self) -> I,
        I: Iterator<Item = String>;
}

impl<G: GraphViewOps> DocumentSource for VertexView<G> {
    fn generate_docs<T, I>(&self, template: &T) -> EntityDocuments
    where
        T: Fn(&Self) -> I,
        I: Iterator<Item = String>,
    {
        let documents = template(self)
            .flat_map(|text| split_text_by_line_breaks(text, DOCUMENT_MAX_SIZE).into_iter())
            .collect_vec();
        EntityDocuments {
            id: EntityId::Node { id: self.id() },
            documents,
        }
    }
}

impl<G: GraphViewOps> DocumentSource for EdgeView<G> {
    fn generate_docs<T, I>(&self, template: &T) -> EntityDocuments
    where
        T: Fn(&Self) -> I,
        I: Iterator<Item = String>,
    {
        let documents = template(self)
            .flat_map(|text| split_text_by_line_breaks(text, DOCUMENT_MAX_SIZE).into_iter())
            .collect_vec();
        EntityDocuments {
            id: EntityId::Edge {
                src: self.src().id(),
                dst: self.dst().id(),
            },
            documents,
        }
    }
}

/// Splits the input text in chunks of no more than max_size trying to use line breaks
/// as much as possible
fn split_text_by_line_breaks(text: String, max_size: usize) -> Vec<String> {
    // TODO: maybe use async_stream crate instead
    let break_line = Regex::new(r"(.*\n)").unwrap(); // we don't want to remove the pattern!
    let mut substrings = break_line.split(&text.as_str());
    let first_substring = substrings.next().unwrap().to_owned();
    let mut chunks = vec![first_substring];

    // are we sure this excludes the first value? TODO: make a test for this function
    for substring in substrings {
        let last_chunk = chunks.last_mut().unwrap(); // at least one element
        if substring.len() > max_size {
            for subsubstring in split_text_with_constant_size(substring, max_size).into_iter() {
                chunks.push(subsubstring.to_owned());
            }
        } else if last_chunk.len() + substring.len() <= max_size {
            last_chunk.push_str(substring)
        } else {
            chunks.push(substring.to_owned());
        }
    }

    chunks
}

fn split_text_with_constant_size(input: &str, chunk_size: usize) -> Vec<&str> {
    let mut substrings = Vec::new();
    let mut start = 0;

    while start < input.len() {
        // Use char_indices to ensure we split the string at valid UTF-8 boundaries
        let end = input[start..]
            .char_indices()
            .take(chunk_size)
            .map(|(i, _)| i)
            .last()
            .map_or(start + chunk_size, |idx| start + idx);

        let substring = &input[start..end];
        substrings.push(substring);

        start = end;
    }

    substrings
}
