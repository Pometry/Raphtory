use crate::vectors::{document_ref::DocumentRef, Embedding};
use itertools::Itertools;

pub(crate) fn score_documents<'a, I>(
    query: &'a Embedding,
    documents: I,
) -> impl Iterator<Item = (DocumentRef, f32)> + 'a
where
    I: IntoIterator<Item = DocumentRef> + 'a,
{
    documents.into_iter().map(|doc| {
        let score = cosine(query, &doc.embedding);
        (doc, score)
    })
}

/// Returns the top k nodes in descending order
pub(crate) fn find_top_k<'a, I>(
    elements: I,
    k: usize,
) -> impl Iterator<Item = (DocumentRef, f32)> + 'a
where
    I: Iterator<Item = (DocumentRef, f32)> + 'a,
{
    // TODO: add optimization for when this is used -> don't maintain more candidates than the max number of documents to return !!!
    elements
        .sorted_by(|(_, score1), (_, score2)| score1.partial_cmp(&score2).unwrap().reverse())
        // We use reverse because default sorting is ascending but we want it descending
        .take(k)
}

fn cosine(vector1: &Embedding, vector2: &Embedding) -> f32 {
    assert_eq!(vector1.len(), vector2.len());

    let dot_product: f32 = vector1.iter().zip(vector2.iter()).map(|(x, y)| x * y).sum();
    let x_length: f32 = vector1.iter().map(|x| x * x).sum();
    let y_length: f32 = vector2.iter().map(|y| y * y).sum();
    // TODO: store the length of the vector as well so we don't need to recompute it
    // Vectors are already normalized for ada but nor for all the models:
    // see: https://platform.openai.com/docs/guides/embeddings/which-distance-function-should-i-use

    let normalized = dot_product / (x_length.sqrt() * y_length.sqrt());
    // println!("cosine for {vector1:?} and {vector2:?} is {normalized}");
    assert!(
        normalized <= 1.001,
        "not valid result: {normalized} for vectors:\n{vector1:?}\n{vector2:?}"
    );
    assert!(
        normalized >= -1.001,
        "not valid result: {normalized} for vectors:\n{vector1:?}\n{vector2:?}"
    );
    normalized
}
