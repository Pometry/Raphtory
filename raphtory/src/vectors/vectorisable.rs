use crate::{
    core::utils::errors::GraphResult,
    db::api::view::{internal::IntoDynamic, StaticGraphViewOps},
    vectors::{
        embedding_cache::EmbeddingCache, entity_id::EntityId, template::DocumentTemplate,
        vectorised_graph::VectorisedGraph, EmbeddingFunction,
    },
};
use arroy::{distances::Cosine, Database as ArroyDatabase, Writer};
use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use itertools::Itertools;
use rand::{rngs::StdRng, SeedableRng};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};
use tracing::info;

use super::{
    storage::{edge_vectors_path, node_vectors_path, VectorMeta},
    vectorised_graph::{EdgeDb, NodeDb, VectorDb},
    Embedding,
};

const CHUNK_SIZE: usize = 1000;

#[async_trait]
pub trait Vectorisable<G: StaticGraphViewOps> {
    /// Create a VectorisedGraph from the current graph
    ///
    /// # Arguments:
    ///   * embedding - the embedding function to translate documents to embeddings
    ///   * cache - the file to be used as a cache to avoid calling the embedding function
    ///   * overwrite_cache - whether or not to overwrite the cache if there are new embeddings
    ///   * template - the template to use to translate entities into documents
    ///   * verbose - whether or not to print logs reporting the progress
    ///
    /// # Returns:
    ///   A VectorisedGraph with all the documents/embeddings computed and with an initial empty selection
    async fn vectorise(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache: Arc<Option<EmbeddingCache>>,
        overwrite_cache: bool,
        template: DocumentTemplate,
        path: Option<&Path>,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>>;
}

const LMDB_MAX_SIZE: usize = 1024 * 1024 * 1024 * 1024; // 1TB // TODO: review !!!!!!!!!!!!

#[async_trait]
impl<G: StaticGraphViewOps + IntoDynamic + Send> Vectorisable<G> for G {
    async fn vectorise(
        &self,
        embedding: Box<dyn EmbeddingFunction>,
        cache: Arc<Option<EmbeddingCache>>,
        overwrite_cache: bool,
        template: DocumentTemplate,
        path: Option<&Path>,
        verbose: bool,
    ) -> GraphResult<VectorisedGraph<G>> {
        if verbose {
            info!("computing embeddings for nodes");
        }
        let nodes = self.nodes();
        let node_docs = nodes
            .iter()
            .filter_map(|node| template.node(node).map(|doc| (node.node.0 as u32, doc)));
        let node_path = path.map(node_vectors_path);
        let node_db = db_from_docs(node_docs, embedding.as_ref(), &cache, node_path).await?;

        if verbose {
            info!("computing embeddings for edges");
        }
        let edges = self.edges();
        let edge_docs = edges.iter().filter_map(|edge| {
            template
                .edge(edge)
                .map(|doc| (edge.edge.pid().0 as u32, doc))
        });
        let edge_path = path.map(edge_vectors_path);
        let edge_db = db_from_docs(edge_docs, embedding.as_ref(), &cache, edge_path).await?;

        if overwrite_cache {
            cache.iter().for_each(|cache| cache.dump_to_disk());
        }

        if let Some(path) = path {
            let meta = VectorMeta {
                template: template.clone(),
            };
            meta.write_to_path(path)?;
        }

        Ok(VectorisedGraph {
            source_graph: self.clone(),
            template,
            embedding: embedding.into(),
            cache_storage: cache.into(),
            node_db: NodeDb(node_db),
            edge_db: EdgeDb(edge_db),
        })
    }
}

pub(super) fn open_env(path: &Path) -> heed::Env {
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .map_size(LMDB_MAX_SIZE)
            .open(path)
    }
    .unwrap();
    // FIXME: remove unwrap
    env
}

async fn db_from_docs(
    docs: impl Iterator<Item = (u32, String)> + Send,
    embedding: &dyn EmbeddingFunction,
    cache: &Option<EmbeddingCache>,
    path: Option<PathBuf>,
) -> GraphResult<VectorDb> {
    let (env, tempdir) = match path {
        Some(path) => {
            std::fs::create_dir_all(&path).unwrap();
            (open_env(&path), None)
        }
        None => {
            let tempdir = tempfile::tempdir()?;
            (open_env(tempdir.path()), Some(tempdir.into()))
        }
    };

    // we will open the default LMDB unnamed database
    let mut wtxn = env.write_txn().unwrap(); // FIXME: remove unwrap
    let db: ArroyDatabase<Cosine> = env.create_database(&mut wtxn, None).unwrap();

    let vectors = compute_embeddings(docs, embedding, &cache);
    futures_util::pin_mut!(vectors);
    let first_vector = vectors.next().await;
    let dimensions = if let Some(Ok(first_vector)) = first_vector {
        let dimensions = first_vector.1.len(); // TODO: if vectors is empty, simply don't wirte anything!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        let writer = Writer::<Cosine>::new(db, 0, dimensions);

        while let Some(result) = vectors.next().await {
            let (id, vector) = result?;
            writer.add_item(&mut wtxn, id, &vector).unwrap();
        }

        // TODO: review this -> You can specify the number of trees to use or specify None.
        let mut rng = StdRng::seed_from_u64(42);
        dbg!();
        writer
            .builder(&mut rng)
            // .available_memory(1024 * 1024 * 1024 * 12) // 12 GB
            .progress(|progress| {
                // dbg!(progress);
            })
            .build(&mut wtxn)
            .unwrap();
        dimensions.into()
    } else {
        OnceLock::new()
    };

    wtxn.commit().unwrap();

    Ok(VectorDb {
        vectors: db,
        env,
        _tempdir: tempdir.into(),
        dimensions,
    })
}

// TODO: mvoe this to common place, utils maybe?
pub(super) fn compute_embeddings<'a, I>(
    documents: I,
    embedding: &'a dyn EmbeddingFunction,
    cache: &'a Option<EmbeddingCache>,
) -> impl futures_util::Stream<Item = GraphResult<(u32, Embedding)>> + Send + 'a
// TODO: review if this type make sense?
where
    I: Iterator<Item = (u32, String)> + Send + 'a,
{
    stream! {
        // tried to implement this using documents.chunks(), but the resulting type is not Send and breaks this function
        let mut buffer = Vec::with_capacity(CHUNK_SIZE);
        for document in documents {
            buffer.push(document);
            if buffer.len() >= CHUNK_SIZE {
                let chunk = compute_chunk(&buffer, embedding, cache).await?;
                for result in chunk {
                    yield Ok(result)
                }
                buffer.clear();
            }
        }
        if buffer.len() > 0 {
            let chunk = compute_chunk(&buffer, embedding, cache).await?;
            for result in chunk {
                yield Ok(result)
            }
        }
        // for chunk in documents.chunks(CHUNK_SIZE).into_iter().map(|chunk| chunk.collect::<Vec<_>>()) {
        //     match compute_chunk(&chunk, embedding, cache).await {
        //         Ok(results) => {
        //             for result in results {
        //                 yield Ok(result);
        //             }
        //         }
        //         Err(error) => {
        //             // Handle error if needed
        //         }
        //     }
        // }
    }
}

async fn compute_chunk(
    documents: &Vec<(u32, String)>,
    embedding: &dyn EmbeddingFunction,
    cache: &Option<EmbeddingCache>,
) -> GraphResult<Vec<(u32, Embedding)>> {
    let mut misses = vec![];
    let mut embedded = vec![];
    match cache {
        Some(cache) => {
            for (id, doc) in documents {
                let embedding = cache.get_embedding(&doc);
                match embedding {
                    Some(embedding) => embedded.push((*id, embedding)),
                    None => misses.push((*id, doc.clone())),
                }
            }
        }
        None => misses = documents.iter().cloned().collect(),
    };

    let texts = misses.iter().map(|(id, doc)| doc.clone()).collect_vec();
    let embeddings = if texts.is_empty() {
        vec![]
    } else {
        embedding.call(texts).await?
    };

    for ((id, doc), embedding) in misses.into_iter().zip(embeddings) {
        if let Some(cache) = cache {
            cache.upsert_embedding(&doc, embedding.clone())
        };
        embedded.push((id, embedding));
    }

    Ok(embedded)
}
