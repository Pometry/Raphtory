use criterion::{criterion_group, criterion_main, Criterion};
use rand::{
    distributions::{Alphanumeric, DistString},
    thread_rng, Rng,
};
use raphtory::core::DocumentInput;
use raphtory::db::graph::views::deletion_graph::GraphWithDeletions;
use raphtory::vectors::{document_template::DocumentTemplate, vectorisable::Vectorisable};
use raphtory::{core::entities::nodes::input_node::InputNode, prelude::*, vectors::Embedding};
use std::path::PathBuf;
use tokio::runtime::Runtime;

mod common;

async fn random_embedding(texts: Vec<String>) -> Vec<Embedding> {
    let mut rng = thread_rng();
    texts
        .iter()
        .map(|_| (0..1536).map(|_| rng.gen()).collect())
        .collect()
}

struct EmptyTemplate;

impl DocumentTemplate<Graph> for EmptyTemplate {
    fn graph(&self, _graph: &Graph) -> Box<dyn Iterator<Item = DocumentInput>> {
        Box::new(std::iter::empty())
    }

    fn node(
        &self,
        _node: &raphtory::db::graph::node::NodeView<Graph>,
    ) -> Box<dyn Iterator<Item = DocumentInput>> {
        Box::new(std::iter::once("".into()))
    }

    fn edge(
        &self,
        _edge: &raphtory::db::graph::edge::EdgeView<Graph, Graph>,
    ) -> Box<dyn Iterator<Item = DocumentInput>> {
        Box::new(std::iter::once("".into()))
    }
}

pub fn vectors(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let g = Graph::new();
    for id in 0..500_000 {
        g.add_node(0, id, NO_PROPS, None).unwrap();
    }
    for id in 0..500_000 {
        g.add_edge(0, 0, id, NO_PROPS, None).unwrap();
    }
    let query = rt.block_on(random_embedding(vec!["".to_owned()])).remove(0);
    let cache_path = || Some(PathBuf::from("/tmp/raphtory/vector-bench"));

    let native_vectorised_graph = rt.block_on(g.vectorise_with_template(
        Box::new(random_embedding),
        cache_path(),
        true,
        EmptyTemplate,
        false, // use faiss
        false,
    ));
    c.bench_function("native-index", |b| {
        b.iter(|| native_vectorised_graph.append_by_similarity(&query, 1, None));
    });

    let faiss_vectorised_graph = rt.block_on(g.vectorise_with_template(
        Box::new(random_embedding),
        cache_path(),
        true,
        EmptyTemplate,
        true, // use faiss
        false,
    ));
    c.bench_function("faiss-index", |b| {
        b.iter(|| faiss_vectorised_graph.append_by_similarity(&query, 1, None));
    });
}

struct FVecsContent {
    dimensions: u32,
    vectors: Vec<f32>,
}

use std::fs;
use std::io::Error as IoError;
use std::io::ErrorKind::InvalidData;

fn read_fvecs(file_name: &str) -> Result<FVecsContent, std::io::Error> {
    let data = fs::read(file_name)?;
    let (dim_data, vector_data) = data.split_at(4);
    let dim = dim_data
        .try_into()
        .map_err(|e| IoError::new(InvalidData, e))?;
    let dimensions = u32::from_le_bytes(dim);
    let vectors: Vec<_> = vector_data
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect();

    Ok(FVecsContent {
        dimensions,
        vectors,
    })
}

// fn read_fvecs(file_name: &str) -> Result<FVecsContent, std::io::Error> {
//     let mut file = File::open(file_name)?;
//     let mut buffer = [0u8; 4];
//     file.read_exact(&mut buffer);
//     let mut cursor = Cursor::new(buffer);
//     let value = cursor.read_i32::<std::io::LittleEndian>()?;
//     ...
// }

use faiss::index::io_flags::IoFlags;
use faiss::index::IndexImpl;
use faiss::index::{io::read_index_with_flags, NativeIndex};
use faiss::{index_factory, read_index, write_index, Idx, Index, MetricType};

// this is based on https://github.com/facebookresearch/faiss/blob/12b92e9fa5d8e8fb3da53c57af9ff007c826b1ee/contrib/ondisk.py
fn merge_on_disk(trained_index: &IndexImpl, shard_fnames: Vec<&str>, ivfdata_fname: &str) {
    // assert not isinstance(
    //     trained_index, faiss.IndexIVFPQR
    // ), "IndexIVFPQR is not supported as an on disk index."
    //

    let ivfs = shard_fnames.iter().map(|filename| {
        let index = read_index_with_flags(filename, IoFlags::MEM_MAP).unwrap();
        index.into_ivf_flat().unwrap();
    });

    let index_ivf = trained_index.into_ivf_flat().unwrap();

    assert_eq!(trained_index.ntotal(), 0, "works only on empty index")

    // FIXME: can't figure out next line !!
    // invlists = faiss.OnDiskInvertedLists(
    //     index_ivf.nlist, index_ivf.code_size, ivfdata_fname
    // )

    // # merge all the inverted lists
    // ivf_vector = faiss.InvertedListsPtrVector()
    // for ivf in ivfs:
    //     ivf_vector.push_back(ivf)

    // LOG.info("merge %d inverted lists " % ivf_vector.size())
    // ntotal = invlists.merge_from(ivf_vector.data(), ivf_vector.size())

    // # now replace the inverted lists in the output index
    // index.ntotal = index_ivf.ntotal = ntotal
    // index_ivf.replace_invlists(invlists, True)
    // invlists.this.disown()
}

pub fn faiss(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let tmpfile = |filename: &str| "/tmp/faiss-disk-test/".to_owned() + filename;

    println!("Training index");
    let FVecsContent {
        dimensions,
        vectors,
    } = read_fvecs("resources/sift/sift_learn.fvecs").unwrap();
    // println!("dimensions -> {dimensions}");
    // let sample: Vec<_> = flattened_vectors.iter().take(8).collect();
    // println!("sample -> {sample:?}");
    let mut index = index_factory(dimensions, "IVF4096,Flat", MetricType::InnerProduct).unwrap();
    index.train(vectors.as_slice()).unwrap();
    write_index(&index, tmpfile("trained.index")).unwrap();

    println!("Splitting vectors into files");
    let vectors = read_fvecs("resources/sift/sift_base.fvecs")
        .unwrap()
        .vectors;

    let num_vectors = vectors.len() / dimensions as usize;
    let vectors_per_chunk = num_vectors / 4 + 1;

    for (chunk_number, chunk) in vectors
        .chunks(vectors_per_chunk * dimensions as usize)
        .enumerate()
    {
        let first_id = vectors_per_chunk * chunk_number;
        let ids_range = first_id..(first_id + chunk.len());
        let ids: Vec<_> = ids_range.map(|id| Idx::from(id as i64)).collect();
        let mut index = read_index(tmpfile("trained.index")).unwrap();
        index.add_with_ids(chunk, ids.as_slice()).unwrap();
        let block_name = format!("block_{chunk_number}.index");
        write_index(&index, tmpfile(block_name.as_str())).unwrap();
    }

    println!("loading trained index");
    let index = read_index(tmpfile("trained.index")).unwrap();

    // let block_fnames: Vec<_> = (0..4).map(|n_chunk| tmpfile(format!("block_{n_chunk}.index").as_str())).collect();
    // merge_ondisk(index, block_fnames, tmpdir + "merged_index.ivfdata").unwrap();
    // write_index(&index, tmpdir + "populated.index").unwrap();

    // c.bench_function("faiss-index", |b| {
    //     b.iter(|| ());
    // });
}

criterion_group!(benches, faiss);
criterion_main!(benches);
