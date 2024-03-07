use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use disk_faiss::merge_ondisk;
use disk_test::{FVecs, FVecsReader};
use faiss::{index_factory, read_index, write_index, Idx, Index, MetricType};
use rand::{self, RngCore};
use std::fs;

struct SerialGenerator {
    current: usize,
}

impl SerialGenerator {
    fn new() -> Self {
        Self { current: 0 }
    }
}

impl Iterator for SerialGenerator {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        self.current += 1;
        Some(self.current)
    }
}

// fn prepare_index_simple(index_type: &str, learn: &str, base: &str, dataset_id: &str) -> String {
//     let base_path = format!("/tmp/faiss-disk-test/{dataset_id}/");
//     fs::create_dir_all(base_path.clone()).unwrap();
//     let tmpfile = |filename: &str| base_path.clone() + filename.as_ref();

//     println!("Training index");
//     let FVecs {
//         dimensions,
//         vectors,
//     } = FVecs::from_file(learn).unwrap();
//     let mut index = index_factory(dimensions as u32, index_type, MetricType::InnerProduct).unwrap();
//     index.train(vectors.as_slice()).unwrap();

//     let index = index.into_ivf_flat().unwrap();
//     write_index(&index, tmpfile("trained.index")).unwrap();

//     let full_dataset = FVecs::from_file(base).unwrap();
//     let vectors = &full_dataset.vectors;
//     let num_vectors = vectors.len() / dimensions as usize;
//     println!("Splitting {num_vectors} vectors into 4 files");

//     let num_chunks = 4;
//     let vectors_per_chunk = num_vectors / num_chunks + 1;
//     let chunk_files: Vec<_> = (0..num_chunks)
//         .map(|chunk_number| tmpfile(format!("block_{chunk_number}.index").as_str()))
//         .collect();
//     let chunk_files: Vec<_> = chunk_files.iter().map(|f| f.as_str()).collect();

//     for ((chunk_number, chunk), filename) in vectors
//         .chunks(vectors_per_chunk * dimensions as usize)
//         .enumerate()
//         .zip(chunk_files.iter())
//     {
//         let first_id = vectors_per_chunk * chunk_number;
//         let ids_range = first_id..(first_id + chunk.len());
//         let ids: Vec<_> = ids_range.map(|id| Idx::from(id as i64)).collect();
//         let mut index = read_index(tmpfile("trained.index")).unwrap();
//         index.add_with_ids(chunk, ids.as_slice()).unwrap();
//         write_index(&index, filename).unwrap();
//     }

//     println!("merging indexes on disk");
//     merge_ondisk(
//         &tmpfile("trained.index"),
//         chunk_files,
//         &tmpfile("merged_index.ivfdata"),
//         &tmpfile("populated.index"),
//     );
//     tmpfile("populated.index")
// }

fn prepare_index(index_type: &str, learn: &str, base: &[&str], dataset_id: &str) -> String {
    let base_path = format!("/tmp/faiss-disk-test/{dataset_id}/");
    fs::create_dir_all(base_path.clone()).unwrap();
    let tmpfile = |filename: &str| base_path.clone() + filename.as_ref();

    println!("Training index");
    let FVecs {
        dimensions,
        vectors,
    } = FVecs::from_file(learn).unwrap();
    let mut index = index_factory(dimensions as u32, index_type, MetricType::InnerProduct).unwrap();
    index.train(vectors.as_slice()).unwrap();

    let index = index.into_ivf_flat().unwrap();
    write_index(&index, tmpfile("trained.index")).unwrap();

    let shards = base
        .iter()
        .enumerate()
        .map(|(index, filename)| {
            println!("opening new file {filename}");
            let dimensions = if index != 0 {
                Some(dimensions)
            } else {
                None
            };
            FVecsReader::from_file(filename, 10_000_000, dimensions).unwrap()})
        .flat_map(|reader| reader.into_iter());
    let shard_files_iter =
        || (0..usize::MAX).map(|file_num| tmpfile(format!("shard_{file_num}.index").as_str()));

    let mut id_gen = 0..usize::MAX;
    for (chunk, filename) in shards.zip(shard_files_iter()) {
        let num_vecs = chunk.len() / dimensions;
        println!("Processing {} vectors", num_vecs);
        let ids: Vec<_> = id_gen
            .by_ref()
            .take(num_vecs)
            .map(|id| Idx::from(id as i64))
            .collect();
        let mut index = read_index(tmpfile("trained.index")).unwrap();
        index.add_with_ids(&chunk, &ids).unwrap();
        println!("Writting them to {filename}");
        write_index(&index, filename).unwrap();
    }

    let all_shard_files: Vec<_> = shard_files_iter()
        .take_while(|filename| match fs::metadata(filename) {
            Err(_) => false,
            Ok(metadata) => metadata.is_file(),
        })
        .collect();
    let all_shard_files: Vec<_> = all_shard_files
        .iter()
        .map(|filename| filename.as_str())
        .collect();

    println!("merging indexes: {}", all_shard_files.join(", "));
    merge_ondisk(
        &tmpfile("trained.index"),
        all_shard_files,
        &tmpfile("merged_index.ivfdata"),
        &tmpfile("populated.index"),
    );
    tmpfile("populated.index")
}

fn bench(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    // let index_path = prepare_index(
    //     "IVF4096,Flat",
    //     "resources/sift/sift_learn.fvecs",
    //     &["resources/sift/sift_base.fvecs"],
    //     "sift",
    // );
    // let mut index = read_index(index_path).unwrap();
    // let query_batch = FVecs::from_file("resources/sift/sift_query.fvecs").unwrap();
    // let queries: Vec<_> = query_batch.split().collect();
    // c.bench_function("sift 1M", |b| {
    //     b.iter_batched(
    //         || queries[rng.next_u64() as usize % queries.len()],
    //         |query| index.search(query, 1).unwrap(),
    //         BatchSize::SmallInput,
    //     );
    // });

    // let index_path = prepare_index(
    //     "IVF4096,Flat",
    //     "resources/deep/deep10M.fvecs",
    //     &["resources/deep/deep10M.fvecs"],
    //     "deep10",
    // );
    // let mut index = read_index(index_path).unwrap();
    // let query_batch = FVecs::from_file("resources/deep/deep1B_queries.fvecs").unwrap();
    // let queries: Vec<_> = query_batch.split().collect();
    // c.bench_function("deep 10M", |b| {
    //     b.iter_batched(
    //         || queries[rng.next_u64() as usize % queries.len()],
    //         |query| index.search(query, 1).unwrap(),
    //         BatchSize::SmallInput,
    //     );
    // });

    let index_path = prepare_index(
        "IVF262144_HNSW32,Flat",
        "resources/deep/deep10M.fvecs",
        &[
            "resources/deep/base_00",
            "resources/deep/base_01",
            "resources/deep/base_02",
            "resources/deep/base_03",
            "resources/deep/base_00",
            "resources/deep/base_01",
            "resources/deep/base_02",
            "resources/deep/base_03",
            "resources/deep/base_00",
            "resources/deep/base_01",
            "resources/deep/base_02",
            "resources/deep/base_03",
        ],
        // &["resources/deep/deep10M.fvecs"],
        "deep",
    );
    let mut index = read_index(index_path).unwrap();
    let query_batch = FVecs::from_file("resources/deep/deep1B_queries.fvecs").unwrap();
    let queries: Vec<_> = query_batch.split().collect();
    c.bench_function("deep 325M", |b| {
        b.iter_batched(
            || queries[rng.next_u64() as usize % queries.len()],
            |query| index.search(query, 1).unwrap(),
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
