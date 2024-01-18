use std::{
    io::BufReader,
    path::{Path, PathBuf},
};

use arrow2::{
    array::{Array, MutableUtf8Array, PrimitiveArray, Utf8Array},
    chunk::Chunk,
    compute::{
        merge_sort,
        sort::{self, SortColumn, SortOptions},
    },
    datatypes::{Field, Schema},
    io::parquet::read,
};
use itertools::Itertools;
use rayon::prelude::*;

use crate::arrow::mmap::{mmap_batch, write_batches};

use super::{global_order::SortedGIDs, Error, GraphChunk};

#[derive(Debug)]
pub struct ExternalEdgeList<'a, P: AsRef<Path>> {
    pub layer: &'a str,
    pub path: P,
    pub src_col: &'a str,
    pub src_hash_col: &'a str,
    pub dst_col: &'a str,
    pub dst_hash_col: &'a str,
    pub time_col: &'a str,
    pub parquet_files: Vec<PathBuf>,
}

impl<'a, P: AsRef<Path>> ExternalEdgeList<'a, P> {
    pub fn new(
        layer: &'a str,
        path: P,
        src_col: &'a str,
        src_hash_col: &'a str,
        dst_col: &'a str,
        dst_hash_col: &'a str,
        time_col: &'a str,
    ) -> Result<Self, Error> {
        let parquet_files = if std::fs::read_dir(&path).is_ok() {
            std::fs::read_dir(&path)?
                .map(|res| res.unwrap())
                .filter(|e| {
                    e.path()
                        .extension()
                        .filter(|ext| ext == &"parquet")
                        .is_some()
                })
                .sorted_by(|f1, f2| f1.path().cmp(&f2.path()))
                .map(|e| e.path())
                .collect_vec()
        } else {
            vec![path.as_ref().to_path_buf()]
        };

        Ok(Self {
            layer,
            path,
            src_col,
            src_hash_col,
            dst_col,
            dst_hash_col,
            time_col,
            parquet_files,
        })
    }

    pub(crate) fn layer(&self) -> &'a str {
        self.layer
    }

    pub(crate) fn par_sorted_nodes(
        &self,
    ) -> impl Iterator<Item = (Box<dyn Array>, Box<dyn Array>)> + '_ {
        let concurrent_files = 16;
        let file_groups = self.parquet_files.iter().chunks(concurrent_files).into_iter().map(|c|c.cloned().collect_vec()).collect_vec();

        file_groups.into_iter().flat_map(|paths| {
            let now = std::time::Instant::now();

            let res = paths
                .into_par_iter()
                .flat_map(|path| {
                    sort_within_chunks_iter(
                        path,
                        self.src_col,
                        self.src_hash_col,
                        self.dst_col,
                        self.dst_hash_col,
                    )
                    .expect("failed to sort within chunks")
                })
                .reduce_with(|(lhs_hash, lhs), (rhs_hash, rhs)| {
                    merge_sort_dest_src((&lhs_hash, &lhs), (&rhs_hash, &rhs))
                        .expect("failed to merge sort destinations and sources")
                });

            println!(
                "DONE sorting time: {:?}, len: {}",
                now.elapsed(),
                res.as_ref().map(|(_, arr)| arr.len()).unwrap_or(0)
            );
            res
        })
    }

    pub(crate) fn files(&self) -> &[PathBuf] {
        &self.parquet_files
    }
}
pub(crate) fn sort_within_chunks_iter<P: AsRef<Path>>(
    parquet_file: P,
    src_col: &str,
    src_hash_col: &str,
    dst_col: &str,
    dst_hash_col: &str,
) -> Result<impl ParallelIterator<Item = (Box<dyn Array>, Box<dyn Array>)>, Error> {
    let thread_id = std::thread::current().id();
    println!(
        "pre sort reading file: {:?} on thread {thread_id:?}",
        parquet_file.as_ref()
    );
    let file = std::fs::File::open(&parquet_file)?;
    let mut reader = BufReader::new(file);
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata)?;

    let schema = schema.filter(|_, field| {
        field.name == src_col
            || field.name == dst_col
            || field.name == src_hash_col
            || field.name == dst_hash_col
    });

    let src_idx = schema
        .fields
        .iter()
        .position(|f| f.name == src_col)
        .unwrap();
    let dst_idx = schema
        .fields
        .iter()
        .position(|f| f.name == dst_col)
        .unwrap();
    let src_hash_idx = schema
        .fields
        .iter()
        .position(|f| f.name == src_hash_col)
        .unwrap();
    let dst_hash_idx = schema
        .fields
        .iter()
        .position(|f| f.name == dst_hash_col)
        .unwrap();

    let reader = read::FileReader::new(
        reader,
        metadata.row_groups,
        schema,
        Some(2_000_000),
        None,
        None,
    );
    let sorted_nodes = reader.flatten().par_bridge().map(move |chunk| {
        let (dest_hash, dest) = sort_destinations(&chunk[dst_hash_idx], &chunk[dst_idx])
            .expect("failed to sort destinations");
        let (nodes_hash, nodes) =
            merge_sort_dest_src((&dest_hash, &dest), (&chunk[src_hash_idx], &chunk[src_idx]))
                .expect("failed to merge sort destinations and sources");

        (nodes_hash, nodes)
    });

    Ok(sorted_nodes)
}

pub(crate) fn sort_destinations(
    dest_hashes: &Box<dyn Array>,
    dest: &Box<dyn Array>,
) -> Result<(Box<dyn Array>, Box<dyn Array>), Error> {
    let mut arr = sort::lexsort::<i32>(
        &vec![
            SortColumn {
                values: dest_hashes.as_ref(),
                options: None,
            },
            SortColumn {
                values: dest.as_ref(),
                options: None,
            },
        ],
        None,
    )?;
    let sorted_hashes = arr.remove(0);
    let sorted_dest = arr.remove(0);
    Ok((sorted_hashes, sorted_dest))
}

pub(crate) fn merge_sort_dest_src(
    lhs: (&Box<dyn Array>, &Box<dyn Array>),
    rhs: (&Box<dyn Array>, &Box<dyn Array>),
) -> Result<(Box<dyn Array>, Box<dyn Array>), Error> {
    let options = SortOptions {
        descending: false,
        nulls_first: false,
    };

    let (rhs_hash, rhs_nodes) = rhs;
    let (lhs_hash, lhs_nodes) = lhs;

    let hashes = vec![rhs_hash.as_ref(), lhs_hash.as_ref()];
    let nodes = vec![rhs_nodes.as_ref(), lhs_nodes.as_ref()];
    let pairs = vec![(hashes.as_ref(), &options), (nodes.as_ref(), &options)];

    let slices = merge_sort::slices(pairs.as_ref())?;

    let new_hashes = merge_sort::take_arrays(
        &[rhs_hash.as_ref(), lhs_hash.as_ref()],
        slices.iter().copied(),
        None,
    );
    let merged = merge_sort::take_arrays(
        &[rhs_nodes.as_ref(), lhs_nodes.as_ref()],
        slices.iter().copied(),
        None,
    );

    if let Some(hash_arr) = new_hashes.as_any().downcast_ref::<PrimitiveArray<i64>>() {
        let mut deduped_hash: Vec<i64> = Vec::with_capacity(hash_arr.len());

        if let Some(arr) = merged.as_any().downcast_ref::<PrimitiveArray<u64>>() {
            let mut deduped: Vec<u64> = Vec::with_capacity(arr.len());
            for (h, v) in hash_arr.values().iter().zip(arr.values().iter()).dedup() {
                deduped_hash.push(*h);
                deduped.push(*v);
            }
            let arr = PrimitiveArray::from_vec(deduped);
            let next_hash = PrimitiveArray::from_vec(deduped_hash);
            Ok((next_hash.boxed(), arr.to_boxed()))
        } else if let Some(arr) = merged.as_any().downcast_ref::<Utf8Array<i64>>() {
            let mut deduped = MutableUtf8Array::<i64>::new();
            for (h, v) in hash_arr
                .values()
                .iter()
                .zip(arr.into_iter().flatten())
                .dedup()
            {
                deduped_hash.push(*h);
                deduped.push(Some(v));
            }
            let next_hash = PrimitiveArray::from_vec(deduped_hash);
            let arr: Utf8Array<i64> = deduped.into();
            Ok((next_hash.boxed(), arr.to_boxed()))
        } else if let Some(arr) = merged.as_any().downcast_ref::<Utf8Array<i32>>() {
            let mut deduped = MutableUtf8Array::<i32>::new();
            for (h, v) in hash_arr
                .values()
                .iter()
                .zip(arr.into_iter().flatten())
                .dedup()
            {
                deduped_hash.push(*h);
                deduped.push(Some(v));
            }
            let next_hash = PrimitiveArray::from_vec(deduped_hash);
            let arr: Utf8Array<i32> = deduped.into();
            Ok((next_hash.boxed(), arr.to_boxed()))
        } else if let Some(arr) = merged.as_any().downcast_ref::<PrimitiveArray<i64>>() {
            let mut deduped: Vec<i64> = Vec::with_capacity(arr.len());
            for (h, v) in hash_arr.values().iter().zip(arr.values().iter()).dedup() {
                deduped_hash.push(*h);
                deduped.push(*v);
            }
            let arr = PrimitiveArray::from_vec(deduped);
            let next_hash = PrimitiveArray::from_vec(deduped_hash);
            Ok((next_hash.boxed(), arr.to_boxed()))
        } else {
            Err(Error::InvalidTypeColumn(format!(
                "src or dst column need to be u64, i64 or string, found: {:?}",
                merged.data_type()
            )))
        }
    } else {
        Err(Error::InvalidTypeColumn(format!(
            "hash column need to be i64 found: {:?}",
            new_hashes.data_type()
        )))
    }
}

pub(crate) fn read_file_chunks<P: AsRef<Path>>(
    parquet_file: P,
    src_col: &str,
    dst_col: &str,
    time_col: &str,
) -> Result<impl Iterator<Item = GraphChunk>, Error> {
    println!("reading file: {:?}", parquet_file.as_ref());

    let file = std::fs::File::open(&parquet_file)?;
    let mut reader = BufReader::new(file);
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata)?;
    let src_col_field = schema
        .fields
        .iter()
        .find(|field| &field.name == src_col)
        .ok_or_else(|| Error::ColumnNotFound(src_col.to_owned()))?;
    let dst_col_field = schema
        .fields
        .iter()
        .find(|field| &field.name == dst_col)
        .ok_or_else(|| Error::ColumnNotFound(dst_col.to_owned()))?;
    let time_col_field = schema
        .fields
        .iter()
        .find(|field| &field.name == time_col)
        .ok_or_else(|| Error::ColumnNotFound(time_col.to_owned()))?;

    let schema = Schema::from(vec![
        src_col_field.clone(),
        dst_col_field.clone(),
        time_col_field.clone(),
    ])
    .with_metadata(schema.metadata);

    let reader = read::FileReader::new(
        reader,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );
    Ok(reader
        .flatten()
        .map(|chunk| GraphChunk::from_chunk(chunk, 0, 1)))
}

pub(crate) fn make_global_ordering<'a, P: AsRef<Path> + Sync + Send>(
    sorted_gids_path: impl AsRef<Path>,
    edge_lists: &[ExternalEdgeList<'a, P>],
    num_threads: usize,
) -> Result<SortedGIDs, Error> {
    let now = std::time::Instant::now();

    let (node_hashes, nodes) = if sorted_gids_path.as_ref().exists() {
        read_sorted_gids(sorted_gids_path.as_ref())?
    } else {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        let (node_hashes, nodes) = thread_pool.install(|| {
            edge_lists
                .iter()
                .filter_map(|e_list| {
                    let res = e_list
                        .par_sorted_nodes()
                        .reduce(|(l_hash, l), (r_hash, r)| {
                            merge_sort_dest_src((&l_hash, &l), (&r_hash, &r)).unwrap()
                        });
                    res
                })
                .reduce(|(l_hash, l), (r_hash, r)| {
                    merge_sort_dest_src((&l_hash, &l), (&r_hash, &r)).unwrap()
                })
                .ok_or_else(|| Error::NoEdgeLists)
        })?;

        persist_sorted_gids(sorted_gids_path, node_hashes.clone(), nodes.clone())?;
        (node_hashes, nodes)
    };

    println!(
        "DONE sorting time: {:?}, len: {}",
        now.elapsed(),
        nodes.len()
    );

    (Some(node_hashes), nodes).try_into()
}

fn merge_inter_stage(
    new_vec: Vec<(Box<dyn Array>, Box<dyn Array>)>,
) -> (Box<dyn Array>, Box<dyn Array>) {
    let res = new_vec
        .into_par_iter()
        .reduce_with(|(lhs_hash, lhs), (rhs_hash, rhs)| {
            merge_sort_dest_src((&lhs_hash, &lhs), (&rhs_hash, &rhs))
                .expect("failed to merge sort destinations and sources")
        })
        .unwrap();
    res
}

pub(crate) fn persist_sorted_gids(
    sorted_gids_path: impl AsRef<Path>,
    node_hashes: Box<dyn Array>,
    nodes: Box<dyn Array>,
) -> Result<(), Error> {
    let schema = Schema::from(vec![
        Field::new("hash", node_hashes.data_type().clone(), false),
        Field::new("gid", nodes.data_type().clone(), false),
    ]);
    let chunk = [Chunk::try_new(vec![node_hashes, nodes])?];
    write_batches(sorted_gids_path.as_ref(), schema, &chunk)?;
    Ok(())
}

pub(crate) fn read_sorted_gids(
    sorted_gids_path: impl AsRef<Path>,
) -> Result<(Box<dyn Array>, Box<dyn Array>), Error> {
    let chunk = unsafe { mmap_batch(sorted_gids_path.as_ref(), 0)? };
    let arrays = chunk.into_arrays();
    let hash_arr = arrays[0].clone();
    let gid_arr = arrays[1].clone();
    Ok((hash_arr, gid_arr))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sort_merge_dedup_2_cols() {
        let rhs = PrimitiveArray::from_vec(vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10]).boxed();
        let hash_rhs = PrimitiveArray::from_vec(vec![4i64, 5, 5, 6, 7, 7, 8, 8, 9, 11]).boxed();

        let lhs = PrimitiveArray::from_vec(vec![0u64, 9, 10, 6, 7, 8, 3, 1, 4, 15]).boxed();
        let hash_lhs = PrimitiveArray::from_vec(vec![-3i64, 9, 11, 7, 8, 8, 5, 4, 6, 17]).boxed();

        let (hash_lhs, lhs) = sort_destinations(&hash_lhs, &lhs).unwrap();
        let (actual_h, actual_v) =
            merge_sort_dest_src((&hash_lhs, &lhs), (&hash_rhs, &rhs)).unwrap();

        let expected_v =
            PrimitiveArray::from_vec(vec![0u64, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15]).boxed();
        let expected_h =
            PrimitiveArray::from_vec(vec![-3i64, 4, 5, 5, 6, 7, 7, 8, 8, 9, 11, 17]).boxed();

        assert_eq!(actual_h, expected_h);
        assert_eq!(actual_v, expected_v);
    }
}
