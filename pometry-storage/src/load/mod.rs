use crate::{
    arrow2::{
        array::{Array, PrimitiveArray, Utf8Array},
        datatypes::{ArrowDataType as DataType, ArrowSchema as Schema, Field},
        record_batch::RecordBatch as Chunk,
    },
    compute::sort,
    disk_hmap::DiskHashMap,
    file_prefix::GraphPaths,
    load::{
        mmap::{mmap_batch, write_batches},
        parquet_reader::{read_file_metadata, read_parquet_file},
    },
};
use itertools::Itertools;
use polars_arrow::{
    array::{MutableBinaryViewArray, Utf8ViewArray},
    compute::concatenate::concatenate,
    datatypes::ArrowSchema,
    legacy::error::PolarsResult,
};
use polars_parquet::read;
use rayon::prelude::*;
use std::{
    fmt::Debug,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};
use tracing::instrument;

pub mod ipc;
pub mod mmap;
pub(crate) mod parquet_reader;
pub(crate) mod parquet_source;
pub mod writer;

use super::RAError;

/// Convert all Utf8View columns to LargeUtf8
pub fn read_schema(file_metadata: &read::FileMetaData) -> PolarsResult<ArrowSchema> {
    let schema = read::infer_schema(file_metadata)?;
    let fields = schema
        .fields
        .iter()
        .map(|f| {
            if f.data_type == DataType::Utf8View {
                Field::new(f.name.clone(), DataType::LargeUtf8, f.is_nullable)
            } else {
                f.clone()
            }
        })
        .collect::<Vec<_>>();

    Ok(ArrowSchema::from(fields).with_metadata(schema.metadata))
}

#[derive(Debug)]
pub struct ExternalEdgeList<'a, P: AsRef<Path>> {
    pub layer: &'a str,
    pub path: P,
    pub src_col: &'a str,
    pub dst_col: &'a str,
    pub time_col: &'a str,
    pub parquet_files: Vec<PathBuf>,
    pub exclude_edge_props: Vec<&'a str>,
}

impl<'a, P: AsRef<Path>> ExternalEdgeList<'a, P> {
    pub fn new(
        layer: &'a str,
        path: P,
        src_col: &'a str,
        dst_col: &'a str,
        time_col: &'a str,
        exclude_edge_props: Vec<&'a str>,
    ) -> Result<Self, RAError> {
        let parquet_files = list_parquet_files(path.as_ref())?;

        Ok(Self {
            layer,
            path,
            src_col,
            dst_col,
            time_col,
            parquet_files,
            exclude_edge_props,
        })
    }

    pub(crate) fn layer(&self) -> &'a str {
        self.layer
    }

    pub(crate) fn par_sorted_nodes(
        &self,
        read_chunk_size: Option<usize>,
        concurrent_files: usize,
    ) -> impl Iterator<Item = Box<dyn Array>> + '_ {
        let file_groups = self
            .parquet_files
            .iter()
            .chunks(concurrent_files)
            .into_iter()
            .map(|c| c.cloned().collect_vec())
            .collect_vec();

        file_groups.into_iter().flat_map(move |paths| {
            paths
                .into_par_iter()
                .flat_map(|path| {
                    sort_within_chunks_iter(path, read_chunk_size, self.src_col, self.dst_col)
                        .expect("failed to sort within chunks")
                })
                .reduce_with(|lhs, rhs| {
                    merge_sort_dest_src(&lhs, &rhs)
                        .expect("failed to merge sort destinations and sources")
                })
        })
    }

    pub(crate) fn files(&self) -> &[PathBuf] {
        &self.parquet_files
    }
}

pub fn list_parquet_files<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>, RAError> {
    let parquet_files = if std::fs::read_dir(path.as_ref()).is_ok() {
        std::fs::read_dir(path)?
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
    Ok(parquet_files)
}

pub(crate) fn sort_within_chunks_iter<P: AsRef<Path>>(
    parquet_file: P,
    read_chunk_size: Option<usize>,
    src_col: &str,
    dst_col: &str,
) -> Result<impl ParallelIterator<Item = Box<dyn Array>>, RAError> {
    let file = std::fs::File::open(&parquet_file)?;
    let mut reader = BufReader::new(file);
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read_schema(&metadata)?;

    let schema = schema.filter(|_, field| field.name == src_col || field.name == dst_col);

    let src_idx = find_col_pos(src_col, &schema)?;
    let dst_idx = find_col_pos(dst_col, &schema)?;

    let reader = read::FileReader::new(
        reader,
        metadata.row_groups,
        schema,
        read_chunk_size,
        None,
        None,
    );
    let sorted_nodes = reader.flatten().par_bridge().map(move |chunk| {
        let dest = sort_destinations(&chunk[dst_idx]).expect("failed to sort destinations");

        merge_sort_dest_src(&dest, &chunk[src_idx])
            .expect("failed to merge sort destinations and sources")
    });

    Ok(sorted_nodes)
}

fn find_col_pos(src_col: &str, schema: &Schema) -> Result<usize, RAError> {
    schema
        .fields
        .iter()
        .position(|f| f.name == src_col)
        .ok_or(RAError::ColumnNotFound(format!(
            "Column {} not found in schema: {:?}",
            src_col, schema
        )))
}

pub(crate) fn sort_destinations(dest: &Box<dyn Array>) -> Result<Box<dyn Array>, RAError> {
    sort(dest.as_ref())
}

pub(crate) fn merge_sort_dest_src(
    lhs_nodes: &Box<dyn Array>,
    rhs_nodes: &Box<dyn Array>,
) -> Result<Box<dyn Array>, RAError> {
    if rhs_nodes.data_type() == &DataType::UInt64 {
        let rhs_nodes = rhs_nodes
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();
        let lhs_nodes = lhs_nodes
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .unwrap();

        let res: Vec<_> = rhs_nodes
            .values_iter()
            .copied()
            .merge(lhs_nodes.values_iter().copied())
            .dedup()
            .collect();

        Ok(PrimitiveArray::from_vec(res).boxed())
    } else if rhs_nodes.data_type() == &DataType::Int64 {
        let rhs_nodes = rhs_nodes
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap();
        let lhs_nodes = lhs_nodes
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .unwrap();

        let res: Vec<_> = rhs_nodes
            .values_iter()
            .copied()
            .merge(lhs_nodes.values_iter().copied())
            .dedup()
            .collect();

        Ok(PrimitiveArray::from_vec(res).boxed())
    } else if rhs_nodes.data_type() == &DataType::LargeUtf8 {
        let rhs_nodes = rhs_nodes.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
        let lhs_nodes = lhs_nodes.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();

        let res: Vec<_> = rhs_nodes.into_iter().merge(lhs_nodes).dedup().collect();

        let arr: Utf8Array<i64> = Utf8Array::from(res);
        Ok(arr.boxed())
    } else if rhs_nodes.data_type() == &DataType::Utf8 {
        let rhs_nodes = rhs_nodes.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
        let lhs_nodes = lhs_nodes.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();

        let res: Vec<_> = rhs_nodes.into_iter().merge(lhs_nodes).dedup().collect();

        let arr: Utf8Array<i32> = Utf8Array::from(res);
        Ok(arr.boxed())
    } else if rhs_nodes.data_type() == &DataType::Utf8View {
        let rhs_nodes = rhs_nodes.as_any().downcast_ref::<Utf8ViewArray>().unwrap();
        let lhs_nodes = lhs_nodes.as_any().downcast_ref::<Utf8ViewArray>().unwrap();

        let res = rhs_nodes
            .values_iter()
            .merge(lhs_nodes.values_iter())
            .dedup();

        let builder = MutableBinaryViewArray::from_values_iter(res);
        let arr: Utf8ViewArray = builder.into();
        Ok(arr.boxed())
    } else {
        Err(RAError::InvalidTypeColumn(format!(
            "src or dst column need to be u64, i64 or string, found: {:?}",
            rhs_nodes.data_type()
        )))
    }
}

#[instrument(level = "debug", skip(edge_lists))]
pub(crate) fn make_global_ordering<P: AsRef<Path> + Sync + Send>(
    graph_dir: impl AsRef<Path> + Debug,
    edge_lists: &[ExternalEdgeList<P>],
    read_chunk_size: Option<usize>,
) -> Result<Box<dyn Array>, RAError> {
    let nodes = read_sorted_gids(graph_dir.as_ref()).or_else(|err| match err {
        RAError::InvalidFile(_) => {
            let concurrent_files = rayon::current_num_threads();

            let nodes = edge_lists
                .iter()
                .filter_map(|e_list| {
                    let res = e_list
                        .par_sorted_nodes(read_chunk_size, concurrent_files)
                        .reduce(|l, r| merge_sort_dest_src(&l, &r).unwrap());
                    res
                })
                .reduce(|l, r| merge_sort_dest_src(&l, &r).unwrap())
                .ok_or_else(|| RAError::NoEdgeLists)?;

            persist_sorted_gids(graph_dir, nodes.clone())?;
            Ok(nodes)
        }
        _ => Err(err),
    })?;

    Ok(nodes)
}

pub(crate) fn read_global_ordering_from_parquet(
    graph_dir: impl AsRef<Path>,
    parquet_path: impl AsRef<Path>,
    id_col: &str,
) -> Result<Box<dyn Array>, RAError> {
    let files = list_parquet_files(parquet_path)?;
    let first = files.first().ok_or(RAError::NoNodeLists)?;
    let meta = read_file_metadata(first)?;
    let schema = read_schema(&meta)?.filter(|_, field| id_col == &field.name);
    if schema.fields.is_empty() {
        return Err(RAError::ColumnNotFound(id_col.to_string()));
    }
    let mut chunks = vec![];
    for f in files {
        for row_group in read_parquet_file(f, schema.clone())? {
            chunks.extend(row_group?.into_arrays());
        }
    }
    let chunk_refs = chunks.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
    let ids = concatenate(&chunk_refs)?;
    persist_sorted_gids(graph_dir, ids.clone())?;
    Ok(ids)
}

#[instrument(level = "debug", skip(graph_dir, nodes))]
pub(crate) fn persist_sorted_gids(
    graph_dir: impl AsRef<Path>,
    nodes: Box<dyn Array>,
) -> Result<(), RAError> {
    let schema = Schema::from(vec![Field::new("gid", nodes.data_type().clone(), false)]);
    let chunk = [Chunk::try_new(vec![nodes])?];
    let file = File::create_new(graph_dir.as_ref().join("sorted_gids.ipc"))?;
    write_batches(file, schema, &chunk)?;
    Ok(())
}

#[instrument(level = "debug", skip(graph_dir, map))]
pub(crate) fn persist_gid_map(
    graph_dir: impl AsRef<Path>,
    map: &DiskHashMap,
) -> Result<(), RAError> {
    let path = GraphPaths::HashMap.to_path(graph_dir, 0);
    map.write(File::create_new(path)?)?;
    Ok(())
}

pub(crate) fn read_or_create_gid_map(
    graph_dir: impl AsRef<Path>,
    sorted_gids: &Box<dyn Array>,
) -> Result<DiskHashMap, RAError> {
    let path = GraphPaths::HashMap.to_path(graph_dir, 0);
    if path.is_file() {
        DiskHashMap::read(path)
    } else {
        let order = DiskHashMap::from_sorted_dedup(sorted_gids.clone())?;
        order.write(File::create_new(path)?)?;
        Ok(order)
    }
}

pub(crate) fn read_sorted_gids(
    graph_dir: impl AsRef<Path> + Debug,
) -> Result<Box<dyn Array>, RAError> {
    let path = graph_dir.as_ref().join("sorted_gids.ipc");
    if !path.is_file() {
        return Err(RAError::InvalidFile(path));
    }
    let chunk = unsafe { mmap_batch(path, 0)? };
    let arrays = chunk.into_arrays();
    let gid_arr = arrays[0].clone();
    Ok(gid_arr)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sort_merge_dedup_2_cols() {
        let rhs = PrimitiveArray::from_vec(vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9, 10]).boxed();

        let lhs = PrimitiveArray::from_vec(vec![0u64, 9, 10, 6, 7, 8, 3, 1, 4, 15]).boxed();

        let lhs = sort_destinations(&lhs).unwrap();
        let actual_v = merge_sort_dest_src(&lhs, &rhs).unwrap();

        let expected_v =
            PrimitiveArray::from_vec(vec![0u64, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15]).boxed();

        assert_eq!(actual_v, expected_v);
    }
}
