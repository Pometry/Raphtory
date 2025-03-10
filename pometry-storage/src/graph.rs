use super::{
    disk_hmap::DiskHashMap,
    global_order::GlobalOrder,
    graph_fragment::TempColGraphFragment,
    load::{read_sorted_gids, ExternalEdgeList},
    nodes::Node,
    prepare_graph_dir,
    properties::load_node_properties_from_parquet,
    GidRef, RAError,
};
use crate::{
    arrow2::{
        array::{Array, PrimitiveArray, StructArray, Utf8Array},
        datatypes::Field,
    },
    chunked_array::{
        chunked_array::{ChunkedArray, NonNull},
        chunked_offsets::ChunkedOffsets,
        mutable_chunked_array::{
            ChunkedStructArrayBuilder, MutChunkedOffsets, MutPrimitiveChunkedArray,
        },
    },
    compute::sort,
    edge::Edge,
    edge_list::EdgeList,
    edges::EdgeTemporalProps,
    file_prefix::GraphPaths,
    global_order::GIDArray,
    graph_builder::EFBResult,
    graph_fragment::{
        copy_edge_temporal_properties, gen_edge_ids, list_sorted_files, make_metadata,
        read_or_mmap_chunk, static_graph_builder,
    },
    interop::GraphLike,
    load::{
        ipc::read_batch, list_parquet_files, make_global_ordering, merge_sort_dest_src,
        mmap::mmap_buffer, parquet_reader::ParquetReader, persist_gid_map, persist_sorted_gids,
        read_global_ordering_from_parquet, read_or_create_gid_map, writer::write_batch,
    },
    nodes::{AdjTotal, Nodes},
    prelude::{ArrayOps, BaseArrayOps, Chunked},
    properties::{load_node_properties_from_dir, ConstProps, Properties, TemporalProps},
    Time,
};
use itertools::Itertools;
#[cfg(feature = "progress")]
use kdam::BarExt;
use polars_arrow::{
    datatypes::ArrowDataType,
    offset::OffsetsBuffer,
    types::{NativeType, Offset},
};
use raphtory_api::{
    atomic_extra::atomic_usize_from_mut_slice,
    compute::par_cum_sum,
    core::{
        entities::{properties::props::Meta, GidType, EID, GID, VID},
        storage::{
            dict_mapper::{DictMapper, MaybeNew},
            timeindex::AsTime,
        },
        Direction,
    },
};
use rayon::prelude::*;
use std::{
    fmt::Debug,
    marker::Sync,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{atomic::Ordering::Relaxed, Arc},
};
use tempfile::TempDir;
use tracing::instrument;

#[derive(Clone, Debug)]
pub struct TemporalGraph<GO = DiskHashMap> {
    graph_dir: PathBuf,
    global_ordering: Box<dyn Array>,
    global_mapping: Arc<GO>,
    pub(crate) layers: Arc<[TempColGraphFragment]>,
    pub(crate) edge_list: EdgeList,
    pub(crate) nodes: Nodes<AdjTotal>, // total static graph
    pub(crate) node_properties: Properties<VID>,
    pub(crate) node_type_ids: Option<ChunkedArray<PrimitiveArray<u64>, NonNull>>,
    pub(crate) node_types: Option<Utf8Array<i64>>,
    layer_names: Vec<String>,
    earliest_time: i64,
    latest_time: i64,

    node_meta: Arc<Meta>,
    edge_meta: Arc<Meta>,
    prop_mapping: GlobalToLocalPropMapping, //layer_id -> [(Global -> LocalPropId)]
}

impl<GO> AsRef<TemporalGraph<GO>> for TemporalGraph<GO> {
    fn as_ref(&self) -> &TemporalGraph<GO> {
        self
    }
}

#[derive(Debug)]
pub(crate) struct TotalGraphResult {
    pub(crate) src_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    pub(crate) dst_ids: ChunkedArray<PrimitiveArray<u64>, NonNull>,
    pub(crate) offsets: ChunkedOffsets,
}

impl<GO: GlobalOrder> TemporalGraph<GO> {
    pub fn from_parts(
        graph_dir: PathBuf,
        global_ordering: Box<dyn Array>,
        global_mapping: Arc<GO>,
        edge_list: EdgeList,
        nodes: Nodes<AdjTotal>,
        layers: Vec<TempColGraphFragment>,
        node_properties: Properties<VID>,
        node_type_ids: Option<ChunkedArray<PrimitiveArray<u64>, NonNull>>,
        node_types: Option<Utf8Array<i64>>,
        layer_names: Vec<String>,
        earliest_time: i64,
        latest_time: i64,
    ) -> Self {
        let (node_meta, edge_meta, prop_mapping) =
            generate_graph_meta(&layers, node_types.as_ref(), &layer_names, &node_properties);

        Self {
            graph_dir,
            global_ordering,
            global_mapping,
            layers: layers.into(),
            node_properties,
            node_type_ids,
            node_types,
            layer_names,
            earliest_time,
            latest_time,
            edge_list,
            nodes,
            node_meta: Arc::new(node_meta),
            edge_meta: Arc::new(edge_meta),
            prop_mapping,
        }
    }

    pub fn localize_edge_prop_id(&self, layer_id: usize, global_prop_id: usize) -> Option<usize> {
        self.prop_mapping.edges()[layer_id]
            .get(global_prop_id)
            .copied()
            .flatten()
    }

    // global property ids available for this layer
    pub fn edge_global_mapping(&self, layer_id: usize) -> impl Iterator<Item = usize> + '_ {
        self.prop_mapping.edges()[layer_id]
            .iter()
            .enumerate()
            .filter_map(|(i, x)| x.map(|_| i))
    }

    pub fn prop_mapping(&self) -> &GlobalToLocalPropMapping {
        &self.prop_mapping
    }

    pub fn id_type(&self) -> GidType {
        match self.global_ordering.data_type() {
            ArrowDataType::Int64 => GidType::U64,
            ArrowDataType::UInt64 => GidType::U64,
            ArrowDataType::LargeUtf8 => GidType::Str,
            ArrowDataType::Utf8 => GidType::Str,
            ArrowDataType::Utf8View => GidType::Str,
            _ => unreachable!(),
        }
    }

    pub fn graph_dir(&self) -> &Path {
        &self.graph_dir
    }

    pub fn node_t_props_dir(&self) -> PathBuf {
        self.graph_dir.join(GraphPaths::NodeTProps.as_ref())
    }

    pub fn get_valid_layers(&self) -> Vec<String> {
        self.layer_names().into_iter().map(|x| x.clone()).collect()
    }
    pub fn node_type(&self, node_type_id: usize) -> Option<&str> {
        self.node_types.as_ref()?.get(node_type_id)
    }

    pub fn node_type_id(&self, node: VID) -> usize {
        match self.node_type_ids.as_ref() {
            Some(node_type_ids) => node_type_ids.get(node.index()) as usize,
            None => 0,
        }
    }

    pub fn new_from_layers(
        graph_dir: PathBuf,
        global_ordering: Box<dyn Array>,
        global_mapping: Arc<GO>,
        edge_list: EdgeList,
        nodes: Nodes<AdjTotal>,
        layers: Vec<TempColGraphFragment>,
        layer_names: Vec<String>,
    ) -> Self {
        let (earliest_time, latest_time) = earliest_latest_times(&layers);

        Self::from_parts(
            graph_dir,
            global_ordering,
            global_mapping,
            edge_list,
            nodes,
            layers.into(),
            Properties::default(),
            None,
            None,
            layer_names,
            earliest_time,
            latest_time,
        )
    }

    pub fn node_properties(&self) -> &Properties<VID> {
        &self.node_properties
    }

    pub fn node_type_ids(&self) -> Option<&ChunkedArray<PrimitiveArray<u64>, NonNull>> {
        self.node_type_ids.as_ref()
    }

    pub fn node_types(&self) -> Option<&Utf8Array<i64>> {
        self.node_types.as_ref()
    }

    pub fn global_ordering(&self) -> &Box<dyn Array> {
        &self.global_ordering
    }
    pub fn global_mapping(&self) -> &Arc<GO> {
        &self.global_mapping
    }

    pub fn is_empty(&self) -> bool {
        self.global_ordering.is_empty()
    }

    pub fn earliest(&self) -> Option<i64> {
        Some(self.earliest_time).filter(|t| *t != i64::MAX)
    }

    pub fn latest(&self) -> Option<i64> {
        Some(self.latest_time).filter(|t| *t != i64::MIN)
    }

    pub fn internal_node(&self, vid: VID, layer_id: usize) -> Node {
        self.layers[layer_id].node(vid)
    }

    pub fn load_const_node_props_from_chunks(
        &mut self,
        chunks: impl IntoIterator<Item = Result<StructArray, RAError>>,
        chunk_size: usize,
    ) -> Result<(), RAError> {
        self.node_properties.const_props =
            load_node_const_properties(chunk_size, self.graph_dir(), chunks)?;
        Ok(())
    }

    /// Load temporal node properties from iterator over chunks
    ///
    /// Each chunk is a struct array where the first column is the node id, the second column is the
    /// timestamp. If `secondary_index == true`, the third column is the secondary index for the node
    /// properties, otherwise the secondary index is the row number. The rest of the columns are
    /// the property values. All chunks have to have the same schema.
    #[instrument("debug", skip_all)]
    pub fn load_temporal_node_props_from_chunks(
        &mut self,
        chunks: impl IntoIterator<Item = Result<StructArray, RAError>>,
        chunk_size: usize,
        secondary_index: bool,
    ) -> Result<(), RAError> {
        let sparse_props_path = self
            .node_t_props_dir()
            .join(self.node_properties.temporal_props.len().to_string());

        std::fs::create_dir_all(&sparse_props_path)?;

        let mut chunk_iter = chunks.into_iter().peekable();
        if let Some(first_chunk) = chunk_iter.peek() {
            let first_chunk = first_chunk
                .as_ref()
                .map_err(|e| RAError::GenericFailure(e.to_string()))?;
            let fields = first_chunk.fields();
            let (mut sc_index, first_value_col) = if secondary_index {
                (
                    Some(MutPrimitiveChunkedArray::<u64>::new_persisted(
                        chunk_size,
                        &sparse_props_path,
                        GraphPaths::NodeTPropsSecondaryIndex,
                    )),
                    3,
                )
            } else {
                (None, 2)
            };
            let mut offsets = vec![0usize; self.num_nodes() + 1];
            let offset_counters = atomic_usize_from_mut_slice(&mut offsets[1..]);
            let mut value_builder = ChunkedStructArrayBuilder::new_persisted(
                chunk_size,
                &sparse_props_path,
                GraphPaths::NodeTProps,
                fields[first_value_col..].to_vec(),
            );
            let mut ts_builder = MutPrimitiveChunkedArray::<i64>::new_persisted(
                chunk_size,
                &sparse_props_path,
                GraphPaths::NodeTPropsTimestamps,
            );

            #[cfg(feature = "progress")]
            let chunk_iter = kdam::tqdm!(chunk_iter, position = 0);

            for chunk in chunk_iter {
                let chunk = chunk?;
                let (_, mut values, _) = chunk.into_data();
                let gids = GIDArray::try_from(values[0].clone())?;
                gids.par_iter().try_for_each(|id| {
                    let VID(index) = self
                        .find_node(id)
                        .ok_or_else(|| RAError::MissingNode(id.to_owned()))?;
                    offset_counters[index].fetch_add(1, Relaxed);
                    Ok::<_, RAError>(())
                })?;
                let ts_values = values[1]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i64>>()
                    .ok_or_else(|| {
                        RAError::InvalidTimestampColumn(values[1].data_type().clone())
                    })?;

                #[cfg(feature = "progress")]
                let ts_values = kdam::tqdm!(ts_values.iter(), position = 1);

                for ts in ts_values {
                    let ts = *ts.ok_or(RAError::MissingTimestamp)?;
                    ts_builder.push(ts)?;
                }
                if let Some(sc_builder) = sc_index.as_mut() {
                    let sc_values = values
                        .get(2)
                        .ok_or(RAError::MissingIndexColumn)?
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .ok_or_else(|| {
                            RAError::InvalidIndexColumn(values[2].data_type().clone())
                        })?;
                    for sc in sc_values.iter() {
                        sc_builder.push(sc.copied().unwrap_or_default())?;
                    }
                }
                value_builder.push(&mut values[first_value_col..])?;
            }
            par_cum_sum(&mut offsets);
            let mut offsets_builder = MutChunkedOffsets::new(
                self.num_nodes(),
                Some((GraphPaths::NodeTPropsOffsets, sparse_props_path.clone())),
                true,
            );
            offsets_builder.push_chunk(offsets)?;

            let offsets = offsets_builder.finish()?;
            let props = value_builder.finish()?;
            let timestamps = ts_builder.finish()?;
            let secondary_index = sc_index.map(|sc_index| sc_index.finish()).transpose()?;

            let tprops = TemporalProps::new(offsets, props, timestamps, secondary_index);

            update_node_t_props_meta_layer(
                &mut self.prop_mapping.node_mapping,
                self.node_meta.as_ref(),
                &tprops,
                self.node_properties.temporal_props().len(),
            );
            self.node_properties.add_temporal_props(tprops);
        }
        Ok(())
    }

    pub fn load_node_types_from_chunks(
        &mut self,
        chunks: impl IntoIterator<Item = Result<Box<dyn Array>, RAError>>,
        chunk_size: usize,
    ) -> Result<(), RAError> {
        let mapper = self.node_meta.node_type_meta();
        mapper.get_or_create_id("_default");
        let mut id_chunk = Vec::with_capacity(chunk_size);
        let mut builder = MutPrimitiveChunkedArray::new_persisted(
            chunk_size,
            self.graph_dir(),
            GraphPaths::NodeTypeIds,
        );
        for chunk in chunks {
            let chunk = chunk?;
            match chunk.data_type() {
                ArrowDataType::Utf8 => {
                    utf8_chunk_to_vec::<i32>(&chunk, &mapper, &mut id_chunk);
                }
                ArrowDataType::LargeUtf8 => {
                    utf8_chunk_to_vec::<i64>(&chunk, &mapper, &mut id_chunk);
                }
                dtype => {
                    return Err(RAError::InvalidTypeColumn(format!(
                        "Expected string for node type, not {dtype:?}"
                    )))
                }
            }
            for id in id_chunk.drain(..) {
                builder.push(id)?;
            }
        }
        let node_type_ids = builder.finish()?;
        if node_type_ids.len() != self.num_nodes() {
            return Err(RAError::SizeError {
                actual: node_type_ids.len(),
                expected: self.num_nodes(),
            });
        }
        self.node_type_ids = Some(node_type_ids);
        let node_types = Utf8Array::from_iter_values(mapper.get_keys().iter());
        write_batch(
            GraphPaths::NodeTypes,
            self.graph_dir(),
            node_types.clone(),
            0,
        )?;
        self.node_types = Some(node_types);
        Ok(())
    }

    pub fn num_layers(&self) -> usize {
        self.layers.len()
    }

    pub fn edge_meta(&self) -> &Meta {
        &self.edge_meta
    }

    pub fn node_meta(&self) -> &Meta {
        &self.node_meta
    }
}

fn utf8_chunk_to_vec<I: Offset>(
    chunk: &Box<dyn Array>,
    mapper: &DictMapper,
    id_chunk: &mut Vec<u64>,
) {
    let chunk = chunk.as_any().downcast_ref::<Utf8Array<I>>().unwrap();
    (0..chunk.len())
        .into_par_iter()
        .map(|i| {
            chunk
                .get(i)
                .map(|t| mapper.get_or_create_id(t).inner() as u64)
                .unwrap_or(0)
        })
        .collect_into_vec(id_chunk);
}

#[derive(Clone, Debug)]
pub struct GlobalToLocalPropMapping {
    pub(crate) node_mapping: Vec<Option<(usize, usize)>>,
    reverse_node_mapping: Vec<Vec<usize>>,
    edge_mapping: Vec<Vec<Option<usize>>>,
}

impl GlobalToLocalPropMapping {
    pub fn new(
        node_mapping: Vec<Option<(usize, usize)>>,
        edge_mapping: Vec<Vec<Option<usize>>>,
    ) -> Self {
        let reverse_node_mapping = generate_reverse_node_mappings(&node_mapping);
        Self {
            node_mapping,
            reverse_node_mapping,
            edge_mapping,
        }
    }

    pub fn nodes(&self) -> &[Option<(usize, usize)>] {
        &self.node_mapping
    }

    pub fn edges(&self) -> &[Vec<Option<usize>>] {
        &self.edge_mapping
    }

    pub fn localise_edge_prop_id(&self, layer_id: usize, global_prop_id: usize) -> Option<usize> {
        self.edge_mapping
            .get(layer_id)?
            .get(global_prop_id)
            .copied()
            .flatten()
    }

    pub fn localise_node_prop_id(&self, global_prop_id: usize) -> Option<(usize, usize)> {
        self.node_mapping.get(global_prop_id).copied().flatten()
    }

    pub fn globalise_node_prop_id(&self, layer_id: usize, local_prop_id: usize) -> Option<usize> {
        self.reverse_node_mapping
            .get(layer_id)?
            .get(local_prop_id)
            .copied()
    }
}

fn generate_reverse_node_mappings(node_mapping: &[Option<(usize, usize)>]) -> Vec<Vec<usize>> {
    let num_layers = node_mapping
        .iter()
        .filter_map(|a| a.as_ref())
        .map(|(l, _)| l + 1)
        .max()
        .unwrap_or(0);
    let mut reverse_mappings = vec![vec![]; num_layers];

    for (global_prop_id, local_id) in node_mapping.iter().enumerate() {
        if let Some((layer, local_id)) = local_id {
            if reverse_mappings[*layer].get(*local_id).is_none() {
                reverse_mappings[*layer].resize(local_id + 1, usize::MAX);
                reverse_mappings[*layer][*local_id] = global_prop_id;
            }
        }
    }

    reverse_mappings
}

fn generate_graph_meta<'a>(
    layers: &'a [TempColGraphFragment],
    node_types: Option<&'a Utf8Array<i64>>,
    layer_names: &'a [String],
    node_properties: &Properties<VID>,
) -> (Meta, Meta, GlobalToLocalPropMapping) {
    let node_meta = Meta::new();
    let edge_meta = Meta::new();
    let mut edge_global_mapping: Vec<Vec<Option<usize>>> = vec![]; // Layer[GlobaPropId -> LocalPropId]
    for _ in 0..layers.len() {
        edge_global_mapping.push(vec![]);
    }

    let mut node_global_mapping = vec![];

    for node_type in node_types.into_iter().flatten() {
        if let Some(node_type) = node_type {
            node_meta.get_or_create_node_type_id(node_type);
        } else {
            panic!("Node types cannot be null");
        }
    }

    for (layer_id, layer) in layers.into_iter().enumerate() {
        let edge_props_fields = layer.edges_data_type();

        for (local_prop_id, field) in edge_props_fields.iter().enumerate().skip(1) {
            // we assume the first field is the timestamp
            let prop_name = &field.name;
            let data_type = field.data_type();

            let resolved_id = edge_meta
                .resolve_prop_id(prop_name, data_type.into(), false)
                .expect("Conflicting property types")
                .inner();

            edge_global_mapping[layer_id].resize(resolved_id + 1, None);
            edge_global_mapping[layer_id][resolved_id] = Some(local_prop_id);
        }
    }

    edge_global_mapping
        .iter_mut()
        .for_each(|mapping| mapping.resize(edge_meta.temporal_prop_meta().len(), None));

    for (l_id, l_name) in layer_names.into_iter().enumerate() {
        edge_meta.layer_meta().set_id(l_name.as_str(), l_id);
    }

    if let Some(props) = &node_properties.const_props {
        let node_const_props_fields = props.prop_dtypes();
        for field in node_const_props_fields {
            node_meta
                .resolve_prop_id(&field.name, field.data_type().into(), true)
                .expect("Initial resolve should not fail");
        }
    }

    let node_properties = node_properties.temporal_props();
    for (layer_id, props) in node_properties.into_iter().enumerate() {
        update_node_t_props_meta_layer(&mut node_global_mapping, &node_meta, props, layer_id);
    }
    (
        node_meta,
        edge_meta,
        GlobalToLocalPropMapping::new(node_global_mapping, edge_global_mapping),
    )
}

fn update_node_t_props_meta_layer(
    node_global_mapping: &mut Vec<Option<(usize, usize)>>,
    node_meta: &Meta,
    props: &TemporalProps<VID>,
    layer_id: usize,
) {
    for (local_prop_id, field) in props.prop_dtypes().into_iter().enumerate() {
        let resolved_id = node_meta
            .resolve_prop_id(&field.name, field.data_type().into(), false)
            .expect("Initial resolve should not fail");

        let resolved_id = if let MaybeNew::New(_) = resolved_id {
            resolved_id.inner()
        } else {
            unimplemented!("Cannot have duplicate property names in node temporal properties")
        };

        node_global_mapping.resize(resolved_id + 1, None);
        node_global_mapping[resolved_id] = Some((layer_id, local_prop_id));
    }
}

impl TemporalGraph<DiskHashMap> {
    pub fn from_edge_columns<'a, P: AsRef<Path> + Sync + Send + std::fmt::Debug>(
        graph_dir: P,
        cols: &[StructArray],
    ) -> Result<Self, RAError> {
        if cols.is_empty() {
            return Err(RAError::EmptyChunk);
        }
        let chunk_size = cols[0].len();
        Self::from_sorted_edge_list(graph_dir, 0, 1, 2, chunk_size, chunk_size, cols)
    }

    #[instrument(level = "debug", skip(edge_lists))]
    pub fn from_parquets<'a, P: AsRef<Path> + Sync + Send + Debug>(
        num_threads: usize,
        chunk_size: usize,
        t_props_chunk_size: usize,
        graph_dir: impl AsRef<Path> + Debug,
        edge_lists: impl IntoIterator<Item = ExternalEdgeList<'a, P>>,
        exclude_cols: &[&str],
        node_properties_path: Option<&Path>,
        node_type_col: Option<&str>,
        node_id_col: Option<&str>,
    ) -> Result<Self, RAError> {
        let graph_dir = graph_dir.as_ref();
        let edge_lists = edge_lists
            .into_iter()
            .sorted_by(|el1, el2| el1.layer().cmp(el2.layer()))
            .collect_vec();

        prepare_graph_dir(graph_dir)?;
        let num_threads = TryInto::<NonZeroUsize>::try_into(num_threads)?;

        let node_properties_path = node_properties_path.as_ref();
        let node_properties = if let Some(node_properties_path) = node_properties_path {
            load_node_properties_from_parquet(
                graph_dir,
                node_properties_path,
                num_threads,
                chunk_size,
                node_id_col,
            )?
        } else {
            Properties::default()
        };

        let sorted_gids = if let (Some(node_properties_path), Some(id_col)) =
            (node_properties_path, node_id_col)
        {
            read_global_ordering_from_parquet(graph_dir, node_properties_path, id_col)?
        } else {
            make_global_ordering(graph_dir, &edge_lists, None)?
        };
        let global_order = Arc::new(read_or_create_gid_map(graph_dir, &sorted_gids)?);

        // load all the static graphs for layers
        let layer_paths = edge_lists
            .iter()
            .map(|el| {
                let layer_dir = graph_dir.join(format!("l_{}", el.layer()).to_string());
                prepare_graph_dir(&layer_dir)?;
                Ok::<_, RAError>(layer_dir)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let temp_graph_dirs = if layer_paths.len() > 1 {
            layer_paths
                .iter()
                .map(|layer_dir| TempDir::new_in(layer_dir).map(|td| Some(td)))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![None]
        };

        let layers = edge_lists
            .iter()
            .zip(&layer_paths)
            .zip(&temp_graph_dirs)
            .map(|((el, layer_dir), temp_graph_dir)| {
                static_graph_builder(
                    el.files(),
                    1,
                    el.src_col,
                    el.dst_col,
                    global_order.clone(),
                    chunk_size,
                    &layer_dir,
                    temp_graph_dir.as_ref().map(|td| td.path()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let layer_names = edge_lists.iter().map(|el| el.layer().to_string()).collect();

        // create the global edge view
        let TotalGraphResult {
            src_ids: global_src_ids,
            dst_ids: global_dst_ids,
            offsets,
        } = if layers.len() > 1 {
            build_total_edge_list(
                sorted_gids.len(),
                chunk_size,
                graph_dir,
                layers
                    .iter()
                    .map(|fragment| (fragment.src_chunks.iter(), fragment.dst_chunks.iter())),
            )?
        } else {
            let layer_path = layer_paths.first().unwrap();
            let layer = layers.first().unwrap();

            for id in 0..layer.src_chunks.num_chunks() {
                std::fs::copy(
                    GraphPaths::AdjOutSrcs.to_path(layer_path, id),
                    GraphPaths::AdjOutSrcs.to_path(graph_dir, id),
                )?;
            }

            for id in 0..layer.dst_chunks.num_chunks() {
                std::fs::copy(
                    GraphPaths::AdjOutDsts.to_path(layer_path, id),
                    GraphPaths::AdjOutDsts.to_path(graph_dir, id),
                )?;
            }

            for (id, chunk) in layer.adj_out_offsets.iter_chunks().enumerate() {
                if id == layer.adj_out_offsets.num_chunks() - 1 && chunk.len() == 1 {
                    // we could be missing the last chunk on disk if it only has the first item
                    break;
                }
                std::fs::copy(
                    GraphPaths::AdjOutOffsets.to_path(layer_path, id),
                    GraphPaths::AdjOutOffsets.to_path(graph_dir, id),
                )?;
            }

            TotalGraphResult {
                src_ids: layer.src_chunks.clone(),
                dst_ids: layer.dst_chunks.clone(),
                offsets: layer.adj_out_offsets.clone(),
            }
        };

        let total_num_edges = global_dst_ids.len();

        let global_nodes = Nodes::new_out_only_total(offsets, global_dst_ids.clone());
        let nodes = if layers.len() > 1 {
            rebuild_edge_ids_values_for_static_graph(
                chunk_size,
                &layers,
                &layer_paths,
                &global_nodes,
            )?
        } else {
            let layer = layers.first().unwrap();
            let layer_path = layer_paths.first().unwrap();
            let edge_ids = gen_edge_ids(chunk_size, &layer_path, total_num_edges, None::<&Path>)?;
            let nodes = Nodes::new(
                layer_path,
                layer.adj_out_offsets.clone(),
                layer.dst_chunks.clone(),
                edge_ids,
            )?;
            vec![nodes]
        };

        let fragments = edge_lists
            .iter()
            .zip(&layer_paths)
            .zip(&layers)
            .zip(nodes.into_iter())
            .map(|(((el, graph_dir), ebf_result), nodes)| {
                let t_prop_offsets = &ebf_result.edge_offsets;
                let src_ids = &ebf_result.src_chunks;
                let layer_edge_ids = nodes.outbound_edges().values();

                let t_prop_new_offsets = if layers.len() > 1 {
                    fix_t_prop_offsets(
                        total_num_edges,
                        chunk_size,
                        graph_dir,
                        t_prop_offsets,
                        0u64..ebf_result.src_chunks.len() as u64,
                        layer_edge_ids.iter(),
                    )?
                } else {
                    t_prop_offsets.clone()
                };

                let t_prop_values = copy_edge_temporal_properties(
                    el.src_col,
                    el.dst_col,
                    exclude_cols,
                    graph_dir,
                    el.files(),
                    el.time_col,
                    num_threads,
                    t_props_chunk_size,
                )?;

                let edges =
                    EdgeTemporalProps::from_structs(t_prop_values.into(), t_prop_new_offsets);

                let metadata = make_metadata(&edges, chunk_size, t_props_chunk_size, &graph_dir)?;
                let graph_dir = graph_dir.to_path_buf();

                let mut t_graph_frag = TempColGraphFragment {
                    nodes,
                    edges,
                    metadata,
                    graph_dir: graph_dir.into(),
                };
                t_graph_frag.build_node_additions(chunk_size, &src_ids, &t_prop_offsets)?;
                Ok::<_, RAError>(t_graph_frag)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let (earliest_time, latest_time) = earliest_latest_times(&fragments);

        let (node_type_ids, node_types) = if let Some(node_properties_path) = node_properties_path {
            if let Some(node_type_col) = node_type_col {
                let nodes_types_map = DictMapper::default();
                nodes_types_map.get_or_create_id("_default");
                let node_type_ids = load_node_types_from_parquet(
                    graph_dir,
                    node_properties_path,
                    num_threads,
                    chunk_size,
                    node_type_col,
                    &nodes_types_map,
                )?;
                let node_types = Some(Utf8Array::from_iter_values(
                    nodes_types_map.get_keys().iter(),
                ));
                (node_type_ids, node_types)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        let edge_list = EdgeList::new(global_src_ids, global_dst_ids);

        Ok(Self::from_parts(
            graph_dir.to_path_buf(),
            sorted_gids,
            global_order,
            edge_list,
            global_nodes,
            fragments.into(),
            node_properties,
            node_type_ids,
            node_types,
            layer_names,
            earliest_time,
            latest_time,
        ))
    }

    pub fn new(graph_dir: impl AsRef<Path>) -> Result<Self, RAError> {
        let graph_dir = graph_dir.as_ref();
        let files = std::fs::read_dir(graph_dir)?;

        let layer_dirs = files
            .flatten()
            .filter(|file| {
                file.path()
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .filter(|name| name.starts_with("l_"))
                    .is_some()
            })
            .map(|file| file.path())
            .sorted_by(|p1, p2| p1.as_path().cmp(p2.as_path()))
            .collect::<Vec<_>>();

        if layer_dirs.is_empty() {
            return Err(RAError::EmptyGraphDir(graph_dir.to_path_buf()));
        }

        let mut layer_names = vec![];

        for path in &layer_dirs {
            if let Some(name) = path.file_stem().and_then(|name| name.to_str()) {
                if name.starts_with("l_") {
                    layer_names.push(name[2..].to_string());
                }
            }
        }

        let sorted_gids = read_sorted_gids(graph_dir)?;
        let gid_to_vid: DiskHashMap = read_or_create_gid_map(graph_dir, &sorted_gids)?;

        let graph_files = list_sorted_files(graph_dir)?;

        let mut dst_ids_chunks = vec![];
        let mut src_ids_chunks = vec![];
        let mut adj_out_offsets_chunks = vec![];

        for (file_type, path) in graph_files {
            match file_type {
                GraphPaths::AdjOutSrcs => {
                    read_chunk_with_ids::<u64>(&path, &mut src_ids_chunks, true)?;
                }
                GraphPaths::AdjOutDsts => {
                    read_chunk_with_ids::<u64>(&path, &mut dst_ids_chunks, true)?;
                }
                GraphPaths::AdjOutOffsets => {
                    let chunk = unsafe { mmap_buffer(&path, 0) }?;
                    adj_out_offsets_chunks.push(unsafe { OffsetsBuffer::new_unchecked(chunk) });
                }
                _ => {}
            }
        }

        let layers = layer_dirs
            .into_iter()
            .map(|path| TempColGraphFragment::new(path, true))
            .collect::<Result<Vec<_>, _>>()?;

        let (earliest_time, latest_time) = earliest_latest_times(&layers);

        let node_properties = load_node_properties_from_dir(graph_dir, true)?;

        let node_type_ids = load_node_type_ids_from_dir(graph_dir)?;
        let node_types = load_node_types_from_dir(graph_dir)?;

        let chunk_size = src_ids_chunks.get(0).map(|c| c.len()).unwrap_or(1);
        let dst_ids = ChunkedArray::new(dst_ids_chunks, chunk_size);
        let edge_list = EdgeList::new(
            ChunkedArray::new(src_ids_chunks, chunk_size),
            dst_ids.clone(),
        );

        let chunk_size = adj_out_offsets_chunks
            .get(0)
            .map(|c: &OffsetsBuffer<i64>| c.len())
            .unwrap_or(1);
        let offsets = ChunkedOffsets::new(adj_out_offsets_chunks, chunk_size);
        let global_nodes = Nodes::new_out_only_total(offsets, dst_ids.clone());

        let global_order = Arc::new(gid_to_vid);
        Ok(Self::from_parts(
            graph_dir.to_path_buf(),
            sorted_gids,
            global_order,
            edge_list,
            global_nodes,
            layers.into(),
            node_properties,
            node_type_ids,
            node_types,
            layer_names,
            earliest_time,
            latest_time,
        ))
    }

    pub fn from_sorted_edge_list(
        graph_dir: impl AsRef<Path> + Sync,
        src_col_idx: usize,
        dst_col_idx: usize,
        time_col_idx: usize,
        chunk_size: usize,
        t_props_chunk_size: usize,
        edge_list: &[StructArray],
    ) -> Result<Self, RAError> {
        let graph_dir = graph_dir.as_ref();
        prepare_graph_dir(graph_dir)?;
        let layer_path = graph_dir.join("l__default");
        prepare_graph_dir(&layer_path)?;

        let global_ordering = edge_list
            .into_par_iter()
            .filter_map(|chunk| {
                let srcs = &chunk.values()[src_col_idx];
                let dsts = &chunk.values()[dst_col_idx];

                let dsts = sort(dsts.as_ref()).ok()?;

                let global_ordering = merge_sort_dest_src(srcs, &dsts);
                Some(global_ordering)
            })
            .reduce_with(|l, r| {
                l.and_then(|l| r.map(|r| (l, r)))
                    .and_then(|(l, r)| merge_sort_dest_src(&l, &r))
            })
            .unwrap_or(Err(RAError::EmptyChunk))?;

        persist_sorted_gids(graph_dir, global_ordering.clone())?;
        let go = Arc::new(DiskHashMap::from_sorted_dedup(global_ordering.clone())?);
        persist_gid_map(graph_dir, go.as_ref())?;

        let (src_ids, mut layer) = TempColGraphFragment::load_from_edge_list(
            &layer_path,
            None,
            chunk_size,
            t_props_chunk_size,
            go.clone(),
            global_ordering.clone(),
            src_col_idx,
            dst_col_idx,
            time_col_idx,
            false,
            edge_list.iter().cloned(),
        )?;
        let offsets = layer.edges_storage().time_col().offsets().clone();
        layer.build_node_additions(chunk_size, &src_ids, &offsets)?;

        // move AdjOutSrcs into the main graph dir
        for (chunk_id, _) in src_ids.chunks.iter().enumerate() {
            std::fs::rename(
                GraphPaths::AdjOutSrcs.to_path(&layer_path, chunk_id),
                GraphPaths::AdjOutSrcs.to_path(graph_dir, chunk_id),
            )?;
        }

        // copy AdjOutDsts into the main graph dir
        for (chunk_id, _) in src_ids.chunks.iter().enumerate() {
            std::fs::copy(
                GraphPaths::AdjOutDsts.to_path(&layer_path, chunk_id),
                GraphPaths::AdjOutDsts.to_path(graph_dir, chunk_id),
            )?;
        }

        let dst_ids = (&layer.nodes).outbound_neighbours().values();
        let offsets = (&layer.nodes).outbound_neighbours().offsets().clone();

        for (chunk_id, chunk) in offsets.iter_chunks().enumerate() {
            if chunk_id == offsets.num_chunks() - 1 && chunk.len() == 1 {
                // we could be missing the last chunk on disk if it only has the first item
                break;
            }
            std::fs::copy(
                GraphPaths::AdjOutOffsets.to_path(&layer_path, chunk_id),
                GraphPaths::AdjOutOffsets.to_path(graph_dir, chunk_id),
            )?;
        }
        let nodes = Nodes::new_out_only_total(offsets, dst_ids.clone());

        let edge_list = EdgeList::new(src_ids, dst_ids.clone());
        let layers = vec![layer];

        let (earliest_time, latest_time) = earliest_latest_times(&layers);

        Ok(Self::from_parts(
            graph_dir.to_path_buf(),
            global_ordering,
            go,
            edge_list,
            nodes,
            layers.into(),
            Properties::default(),
            None,
            None,
            vec!["_default".to_string()],
            earliest_time,
            latest_time,
        ))
    }
}

#[instrument(level = "debug", skip_all)]
pub fn rebuild_edge_ids_values_for_static_graph(
    chunk_size: usize,
    layers: &[EFBResult],
    layer_paths: &[PathBuf],
    global_nodes: &Nodes<AdjTotal>,
) -> Result<Vec<Nodes>, RAError> {
    let oudbound_edge_values = layers
        .par_iter()
        .zip(layer_paths)
        .map(|(efb_fragment, graph_dir)| {
            let fragment = Nodes::new_out_only_total(
                efb_fragment.adj_out_offsets.clone(),
                efb_fragment.dst_chunks.clone(),
            );

            let mut outbound_edge_values = MutPrimitiveChunkedArray::<u64>::new_persisted(
                chunk_size,
                graph_dir,
                GraphPaths::AdjOutEdges,
            );

            for vid in fragment.iter() {
                let mut edge_ids = Vec::new();
                for chunk in fragment.adj_out.adj_neighbours.value(vid.0).iter_chunks() {
                    chunk
                        .into_par_iter()
                        .map(|dst_id| global_nodes.find_edge(vid, VID(*dst_id as usize)).unwrap())
                        .collect_into_vec(&mut edge_ids);

                    for eid in &edge_ids {
                        outbound_edge_values.push(eid.0 as u64)?;
                    }

                    edge_ids.clear();
                }
            }
            let edge_ids = outbound_edge_values.finish()?;

            Nodes::new(
                graph_dir,
                efb_fragment.adj_out_offsets.clone(),
                efb_fragment.dst_chunks.clone(),
                edge_ids,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(oudbound_edge_values)
}

pub fn fix_t_prop_offsets(
    total_num_edges: usize,
    chunk_size: usize,
    graph_dir: impl AsRef<Path>,
    t_prop_offsets: &ChunkedOffsets,
    old_layer_edge_ids: impl IntoIterator<Item = u64>,
    layer_edge_ids: impl IntoIterator<Item = u64>, // remapped edge ids
) -> Result<ChunkedOffsets, RAError> {
    let mut new_t_prop_offsets = MutChunkedOffsets::new(
        chunk_size,
        Some((
            GraphPaths::EdgeTPropsOffsets,
            graph_dir.as_ref().to_path_buf(),
        )),
        true,
    );

    let mut last = 0u64;
    for (new_eid, old_eid) in layer_edge_ids.into_iter().zip_eq(old_layer_edge_ids) {
        let (start, end) = t_prop_offsets.get(old_eid as usize);
        let i = start..end;

        for _ in last..new_eid {
            new_t_prop_offsets.push_empty()?;
        }
        new_t_prop_offsets.push_count(i.len())?;

        last = new_eid + 1;
    }
    for _ in last..total_num_edges as u64 {
        new_t_prop_offsets.push_empty()?;
    }
    new_t_prop_offsets.finish()
}

#[instrument(level = "debug", skip_all)]
pub(crate) fn build_total_edge_list<
    'a,
    A: IntoIterator<Item = u64>,
    B: IntoIterator<Item = u64>,
>(
    num_nodes: usize,
    chunk_size: usize,
    graph_dir: &Path,
    layers: impl IntoIterator<Item = (A, B)>,
) -> Result<TotalGraphResult, RAError> {
    let edges = layers
        .into_iter()
        .map(|(srcs, dsts)| srcs.into_iter().zip(dsts.into_iter()))
        .kmerge()
        .dedup();

    let mut srcs = MutPrimitiveChunkedArray::<u64>::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::AdjOutSrcs,
    );

    let mut dsts = MutPrimitiveChunkedArray::<u64>::new_persisted(
        chunk_size,
        graph_dir,
        GraphPaths::AdjOutDsts,
    );

    let mut offsets = MutChunkedOffsets::new(
        chunk_size,
        Some((GraphPaths::AdjOutOffsets, graph_dir.to_path_buf())),
        true,
    );

    let mut last = 0;
    let mut count = 0;

    #[cfg(feature = "progress")]
    let edges = kdam::tqdm!(edges);

    for (src, dst) in edges {
        for _ in last..src {
            offsets.push(count)?;
        }
        count += 1;
        srcs.push(src)?;
        dsts.push(dst)?;
        last = src;
    }

    for _ in last..num_nodes as u64 {
        offsets.push(count)?;
    }

    let src_ids = srcs.finish()?;
    let dst_ids = dsts.finish()?;
    let offsets = offsets.finish()?;

    Ok(TotalGraphResult {
        src_ids,
        dst_ids,
        offsets,
    })
}

fn read_chunk_with_ids<T: NativeType>(
    path: &PathBuf,
    src_ids_chunks: &mut Vec<PrimitiveArray<T>>,
    mmap: bool,
) -> Result<(), RAError> {
    let chunk = read_or_mmap_chunk(mmap, path)?;
    let src_ids = chunk[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .clone();
    src_ids_chunks.push(src_ids);
    Ok(())
}

pub fn earliest_latest_times(layers: &[TempColGraphFragment]) -> (i64, i64) {
    let (earliest_time, latest_time) = layers
        .iter()
        .map(|layer| (layer.earliest_time(), layer.latest_time()))
        .fold((i64::MAX, i64::MIN), |(earliest, latest), (e, l)| {
            (earliest.min(e), latest.max(l))
        });
    (earliest_time, latest_time)
}

fn load_node_type_ids_from_dir(
    graph_dir: impl AsRef<Path>,
) -> Result<Option<ChunkedArray<PrimitiveArray<u64>, NonNull>>, RAError> {
    let graph_dir = graph_dir.as_ref();

    let node_type_ids = unsafe {
        ChunkedArray::<PrimitiveArray<u64>, NonNull>::load_from_dir(
            graph_dir,
            GraphPaths::NodeTypeIds,
            false,
        )
    }?;

    if node_type_ids.is_empty() {
        return Ok(None);
    }

    Ok(Some(node_type_ids))
}

pub fn load_node_const_properties(
    chunk_size: usize,
    graph_dir: &Path,
    chunks: impl IntoIterator<Item = Result<StructArray, RAError>>,
) -> Result<Option<ConstProps<VID>>, RAError> {
    let mut chunk_iter = chunks.into_iter();
    if let Some(first_chunk) = chunk_iter.next() {
        let (fields, mut values, _) = first_chunk?.into_data();
        let mut builder = ChunkedStructArrayBuilder::new_persisted(
            chunk_size,
            graph_dir,
            GraphPaths::NodeConstProps,
            fields,
        );
        builder.push(&mut values)?;
        for chunk in chunk_iter {
            let (_, mut values, _) = chunk?.into_data();
            builder.push(&mut values)?;
        }
        let props = builder.finish()?;
        return Ok(Some(ConstProps::new(props)));
    }
    return Ok(None);
}

fn load_node_types_from_dir(
    graph_dir: impl AsRef<Path>,
) -> Result<Option<Utf8Array<i64>>, RAError> {
    let graph_dir = graph_dir.as_ref();
    let file_path = GraphPaths::NodeTypes.to_path(graph_dir, 0);
    if file_path.exists() {
        Ok(Some(
            read_batch(&file_path)?[0]
                .as_any()
                .downcast_ref::<Utf8Array<i64>>()
                .cloned()
                .ok_or(RAError::TypeCastError)?,
        ))
    } else {
        Ok(None)
    }
}

pub fn load_node_type_ids_from_parquet(
    graph_dir: impl AsRef<Path> + Send + Sync,
    parquet_path: impl AsRef<Path>,
    num_threads: NonZeroUsize,
    chunk_size: usize,
    node_type_col: &str,
) -> Result<Option<ChunkedArray<PrimitiveArray<u64>, NonNull>>, RAError> {
    let files = list_parquet_files(parquet_path)?;

    let reader = ParquetReader::new_from_filelist(
        graph_dir,
        files,
        None,
        GraphPaths::NodeTypeIds,
        |name| name == node_type_col,
    )?;

    let values = reader.load_values(num_threads, chunk_size)?;

    let chunked_arrays: ChunkedArray<PrimitiveArray<u64>, NonNull> = ChunkedArray::from_non_nulls(
        values
            .into_iter()
            .map(|c| {
                c.values()[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .cloned()
                    .ok_or(RAError::TypeCastError)
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(Some(chunked_arrays))
}

pub fn load_node_types_from_parquet(
    graph_dir: impl AsRef<Path> + Send + Sync,
    parquet_path: impl AsRef<Path>,
    num_threads: NonZeroUsize,
    chunk_size: usize,
    node_type_col: &str,
    dict_mapper: &DictMapper,
) -> Result<Option<ChunkedArray<PrimitiveArray<u64>, NonNull>>, RAError> {
    let files = list_parquet_files(parquet_path)?;

    let reader = ParquetReader::new_from_filelist(
        graph_dir,
        files,
        None,
        GraphPaths::NodeTypeIds,
        |name| name == node_type_col,
    )?;

    let values = reader.load_dict_encoded_values(num_threads, chunk_size, &dict_mapper)?;
    let chunked_arrays: ChunkedArray<PrimitiveArray<u64>, NonNull> =
        ChunkedArray::from_non_nulls(values);

    Ok(Some(chunked_arrays))
}

impl TemporalGraph<DiskHashMap> {
    pub fn from_graph<T: AsTime, G: GraphLike<T>>(
        graph: &G,
        graph_dir: impl AsRef<Path>,
        node_props: impl Fn() -> Result<Properties<VID>, RAError>,
    ) -> Result<Self, RAError> {
        let graph_dir = graph_dir.as_ref();
        prepare_graph_dir(graph_dir)?;
        let layer_names: Vec<String> = graph.layer_names();
        let (global_mapping, global_ordering) = if (0..graph.num_nodes())
            .into_par_iter()
            .any(|id| graph.find_name(VID(id)).is_some())
        {
            let global_ordering: Box<dyn Array> =
                <Utf8Array<i64>>::from_iter_values(graph.node_names()).boxed();
            let global_mapping = DiskHashMap::from_sorted_dedup(global_ordering.clone())?;
            (global_mapping, global_ordering)
        } else {
            let external_ids = graph.external_ids();
            match external_ids.get(0) {
                Some(GID::U64(_)) => {
                    let ids = external_ids.iter().filter_map(|id| id.as_u64());
                    let global_ordering: Box<dyn Array> = PrimitiveArray::from_values(ids).boxed();
                    let global_mapping = DiskHashMap::from_sorted_dedup(global_ordering.clone())?;
                    (global_mapping, global_ordering)
                }
                Some(GID::Str(_)) => {
                    let ids = external_ids.iter().filter_map(|id| id.as_str());
                    let global_ordering: Box<dyn Array> =
                        Utf8Array::<i64>::from_iter_values(ids).boxed();
                    let global_mapping = DiskHashMap::from_sorted_dedup(global_ordering.clone())?;
                    (global_mapping, global_ordering)
                }
                None => {
                    let global_ordering: Box<dyn Array> =
                        PrimitiveArray::from_values(0..graph.num_nodes() as u64).boxed();
                    let global_mapping = DiskHashMap::from_sorted_dedup(global_ordering.clone())?;
                    (global_mapping, global_ordering)
                }
            }
        };

        persist_sorted_gids(graph_dir, global_ordering.clone())?;
        persist_gid_map(graph_dir, &global_mapping)?;

        let edge_list = EdgeList::from_edges(
            graph
                .all_edges()
                .map(|(VID(src), VID(dst), _)| (src as u64, dst as u64)),
            graph.num_edges(),
            graph_dir,
        )?;

        let edge_ids_map = graph
            .all_edges()
            .map(|(_, _, eid)| eid.0)
            .collect::<Vec<_>>();

        let mut global_out_offsets = MutChunkedOffsets::new(
            graph.num_nodes(),
            Some((GraphPaths::AdjOutOffsets, graph_dir.to_path_buf())),
            true,
        );

        let mut out_offsets = vec![0usize; graph.num_nodes() + 1];
        out_offsets[1..].iter_mut().enumerate().for_each(|(n, o)| {
            let node = VID(n);
            *o = graph.out_neighbours(node).count();
        });

        par_cum_sum(&mut out_offsets);

        global_out_offsets.push_chunk(out_offsets)?;

        let global_nodes =
            Nodes::new_out_only_total(global_out_offsets.finish()?, edge_list.dsts().clone());

        let layers: Vec<_> = layer_names
            .iter()
            .enumerate()
            .filter_map(|(l_id, l_name)| {
                let l_name = format!("l_{l_name}");
                Some(TempColGraphFragment::from_graph(
                    graph,
                    graph_dir.join(l_name),
                    l_id,
                    &global_nodes,
                    &edge_ids_map,
                ))
            })
            .collect::<Result<_, _>>()?;

        let node_properties = node_props()?;

        let node_type_ids = match graph.node_type_ids() {
            Some(node_type_ids) => {
                let v = node_type_ids.map(|c| c as u64).collect_vec();
                let mut builder = MutPrimitiveChunkedArray::new_persisted(
                    v.len(),
                    graph_dir,
                    GraphPaths::NodeTypeIds,
                );
                builder.push_chunk(v)?;
                Some(builder.finish()?)
            }
            None => None,
        };

        let node_types = match graph.node_types() {
            Some(node_types) => {
                let data = Utf8Array::from_iter_values(node_types);
                write_batch(GraphPaths::NodeTypes, graph_dir, data.clone(), 0)?;
                Some(data)
            }
            None => None,
        };

        let (earliest_time, latest_time) = earliest_latest_times(&layers);

        Ok(Self::from_parts(
            graph_dir.to_path_buf(),
            global_ordering,
            global_mapping.into(),
            edge_list,
            global_nodes,
            layers,
            node_properties,
            node_type_ids,
            node_types,
            layer_names,
            earliest_time.min(graph.earliest_time()),
            latest_time.max(graph.latest_time()),
        ))
    }

    pub fn edge(&self, eid: EID) -> Edge {
        Edge::new(eid, self)
    }

    pub fn find_edge(&self, src: VID, dst: VID) -> Option<Edge> {
        self.nodes
            .find_edge(src, dst)
            .map(|eid| Edge::new(eid, self))
    }

    pub fn node(&self, vid: VID, layer_id: usize) -> Node {
        Node::new(vid, self.layer(layer_id).nodes_storage())
    }

    pub fn layer_edges(&self, layer_id: usize) -> impl Iterator<Item = Edge> + '_ {
        self.layers[layer_id]
            .outbound_edges()
            .map(move |(_, eid)| Edge::new(eid, self))
    }

    pub fn exploded_edges(&self) -> impl Iterator<Item = (VID, VID, Time)> + '_ {
        self.layers.into_iter().flat_map(move |layer| {
            self.edge_list
                .all_edges()
                .flat_map(|(eid, src, dst)| layer.edge_additions(eid).map(move |t| (src, dst, t)))
        })
    }

    pub fn edges_iter(&self) -> impl Iterator<Item = Edge> + '_ {
        (0..self.edge_list.len())
            .map(EID)
            .map(move |eid| Edge::new(eid, self))
    }

    pub fn edges_par_iter(&self) -> impl IndexedParallelIterator<Item = Edge> + '_ {
        (0..self.edge_list.len())
            .into_par_iter()
            .map(EID)
            .map(move |eid| Edge::new(eid, self))
    }

    pub fn edges_layer_iter(&self, layer_id: usize) -> impl Iterator<Item = Edge> + '_ {
        self.layer(layer_id)
            .all_edge_ids()
            .map(move |eid| Edge::new(eid, self))
    }

    pub fn edges_layer_par_iter(
        &self,
        layer_id: usize,
    ) -> impl IndexedParallelIterator<Item = Edge> + '_ {
        self.layer(layer_id)
            .all_edge_ids_par()
            .map(move |eid| Edge::new(eid, self))
    }
}

impl<GO: GlobalOrder> TemporalGraph<GO> {
    pub fn edges_data_type(&self, layer_id: usize) -> &[Field] {
        self.layers[layer_id].edges_data_type()
    }

    pub fn num_nodes(&self) -> usize {
        self.global_ordering.len()
    }

    pub fn count_edges(&self) -> usize {
        self.edge_list.len()
    }

    pub fn count_temporal_edges(&self) -> usize {
        self.layers()
            .par_iter()
            .map(|layer| layer.num_temporal_edges())
            .sum()
    }

    pub fn num_edges(&self) -> usize {
        self.edge_list.len()
    }

    pub fn global_src_ids(&self) -> &ChunkedArray<PrimitiveArray<u64>, NonNull> {
        self.edge_list.srcs()
    }

    pub fn global_dst_ids(&self) -> &ChunkedArray<PrimitiveArray<u64>, NonNull> {
        self.edge_list.dsts()
    }

    pub fn find_layer_id(&self, name: &str) -> Option<usize> {
        self.layer_names.iter().position(|n| n == name)
    }

    pub fn layers(&self) -> &[TempColGraphFragment] {
        &self.layers
    }

    pub fn arc_layers(&self) -> &Arc<[TempColGraphFragment]> {
        &self.layers
    }

    pub fn layer_names(&self) -> &[String] {
        &self.layer_names
    }

    pub fn edges(
        &self,
        vid: VID,
        dir: Direction,
        layer_id: usize,
    ) -> impl Iterator<Item = (EID, VID)> + '_ {
        self.layers[layer_id].edges(vid, dir)
    }

    pub fn in_adj_par(
        &self,
        vid: VID,
        layer_id: usize,
    ) -> impl IndexedParallelIterator<Item = (EID, VID)> + '_ {
        let nodes = &self.layers[layer_id].nodes;
        nodes.in_edges_par(vid).zip(nodes.in_neighbours_par(vid))
    }

    pub fn out_adj_par(
        &self,
        vid: VID,
        layer_id: usize,
    ) -> impl IndexedParallelIterator<Item = (EID, VID)> + '_ {
        let nodes = &self.layers[layer_id].nodes;
        nodes.out_edges_par(vid).zip(nodes.out_neighbours_par(vid))
    }

    pub fn edge_property_id(&self, prop_name: &str, layer_id: usize) -> Option<usize> {
        self.layers[layer_id].edge_property_id(prop_name)
    }

    pub fn all_nodes(&self) -> impl Iterator<Item = VID> {
        (0..self.num_nodes()).map(|vid| vid.into())
    }

    pub fn layer_edge_ids(&self, layer_id: usize) -> impl Iterator<Item = (EID, VID, VID)> + '_ {
        self.layers[layer_id].all_edge_ids().map(|eid| {
            let (src, dst) = self.edge_list.get(eid);
            (eid, src, dst)
        })
    }

    pub fn all_edge_ids(&self) -> impl Iterator<Item = (EID, VID, VID)> + '_ {
        self.edge_list.all_edges()
    }

    pub fn all_edge_ids_par(&self, layer_id: usize) -> impl IndexedParallelIterator<Item = EID> {
        self.layers[layer_id].all_edge_ids_par()
    }

    pub fn t_len(&self, layer_id: usize) -> usize {
        self.layers[layer_id].t_len()
    }

    pub fn node_gid(&self, vid: VID) -> Option<GidRef> {
        return if let Some(go) = self
            .global_ordering
            .as_any()
            .downcast_ref::<Utf8Array<i64>>()
        {
            let gid = go.value(vid.into());
            Some(GidRef::Str(gid))
        } else if let Some(go) = self
            .global_ordering
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
        {
            let gid = go.value(vid.into());
            Some(GidRef::U64(gid as u64))
        } else if let Some(go) = self
            .global_ordering
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
        {
            let gid = go.value(vid.into());
            Some(GidRef::Str(gid))
        } else if let Some(go) = self
            .global_ordering
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
        {
            let gid = go.value(vid.into());
            Some(GidRef::U64(gid))
        } else if let Some(go) = self
            .global_ordering
            .as_any()
            .downcast_ref::<PrimitiveArray<u32>>()
        {
            let gid = go.value(vid.into());
            Some(GidRef::U64(gid.into()))
        } else {
            None
        };
    }

    pub fn find_node(&self, gid: GidRef) -> Option<VID> {
        self.global_mapping.find(gid).map(|v| v.into())
    }

    pub fn layer(&self, layer: usize) -> &TempColGraphFragment {
        &self.layers[layer]
    }
}

#[cfg(test)]
mod test {
    use crate::{
        chunked_array::chunked_offsets::ChunkedOffsets,
        graph::TemporalGraph,
        load::ExternalEdgeList,
        merge::merge_graph::merge_graphs,
        prelude::{BaseArrayOps, IntoUtf8Col},
        test::make_graph,
        tprops::DiskTProp,
        utils::calculate_hash_spark,
        GidRef, RAError, GID,
    };
    use itertools::Itertools;
    use polars_arrow::{
        array::{Array, PrimitiveArray, StructArray, Utf8Array},
        buffer::Buffer,
        datatypes::{ArrowDataType, Field},
        offset::OffsetsBuffer,
    };
    use raphtory_api::core::{entities::VID, Direction};
    use rayon::prelude::*;
    use std::{
        ops::Range,
        path::{Path, PathBuf},
    };
    use tempfile::TempDir;

    use super::fix_t_prop_offsets;

    fn load_graph<P: AsRef<Path> + Send + Sync + std::fmt::Debug>(
        test_dir: &TempDir,
        netflow: P,
        wls: P,
        names: P,
        node_type_col: Option<&str>,
    ) -> TemporalGraph {
        let layered_edge_list = [
            ExternalEdgeList::new("netflow", netflow, "source", "destination", "time", vec![])
                .unwrap(),
            ExternalEdgeList::new("wls", wls, "src", "dst", "Time", vec![]).unwrap(),
        ];

        let g = TemporalGraph::from_parquets(
            4,
            4,
            16,
            test_dir.path(),
            layered_edge_list,
            &[],
            Some(names.as_ref()),
            node_type_col,
            None,
        )
        .unwrap();
        g
    }

    fn check_t_prop_fix(
        total_num_edges: usize,
        t_prop_offsets: Vec<i64>,
        layer_edge_ids: Vec<u64>,
        old_layer_edge_ids: Vec<u64>,
        expected: Vec<Range<usize>>,
    ) {
        let chunk_size = t_prop_offsets.len();
        let t_prop_offsets = ChunkedOffsets::new(
            vec![unsafe { OffsetsBuffer::new_unchecked(Buffer::from(t_prop_offsets)) }],
            chunk_size,
        );
        let graph_dir = TempDir::new().unwrap();
        let actual = fix_t_prop_offsets(
            total_num_edges,
            4,
            graph_dir.path(),
            &t_prop_offsets,
            old_layer_edge_ids,
            layer_edge_ids,
        )
        .unwrap()
        .iter()
        .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn fix_t_prop_offsets_1() {
        // total num_edges 2, layer only has 1 edge with some properties
        // layer contains the first edge
        check_t_prop_fix(2, vec![0, 5], vec![0], vec![0], vec![0..5, 5..5]);
        check_t_prop_fix(2, vec![0, 5], vec![1], vec![0], vec![0..0, 0..5]);
        check_t_prop_fix(
            3,
            vec![0, 5, 7],
            vec![0, 2],
            vec![0, 1],
            vec![0..5, 5..5, 5..7],
        );
        check_t_prop_fix(
            10,
            vec![0, 1, 1, 2, 2, 3, 3, 3, 4, 5, 6],
            vec![0, 2, 4, 7, 8, 9],
            vec![0, 2, 4, 7, 8, 9],
            vec![0..1, 1..1, 1..2, 2..2, 2..3, 3..3, 3..3, 3..4, 4..5, 5..6],
        );
    }

    #[test]
    fn xxhash64_like_spark_str() {
        let tests = ["Comp156925", "Comp844043", "Comp422326"];

        let actual = tests
            .into_iter()
            .map(|t| calculate_hash_spark(&GID::Str(t.to_owned())))
            .collect_vec();

        assert_eq!(
            actual,
            vec![
                -9222294199136761192,
                5645856008559730739,
                -9207726617634134598,
            ]
        );
    }

    #[test]
    fn xxhash64_like_spark_u64() {
        let tests = [0, 3];

        let actual = tests
            .into_iter()
            .map(|t| calculate_hash_spark(&GID::U64(t)))
            .collect_vec();

        assert_eq!(actual, vec![-5252525462095825812, 3188756510806108107,]);
    }

    #[test]
    fn load_from_2_parquet_layers() {
        let root = env!("CARGO_MANIFEST_DIR");
        let netflow: PathBuf = PathBuf::from_iter([root, "resources", "test", "netflow.parquet"]);
        let wls: PathBuf = PathBuf::from_iter([root, "resources", "test", "wls.parquet"]);
        let names = PathBuf::from_iter([root, "resources", "test", "node_types.parquet"]);

        let test_dir = TempDir::new().unwrap();
        let g = load_graph(&test_dir, netflow, wls, names, Some("node_type"));

        assert_eq!(g.find_layer_id("netflow"), Some(0));
        assert_eq!(g.find_layer_id("wls"), Some(1));

        // check path exists
        assert!(test_dir.path().join("l_netflow").exists());
        assert!(test_dir.path().join("l_wls").exists());
        assert!(test_dir.path().join("sorted_gids.ipc").exists());

        check_temporal_graph(&g);
        let graph = TemporalGraph::new(test_dir.path()).expect("Failed to load graph");
        check_temporal_graph(&graph);

        assert_eq!(graph.num_nodes(), g.num_nodes());
        assert_eq!(graph.num_nodes(), 3);
    }

    #[test]
    fn add_node_temporal_properties() {
        let root = env!("CARGO_MANIFEST_DIR");
        let netflow: PathBuf = PathBuf::from_iter([root, "resources", "test", "netflow.parquet"]);
        let wls: PathBuf = PathBuf::from_iter([root, "resources", "test", "wls.parquet"]);
        let names = PathBuf::from_iter([root, "resources", "test", "node_types.parquet"]);

        let test_dir = TempDir::new().unwrap();
        let mut g = load_graph(&test_dir, netflow, wls, names, Some("node_type"));

        let ids: Box<dyn Array> =
            Utf8Array::<i64>::from_slice(&["Comp244393", "Comp710070", "Comp710070"]).boxed();
        let time: Box<dyn Array> =
            PrimitiveArray::from_values([7387771i64, 7387801, 739267]).boxed();
        let value: Box<dyn Array> =
            Utf8Array::<i32>::from_slice(&["value1", "value2", "value3"]).boxed();

        let chunk = StructArray::try_new(
            ArrowDataType::Struct(vec![
                Field::new("ids", ArrowDataType::LargeUtf8, false),
                Field::new("time", ArrowDataType::Int64, false),
                Field::new("value", ArrowDataType::Utf8, false),
            ]),
            vec![ids, time, value],
            None,
        )
        .unwrap();

        g.load_temporal_node_props_from_chunks([Ok(chunk)], 17, false)
            .unwrap();

        let check = |g: &TemporalGraph| {
            let value_id = g.node_meta().temporal_prop_meta().get_id("value").unwrap();
            let (seg_id, prop_id) = g.prop_mapping.localise_node_prop_id(value_id).unwrap();
            let actual = g.node_properties().temporal_props()[seg_id]
                .temporal_props()
                .values()
                .into_utf8_col::<i32>(prop_id)
                .unwrap()
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            assert_eq!(actual, vec!["value1", "value2", "value3"]);
        };

        check(&g);
        check_temporal_graph(&g);
        let graph = TemporalGraph::new(test_dir.path()).expect("Failed to load graph");

        check(&graph);
        check_temporal_graph(&graph);
    }

    fn check_temporal_graph(graph: &TemporalGraph) {
        let wls = graph.find_layer_id("wls").expect("wls layer not found");
        let l_nft = graph
            .find_layer_id("netflow")
            .expect("netflow layer not found");
        assert_eq!(wls, 1);
        assert_eq!(l_nft, 0);

        let prop_id = graph.edge_property_id("LogonType", wls).unwrap();

        let p1 = graph
            .layer(wls)
            .edges_storage()
            .prop::<i64>(0.into(), prop_id);
        let actual = match p1 {
            DiskTProp::I64(col) => col.into_iter().collect::<Vec<_>>(),
            _ => vec![],
        };

        let expected = vec![
            (Some(3), 7387771),
            (Some(3), 7387771),
            (Some(3), 7387771),
            (Some(3), 7387771),
            (Some(3), 7387771),
            (None, 7387771),
            (None, 7387801),
            (None, 7387801),
            (None, 7387801),
            (None, 7387801),
        ];
        assert_eq!(expected, actual);

        let const_props = graph.node_properties().const_props.as_ref();

        assert!(const_props.is_some());

        let props = const_props.unwrap();

        let actual = (0..3)
            .into_iter()
            .filter_map(|vid| props.prop_str(VID(vid), 0).map(|s| s.to_string()))
            .collect::<Vec<_>>();

        let expected = vec![
            "Comp244393".to_string(),
            "Comp710070".to_string(),
            "Comp844043".to_string(),
        ];

        assert_eq!(expected, actual);

        let src = graph
            .find_node(GidRef::Str("Comp844043"))
            .expect("node not found");

        let dst = graph
            .find_node(GidRef::Str("Comp710070"))
            .expect("node not found");

        let src_port_id = graph
            .layer(l_nft)
            .edges_storage()
            .property_id("source_port")
            .expect("can't find source_port");
        let c10 = graph
            .layer(l_nft)
            .edges_storage()
            .property_id("_c10")
            .expect("can't find c10");

        for (eid, _) in graph.edges(src, Direction::OUT, l_nft) {
            let edge = graph.edge(eid);
            assert!(edge.has_layer_inner(l_nft));
            assert_eq!(edge.src_id(), src);
            assert_eq!(edge.dst_id(), dst);

            let p1 = graph
                .layer(l_nft)
                .edges_storage()
                .prop::<i64>(eid, src_port_id);
            let src_port_vec = match p1 {
                DiskTProp::Str64(col) => col
                    .into_iter()
                    .filter_map(|(a, b)| a.map(|a| (b, a)))
                    .collect::<Vec<_>>(),
                _ => vec![],
            };

            let expected = vec![
                (6415659, "Port86065"),
                (6415659, "Port00058"),
                (6415659, "Port41565"),
                (6415659, "Port42243"),
                (6415659, "Port48400"),
                (6415659, "Port48756"),
                (6415659, "Port99827"),
                (6415659, "Port42444"),
                (6415659, "Port10679"),
                (6415659, "Port27728"),
            ];

            assert_eq!(expected, src_port_vec);

            let p1 = graph.layer(l_nft).edges_storage().prop::<i64>(eid, c10);
            let c10 = match p1 {
                DiskTProp::I64(col) => col
                    .into_iter()
                    .filter_map(|(v, t)| v.map(|v| (t, v)))
                    .collect::<Vec<_>>(),
                _ => vec![],
            };

            let expected = vec![
                (6415659, 223),
                (6415659, 223),
                (6415659, 223),
                (6415659, 223),
                (6415659, 223),
                (6415659, 223),
                (6415659, 223),
                (6415659, 9656),
                (6415659, 223),
                (6415659, 223),
            ];

            assert_eq!(expected, c10);
        }
    }

    #[test]
    fn test_merge() {
        let root = env!("CARGO_MANIFEST_DIR");
        let netflow: PathBuf = PathBuf::from_iter([root, "resources", "test", "netflow.parquet"]);
        let wls: PathBuf = PathBuf::from_iter([root, "resources", "test", "wls.parquet"]);
        let names = PathBuf::from_iter([root, "resources", "test", "node_types.parquet"]);

        let test_dir_l = TempDir::new().unwrap();
        let g_l = load_graph(&test_dir_l, &netflow, &wls, &names, Some("node_type"));

        let test_dir_r = TempDir::new().unwrap();
        let g_r = load_graph(&test_dir_r, &netflow, &wls, &names, Some("node_type"));

        let test_dir_m = TempDir::new().unwrap();

        let g_m = merge_graphs(&test_dir_m, &g_l, &g_r).unwrap();
        assert_eq!(g_m.num_edges(), g_r.num_edges());
        assert_eq!(g_m.num_nodes(), g_r.num_nodes());
        assert_eq!(
            g_m.layer(0).num_temporal_edges(),
            g_r.layer(0).num_temporal_edges() * 2
        );
    }

    #[test]
    fn test_load_t_node_props() {
        let (mut graph, _test_dir) = make_graph(&[], &[0, 1], 2, 2);
        let id_input = [0u64, 0u64, 0u64, 1u64, 1u64];
        let ts_input = 0i64..5;
        let value_input = 0u64..5;

        let ids = PrimitiveArray::from_values(id_input.clone()).boxed();
        let ts = PrimitiveArray::from_values(ts_input.clone()).boxed();
        let values = PrimitiveArray::from_values(value_input.clone()).boxed();

        let fields = vec![
            Field::new("ids", ids.data_type().clone(), false),
            Field::new("time", ts.data_type().clone(), false),
            Field::new("value", values.data_type().clone(), false),
        ];

        let chunk = StructArray::new(ArrowDataType::Struct(fields), vec![ids, ts, values], None);
        graph
            .load_temporal_node_props_from_chunks([Ok(chunk)], 2, false)
            .unwrap();

        let props = &graph.node_properties().temporal_props()[0];
        let res = props
            .temporal_props()
            .non_null_primitive_col::<u64>(0)
            .unwrap();

        let expected_values: Vec<_> = id_input
            .iter()
            .map(|id| *id as usize)
            .zip(value_input)
            .collect();
        let indexed: Vec<_> = res.par_exploded_indexed().collect();
        assert_eq!(indexed, expected_values);

        let expected_ts: Vec<_> = id_input
            .iter()
            .map(|id| *id as usize)
            .zip(ts_input)
            .collect();
        let indexed_ts: Vec<_> = props.time_col().par_exploded_indexed().collect();
        assert_eq!(indexed_ts, expected_ts)
    }

    #[test]
    fn test_load_2_t_node_props() -> Result<(), RAError> {
        let (mut graph, _test_dir) = make_graph(&[], &[0, 1], 2, 2);
        let id_input = [0u64, 0u64, 0u64, 1u64, 1u64];
        let ts_input = 0i64..5;
        let value_input = 0u64..5;

        let ids = PrimitiveArray::from_values(id_input.clone()).boxed();
        let ts = PrimitiveArray::from_values(ts_input.clone()).boxed();
        let values = PrimitiveArray::from_values(value_input.clone()).boxed();

        let fields = vec![
            Field::new("ids", ids.data_type().clone(), false),
            Field::new("time", ts.data_type().clone(), false),
            Field::new("value", values.data_type().clone(), false),
        ];

        let chunk = StructArray::new(ArrowDataType::Struct(fields), vec![ids, ts, values], None);
        graph.load_temporal_node_props_from_chunks([Ok(chunk)], 2, false)?;

        let actual = graph.node_meta().temporal_prop_meta().get_id("value");
        assert_eq!(actual, Some(0));

        let id_input2 = PrimitiveArray::from_values([0u64, 1u64].iter().cloned()).boxed();
        let balance = PrimitiveArray::from_values([2u64, 1u64].iter().cloned()).boxed();
        let ts = PrimitiveArray::from_values([3i64, 7i64]).boxed();

        let fields = vec![
            Field::new("ids", id_input2.data_type().clone(), false),
            Field::new("time", ts.data_type().clone(), false),
            Field::new("balance", balance.data_type().clone(), false),
        ];

        let chunk = StructArray::new(
            ArrowDataType::Struct(fields),
            vec![id_input2, ts.clone(), balance.clone()],
            None,
        );

        graph.load_temporal_node_props_from_chunks([Ok(chunk)], 2, false)?;

        let actual = graph.node_meta().temporal_prop_meta().get_id("balance");
        assert_eq!(actual, Some(1));

        // check the property values have been ingested for part 1

        let check =
            move |graph: &TemporalGraph, id: usize, expected: Vec<u64>, expected_time: Vec<i64>| {
                assert_eq!(
                    graph.node_properties().temporal_props()[id]
                        .prop_dtypes()
                        .len(),
                    1
                );

                let actual = graph.node_properties().temporal_props()[id]
                    .temporal_props()
                    .non_null_primitive_col::<u64>(0)
                    .unwrap()
                    .values()
                    .into_iter()
                    .collect::<Vec<_>>();

                assert_eq!(actual, expected);

                let actual_time = graph.node_properties().temporal_props()[id]
                    .time_col()
                    .values()
                    .slice(..)
                    .into_iter()
                    .collect::<Vec<_>>();

                assert_eq!(actual_time, expected_time);
            };

        check(&graph, 0, vec![0, 1, 2, 3, 4], vec![0, 1, 2, 3, 4]);
        check(&graph, 1, vec![2, 1], vec![3, 7]);

        assert_eq!(graph.prop_mapping.localise_node_prop_id(0), Some((0, 0)));
        assert_eq!(graph.prop_mapping.localise_node_prop_id(1), Some((1, 0)));
        assert_eq!(graph.prop_mapping.localise_node_prop_id(2), None);

        Ok(())
    }
}
