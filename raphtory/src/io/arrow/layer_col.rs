use std::borrow::Cow;

use crate::{
    errors::{into_graph_err, GraphError, LoadError},
    io::arrow::dataframe::DFChunk,
    prelude::AdditionOps,
};
use arrow::array::{Array, AsArray, LargeStringArray, StringArray, StringViewArray};
use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, IndexedParallelIterator, Iterator, ParallelIterator,
};
use rayon::prelude::*;

#[derive(Copy, Clone, Debug)]
pub(crate) enum LayerCol<'a> {
    Name { name: Option<&'a str>, len: usize },
    Utf8 { col: &'a StringArray },
    LargeUtf8 { col: &'a LargeStringArray },
    Utf8View { col: &'a StringViewArray },
}

#[derive(
    Iterator, DoubleEndedIterator, ExactSizeIterator, ParallelIterator, IndexedParallelIterator,
)]
pub enum LayerColVariants<Name, Utf8, LargeUtf8, Utf8View> {
    Name(Name),
    Utf8(Utf8),
    LargeUtf8(LargeUtf8),
    Utf8View(Utf8View),
}

impl<'a> LayerCol<'a> {
    pub fn par_iter(self) -> impl IndexedParallelIterator<Item = Option<&'a str>> {
        match self {
            LayerCol::Name { name, len } => {
                LayerColVariants::Name((0..len).into_par_iter().map(move |_| name))
            }
            LayerCol::Utf8 { col } => LayerColVariants::Utf8(
                (0..col.len())
                    .into_par_iter()
                    .map(|i| col.is_valid(i).then(|| col.value(i))),
            ),
            LayerCol::LargeUtf8 { col } => LayerColVariants::LargeUtf8(
                (0..col.len())
                    .into_par_iter()
                    .map(|i| col.is_valid(i).then(|| col.value(i))),
            ),

            LayerCol::Utf8View { col } => LayerColVariants::Utf8View(
                (0..col.len())
                    .into_par_iter()
                    .map(|i| col.is_valid(i).then(|| col.value(i))),
            ),
        }
    }

    pub fn iter(self) -> impl Iterator<Item = Option<&'a str>> {
        match self {
            LayerCol::Name { name, len } => LayerColVariants::Name((0..len).map(move |_| name)),
            LayerCol::Utf8 { col } => LayerColVariants::Utf8(col.iter()),
            LayerCol::LargeUtf8 { col } => LayerColVariants::LargeUtf8(col.iter()),
            LayerCol::Utf8View { col } => LayerColVariants::Utf8View(col.iter()),
        }
    }

    pub fn get(&self, row: usize) -> Option<&'a str> {
        match self {
            LayerCol::Name { name, .. } => *name,
            LayerCol::Utf8 { col } => {
                if col.is_valid(row) && row < col.len() {
                    Some(col.value(row))
                } else {
                    None
                }
            }
            LayerCol::LargeUtf8 { col } => {
                if col.is_valid(row) && row < col.len() {
                    Some(col.value(row))
                } else {
                    None
                }
            }
            LayerCol::Utf8View { col } => {
                if col.is_valid(row) && row < col.len() {
                    Some(col.value(row))
                } else {
                    None
                }
            }
        }
    }

    pub fn resolve_layer<'b>(
        self,
        layer_id_col: Option<&'b [u64]>,
        graph: &(impl AdditionOps + Send + Sync),
    ) -> Result<Cow<'b, [usize]>, GraphError> {
        match (self, layer_id_col) {
            (LayerCol::Name { name, len }, _) => {
                let layer = graph.resolve_layer(name).map_err(into_graph_err)?.inner();
                Ok(Cow::Owned(vec![layer; len]))
            }
            (col, None) => {
                let mut res = vec![0usize; col.len()];
                let mut last_name = None;
                let mut last_layer = None;
                for (row, name) in col.iter().enumerate() {
                    if last_name == name && last_layer.is_some() {
                        if let Some(layer) = last_layer {
                            res[row] = layer;
                        }
                        continue;
                    }

                    let layer = graph.resolve_layer(name).map_err(into_graph_err)?.inner();
                    last_layer = Some(layer);
                    res[row] = layer;
                    last_name = name;
                }
                Ok(Cow::Owned(res))
            }
            (col, Some(layer_ids)) => {
                let mut last_pair = None;

                let edge_layer_mapper = graph.edge_meta().layer_meta();
                let node_layer_mapper = graph.node_meta().layer_meta();

                let mut locked_edge_lm = edge_layer_mapper.write();
                let mut locked_node_lm = node_layer_mapper.write();

                for pair @ (name, id) in col
                    .iter()
                    .map(|name| name.unwrap_or("_default"))
                    .zip(layer_ids)
                {
                    if last_pair != Some(pair) {
                        locked_edge_lm.set_id(name, *id as usize);
                        locked_node_lm.set_id(name, *id as usize);
                    }
                    last_pair = Some(pair);
                }
                Ok(Cow::Borrowed(bytemuck::cast_slice(layer_ids)))
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            LayerCol::Name { len, .. } => *len,
            LayerCol::Utf8 { col } => col.len(),
            LayerCol::LargeUtf8 { col } => col.len(),
            LayerCol::Utf8View { col } => col.len(),
        }
    }
}

pub(crate) fn lift_layer_col<'a>(
    layer_name: Option<&'a str>,
    layer_index: Option<usize>,
    df: &'a DFChunk,
) -> Result<LayerCol<'a>, GraphError> {
    match (layer_name, layer_index) {
        (name, None) => Ok(LayerCol::Name {
            name,
            len: df.len(),
        }),
        (None, Some(layer_index)) => {
            let col = &df.chunk[layer_index];
            if let Some(col) = col.as_string_opt() {
                Ok(LayerCol::Utf8 { col })
            } else if let Some(col) = col.as_string_opt() {
                Ok(LayerCol::LargeUtf8 { col })
            } else if let Some(col) = col.as_string_view_opt() {
                Ok(LayerCol::Utf8View { col })
            } else {
                Err(LoadError::InvalidLayerType(col.data_type().clone()).into())
            }
        }
        _ => Err(GraphError::WrongNumOfArgs(
            "layer_name".to_string(),
            "layer_col".to_string(),
        )),
    }
}

pub(crate) fn lift_node_type_col<'a>(
    node_type_name: Option<&'a str>,
    node_type_index: Option<usize>,
    df: &'a DFChunk,
) -> Result<LayerCol<'a>, GraphError> {
    match (node_type_name, node_type_index) {
        (name, None) => Ok(LayerCol::Name {
            name,
            len: df.len(),
        }),
        (None, Some(layer_index)) => {
            let col = &df.chunk[layer_index];
            if let Some(col) = col.as_string_opt() {
                Ok(LayerCol::Utf8 { col })
            } else if let Some(col) = col.as_string_opt() {
                Ok(LayerCol::LargeUtf8 { col })
            } else if let Some(col) = col.as_string_view_opt() {
                Ok(LayerCol::Utf8View { col })
            } else {
                Err(LoadError::InvalidNodeType(col.data_type().clone()).into())
            }
        }
        _ => Err(GraphError::WrongNumOfArgs(
            "node_type_name".to_string(),
            "node_type_col".to_string(),
        )),
    }
}
