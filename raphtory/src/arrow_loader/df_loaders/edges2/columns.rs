use crate::{
    arrow_loader::{
        dataframe::{DFChunk, DFView},
        df_loaders::{edges::ColumnNames, process_shared_properties},
        prop_handler::{combine_properties_arrow, mark_chunk_prop_presence, PropCols},
    },
    db::api::view::StaticGraphViewOps,
    errors::{into_graph_err, GraphError},
    prelude::*,
};
use raphtory_api::core::entities::{
    properties::prop::{AsPropRef, PropRef},
    LayerId,
};
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};
use std::collections::HashMap;

use super::chunk::RowProperties;

/// The input's column names, to find the loader's columns among.
pub(super) type Header = DFView<()>;

/// What the load reads from each chunk.
pub(super) struct EdgeSchema<'a> {
    pub columns: EdgeColumns,
    pub layer: Option<&'a str>,
    pub properties: EdgeProperties<'a>,
}

/// Where each of an edge's fields is among a chunk's columns.
#[derive(Clone, Copy)]
pub(super) struct EdgeColumns {
    pub src: usize,
    pub dst: usize,
    pub time: usize,
    pub event_id: Option<usize>,
    pub layer: Option<usize>,
}

/// The properties to load with each edge: two sets of columns, and metadata the caller gave for
/// every edge.
pub(super) struct EdgeProperties<'a> {
    temporal: PropertyColumns<'a>,
    metadata: PropertyColumns<'a>,
    shared: SharedMetadata,
}

struct PropertyColumns<'a> {
    names: &'a [&'a str],
    positions: Vec<usize>,
    are_metadata: bool,
}

pub(super) struct SharedMetadata(Vec<(usize, Prop)>);

impl EdgeColumns {
    pub fn find(header: &Header, names: ColumnNames) -> Result<Self, GraphError> {
        let optional = |name: Option<&str>| name.map(|name| header.get_index(name)).transpose();
        Ok(Self {
            src: header.get_index(names.src)?,
            dst: header.get_index(names.dst)?,
            time: header.get_index(names.time)?,
            event_id: optional(names.secondary_index)?,
            layer: optional(names.layer_col)?,
        })
    }
}

impl<'a> EdgeProperties<'a> {
    pub fn find<G: AdditionOps>(
        header: &Header,
        graph: &G,
        temporal: &'a [&'a str],
        metadata: &'a [&'a str],
        shared_metadata: Option<&HashMap<String, Prop>>,
    ) -> Result<Self, GraphError> {
        Ok(Self {
            temporal: PropertyColumns::find(header, temporal, false)?,
            metadata: PropertyColumns::find(header, metadata, true)?,
            shared: SharedMetadata::resolve(graph, shared_metadata)?,
        })
    }

    pub fn shared(&self) -> &SharedMetadata {
        &self.shared
    }

    /// Reads a chunk's property columns, and records which layers now have which properties.
    pub fn read<G: StaticGraphViewOps + AdditionOps>(
        &self,
        chunk: &DFChunk,
        graph: &G,
        layers: &[usize],
        distinct_layers: &[LayerId],
    ) -> Result<RowProperties, GraphError> {
        let session = graph.write_session().map_err(into_graph_err)?;
        let properties = RowProperties {
            temporal: self.temporal.read::<G>(chunk, &session)?,
            metadata: self.metadata.read::<G>(chunk, &session)?,
        };
        mark_chunk_prop_presence(
            graph.edge_meta(),
            Some(layers),
            distinct_layers,
            &properties.temporal,
            &properties.metadata,
            &self.shared.ids(),
        );
        Ok(properties)
    }
}

impl<'a> PropertyColumns<'a> {
    fn find(header: &Header, names: &'a [&'a str], are_metadata: bool) -> Result<Self, GraphError> {
        let positions = names
            .iter()
            .map(|name| header.get_index(name))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            names,
            positions,
            are_metadata,
        })
    }

    fn read<G: AdditionOps>(
        &self,
        chunk: &DFChunk,
        session: &<G as InternalAdditionOps>::WS<'_>,
    ) -> Result<PropCols, GraphError> {
        combine_properties_arrow(self.names, &self.positions, chunk, |name, dtype| {
            session
                .resolve_edge_property(name, dtype, self.are_metadata)
                .map_err(into_graph_err)
        })
    }
}

impl SharedMetadata {
    fn resolve<G: AdditionOps>(
        graph: &G,
        metadata: Option<&HashMap<String, Prop>>,
    ) -> Result<Self, GraphError> {
        let session = graph.write_session().map_err(into_graph_err)?;
        let resolved = process_shared_properties(metadata, |name, dtype| {
            session
                .resolve_edge_property(name, dtype, true)
                .map_err(into_graph_err)
        })?;
        Ok(Self(resolved))
    }

    fn ids(&self) -> Vec<usize> {
        self.0.iter().map(|(id, _)| *id).collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, PropRef<'_>)> {
        self.0.iter().map(|(id, prop)| (*id, prop.as_prop_ref()))
    }
}
