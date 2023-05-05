use std::{collections::HashMap, sync::Arc};

use crate::{
    core::{tgraph::VertexRef, Prop},
    db::{
        edge::EdgeView,
        path::PathFromVertex,
        view_api::{
            internal::GraphViewInternalOps, BoxedIter, GraphViewOps, TimeOps, VertexViewOps,
        },
    },
};

pub trait VertexViewInternal {
    type Graph: GraphViewOps;
    fn vertex_ref(&self) -> VertexRef;
    fn graph(&self) -> &Self::Graph;
    fn graph_arc(&self) -> Arc<Self::Graph>;
}

impl<V> VertexViewOps for V
where
    V: VertexViewInternal + TimeOps + Send + Sync,
{
    type Graph = V::Graph;

    type ValueType<T> = T;

    type PathType = PathFromVertex<V::Graph>;

    type EList = BoxedIter<EdgeView<V::Graph>>;

    fn id(&self) -> Self::ValueType<u64> {
        self.vertex_ref().g_id
    }

    fn name(&self) -> Self::ValueType<String> {
        match self.static_property("_id".to_string()) {
            None => self.id().to_string(),
            Some(prop) => prop.to_string(),
        }
    }

    fn earliest_time(&self) -> Self::ValueType<Option<i64>> {
        self.graph().vertex_earliest_time(self.vertex_ref())
    }

    fn latest_time(&self) -> Self::ValueType<Option<i64>> {
        self.graph().vertex_latest_time(self.vertex_ref())
    }

    fn property(
        &self,
        name: String,
        include_static: bool,
    ) -> Self::ValueType<Option<crate::core::Prop>> {
        let props = self.property_history(name.clone());
        match props.last() {
            None => {
                if include_static {
                    match self.graph().static_vertex_prop(self.vertex_ref(), name) {
                        None => None,
                        Some(prop) => Some(prop),
                    }
                } else {
                    None
                }
            }
            Some((_, prop)) => Some(prop.clone()),
        }
    }

    fn history(&self) -> Self::ValueType<Vec<i64>> {
        self.graph().vertex_timestamps(self.vertex_ref())
    }

    fn property_history(&self, name: String) -> Self::ValueType<Vec<(i64, crate::core::Prop)>> {
        self.graph()
            .temporal_vertex_prop_vec(self.vertex_ref(), name)
    }

    fn properties(
        &self,
        include_static: bool,
    ) -> Self::ValueType<std::collections::HashMap<String, crate::core::Prop>> {
        let mut props: HashMap<String, Prop> = self
            .property_histories()
            .iter()
            .map(|(key, values)| (key.clone(), values.last().unwrap().1.clone()))
            .collect();

        if include_static {
            for prop_name in self.graph().static_vertex_prop_names(self.vertex_ref()) {
                match self
                    .graph()
                    .static_vertex_prop(self.vertex_ref(), prop_name.clone())
                {
                    Some(prop) => {
                        props.insert(prop_name, prop);
                    }
                    None => {}
                }
            }
        }
        props
    }

    fn property_histories(
        &self,
    ) -> Self::ValueType<std::collections::HashMap<String, Vec<(i64, crate::core::Prop)>>> {
        todo!()
    }

    fn property_names(&self, include_static: bool) -> Self::ValueType<Vec<String>> {
        todo!()
    }

    fn has_property(&self, name: String, include_static: bool) -> Self::ValueType<bool> {
        todo!()
    }

    fn has_static_property(&self, name: String) -> Self::ValueType<bool> {
        todo!()
    }

    fn static_property(&self, name: String) -> Self::ValueType<Option<crate::core::Prop>> {
        todo!()
    }

    fn degree(&self) -> Self::ValueType<usize> {
        todo!()
    }

    fn in_degree(&self) -> Self::ValueType<usize> {
        todo!()
    }

    fn out_degree(&self) -> Self::ValueType<usize> {
        todo!()
    }

    fn edges(&self) -> Self::EList {
        todo!()
    }

    fn in_edges(&self) -> Self::EList {
        todo!()
    }

    fn out_edges(&self) -> Self::EList {
        todo!()
    }

    fn neighbours(&self) -> Self::PathType {
        todo!()
    }

    fn in_neighbours(&self) -> Self::PathType {
        todo!()
    }

    fn out_neighbours(&self) -> Self::PathType {
        todo!()
    }
}
