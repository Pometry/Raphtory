use raphtory_api::core::entities::VID;
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::graph::TemporalGraph;

pub fn connected_components(graph: &TemporalGraph) -> Vec<usize> {
    let mut components: Vec<AtomicUsize> = Vec::with_capacity(graph.num_nodes());
    (0..graph.num_nodes())
        .into_par_iter()
        .map(AtomicUsize::new)
        .collect_into_vec(&mut components);

    let not_converged = AtomicBool::new(true);

    for _ in 0..graph.num_nodes() {
        not_converged.swap(false, Ordering::SeqCst);
        components
            .par_iter()
            .enumerate()
            .for_each(|(node, component_id)| {
                let old_component_id = component_id.load(Ordering::Relaxed);
                let mut new_component_id = old_component_id;
                for VID(neighbour) in graph
                    .layers()
                    .iter()
                    .flat_map(|layer| layer.nodes.out_neighbours_iter(VID(node)))
                {
                    let neighbour_component_id =
                        components[neighbour].fetch_min(new_component_id, Ordering::Relaxed);
                    if new_component_id < neighbour_component_id {
                        not_converged.fetch_or(true, Ordering::Relaxed);
                    } else {
                        new_component_id = neighbour_component_id
                    }
                }
                if new_component_id < old_component_id {
                    // we got a new value from neighbours, need to rerun
                    component_id.fetch_min(new_component_id, Ordering::Relaxed);
                    not_converged.fetch_or(true, Ordering::Relaxed);
                }
            });
        if !not_converged.load(Ordering::SeqCst) {
            break;
        }
    }
    components.into_iter().map(|v| v.into_inner()).collect()
}

#[cfg(test)]
mod test {
    use crate::{algorithms::connected_components::connected_components, graph::TemporalGraph};
    use polars_arrow::{
        array::{Array, PrimitiveArray, StructArray},
        datatypes::{ArrowDataType, Field},
        types::NativeType,
    };
    use tempfile::TempDir;

    #[test]
    fn simple_test() {
        let graph_dir = TempDir::new().unwrap();
        let path = graph_dir.path();
        let arrow_graph = TemporalGraph::from_edge_columns(
            path,
            &[struct_arr([
                ("src", arr(vec![0u64, 1])),
                ("dst", arr(vec![1u64, 2])),
                ("time", arr(vec![0i64, 0])),
            ])],
        )
        .unwrap();

        let components = connected_components(&arrow_graph);
        assert_eq!(components, [0, 0, 0]);
    }

    fn arr<T: NativeType>(v: Vec<T>) -> Box<dyn Array> {
        PrimitiveArray::from_vec(v).boxed()
    }

    fn struct_arr<'a>(arrs: impl IntoIterator<Item = (&'a str, Box<dyn Array>)>) -> StructArray {
        let fields: (Vec<_>, Vec<_>) = arrs
            .into_iter()
            .map(|(name, arr)| (Field::new(name, arr.data_type().clone(), true), arr))
            .unzip();
        StructArray::new(ArrowDataType::Struct(fields.0), fields.1, None)
    }

    #[test]
    fn two_components() {
        let test_dir = TempDir::new().unwrap();
        let arrow_graph = TemporalGraph::from_edge_columns(
            test_dir.path(),
            &[struct_arr([
                ("src", arr(vec![0u64, 1, 3, 4, 5, 5])),
                ("dst", arr(vec![1u64, 2, 4, 5, 3, 4])),
                ("time", arr(vec![0i64, 0, 0, 0, 0, 0])),
            ])],
        )
        .unwrap();

        let components = connected_components(&arrow_graph);
        assert_eq!(components, [0, 0, 0, 3, 3, 3]);
    }

    #[test]
    fn in_degree_zero() {
        let test_dir = TempDir::new().unwrap();
        let arrow_graph = TemporalGraph::from_edge_columns(
            test_dir.path(),
            &[struct_arr([
                ("src", arr(vec![0u64, 1, 2, 4])),
                ("dst", arr(vec![1u64, 4, 3, 2])),
                ("time", arr(vec![0i64, 0, 0, 0])),
            ])],
        )
        .unwrap();

        let components = connected_components(&arrow_graph);
        assert_eq!(components, [0, 0, 0, 0, 0]);
    }
}
