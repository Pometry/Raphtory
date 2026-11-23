use ahash::HashSet;
use proptest::proptest;
use raphtory::prelude::*;
use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
use raphtory_tests::utils::{assert_valid_graph, generate_mutations, GraphMutation, TestInput};
use rayon::{prelude::*, ThreadPoolBuilder};
use storage::{
    api::{edges::EdgeSegmentOps, nodes::NodeSegmentOps},
    Args,
};

#[test]
///
fn test_concurrent_additions() {
    proptest!(|(graph_f in generate_mutations(0..=10, 0..=10, 0..=10, 0..=10, 0), num_threads in 1..=10usize, num_free_segments in 1..=10usize, node_segment_size in 2..=10u32, edge_segment_size in 1..=10u32)| {
        let TestInput {fixture,updates} = graph_f;
        let g = ThreadPoolBuilder::new().num_threads(num_free_segments).build().unwrap().install(|| Graph::new_with_config(Args::default().with_max_node_page_len(node_segment_size).with_max_edge_page_len(edge_segment_size)).unwrap());
        ThreadPoolBuilder::new().num_threads(num_threads).build().unwrap().install(||
            updates.0.into_par_iter().for_each(|mutation| {
                match mutation {
                    GraphMutation::AddNode{node,time,props,node_type,metadata,layer  } => {
                        g.add_node(time, node, props, node_type.as_deref(), layer.as_deref()).unwrap().add_metadata(metadata).unwrap();
                    }
                    GraphMutation::AddEdge{src,dst,time,props,metadata,layer  } => {
                        g.add_edge(time, src, dst, props, layer.as_deref()).unwrap().add_metadata(metadata, None).unwrap();
                    }
                    GraphMutation::DeleteEdge{src,dst,time,layer  } => {
                        g.delete_edge(time, src, dst, layer.as_deref()).unwrap();
                    }
                    GraphMutation::DropAndLoad => {}
                }
            })
        );
        assert_valid_graph(&fixture, &g);

        // check we haven't orphaned any free segments
        let storage = g.mutable().unwrap().storage();
        let free_node_segments: HashSet<_> = storage.nodes().free_segments().iter_slots().map(|slot| *slot.read()).collect();
        for segment in storage.nodes().segments_iter() {
            // one free slot is possible as we add both endpoints of an edge to the same segment if they are both new
            if segment.num_nodes() < node_segment_size -1 {
                assert!(free_node_segments.contains(&segment.segment_id()))
            }
        }

        let free_edge_segments: HashSet<_> = storage.edges().free_segments().iter_slots().map(|slot| *slot.read()).collect();
        for (_, segment) in storage.edges().segments().iter() {
            if segment.num_edges() < edge_segment_size {
                assert!(free_edge_segments.contains(&segment.segment_id()))
            }
        }
    })
}
