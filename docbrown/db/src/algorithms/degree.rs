use crate::graph_window::WindowedGraph;

pub fn max_out_degree(windowed_graph: &WindowedGraph) -> usize {
    let mut highest_degree = 0;
    windowed_graph.vertex_ids().for_each(|v| {
        if windowed_graph.vertex(v).unwrap().out_degree() > highest_degree {
            highest_degree = windowed_graph.vertex(v).unwrap().out_degree();
        }
    });
    highest_degree
}

pub fn max_in_degree(windowed_graph: &WindowedGraph) -> usize {
    let mut highest_degree = 0;
    windowed_graph.vertex_ids().for_each(|v| {
        if windowed_graph.vertex(v).unwrap().in_degree() > highest_degree {
            highest_degree = windowed_graph.vertex(v).unwrap().in_degree();
        }
    });
    highest_degree
}

pub fn min_out_degree(windowed_graph: &WindowedGraph) -> usize {
    let mut lowest_degree = 0;
    windowed_graph.vertex_ids().for_each(|v| {
        if windowed_graph.vertex(v).unwrap().out_degree() < lowest_degree {
            lowest_degree = windowed_graph.vertex(v).unwrap().out_degree();
        }
    });
    lowest_degree
}

pub fn min_in_degree(windowed_graph: &WindowedGraph) -> usize {
    let mut lowest_degree = 0;
    windowed_graph.vertex_ids().for_each(|v| {
        if windowed_graph.vertex(v).unwrap().in_degree() < lowest_degree {
            lowest_degree = windowed_graph.vertex(v).unwrap().in_degree();
        }
    });
    lowest_degree
}

pub fn average_degree(windowed_graph: &WindowedGraph) -> f32 {
    let mut total_degree = 0.0;
    windowed_graph
        .vertex_ids()
        .for_each(|v| total_degree += windowed_graph.vertex(v).unwrap().degree() as f32);
    total_degree / windowed_graph.graph.len() as f32
}

#[cfg(test)]
mod degree_test {

    use crate::{
        algorithms::degree::{average_degree, max_in_degree, min_in_degree, min_out_degree},
        graph::Graph,
    };

    use super::max_out_degree;

    #[test]
    fn degree_test() {
        let g = Graph::new(1);
        let windowed_graph = g.window(0, 7);
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
        ];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected_max_out_degree = 3;
        let actual_max_out_degree = max_out_degree(&windowed_graph);

        let expected_max_in_degree = 2;
        let actual_max_in_degree = max_in_degree(&windowed_graph);

        let expected_min_out_degree = 0;
        let actual_min_out_degree = min_out_degree(&windowed_graph);

        let expected_min_in_degree = 0;
        let actual_min_in_degree = min_in_degree(&windowed_graph);

        let expected_average_degree = 2.0;
        let actual_average_degree = average_degree(&windowed_graph);

        assert_eq!(expected_max_out_degree, actual_max_out_degree);
        assert_eq!(expected_max_in_degree, actual_max_in_degree);
        assert_eq!(expected_min_out_degree, actual_min_out_degree);
        assert_eq!(expected_min_in_degree, actual_min_in_degree);
        assert_eq!(expected_average_degree, actual_average_degree);
    }
}
