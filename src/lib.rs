use std::ops::Range;

mod graph;

trait TemporalGraphStorage {
    fn add_vertex(&self, v: u64, t: u64) -> &Self;

    fn enumerate_vertices(&self) -> Vec<u64>;

    fn enumerate_vs_at(&self, t: Range<u64>) -> Vec<u64>;
}

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

fn process_csv(data: &str) {
    let records = data.lines();

    for (i, record) in records.enumerate() {
        if i == 0 || record.trim().len() == 0 {
            continue;
        }

        let fields: Vec<_> = record.split(",").map(|field| field.trim()).collect();

        if (cfg!(debug_assertions)) {
            eprintln!("debug: {:?} -> {:?}", record, fields);
        }

        let name = fields[0];
        if let Ok(length) = fields[1].parse::<f32>() {
            println!("{}, {}cm", name, length)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_vertex_at_time_t1() {
        let g = graph::TemporalGraph::new_mem();

        g.add_vertex(9, 1);

        assert_eq!(g.enumerate_vertices(), vec![9])
    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let g = graph::TemporalGraph::new_mem();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        assert_eq!(g.enumerate_vs_at(0..1), vec![9]);
        assert_eq!(g.enumerate_vs_at(2..10), vec![1]);
    }

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
    #[test]
    fn test_some_csv_penguins() {
        let penguin_data = "\
        common name,length (cm)
        Little penguin,33
        Yellow-eyed penguin,65
        Fiordland penguin,60
        Invalid,data
        ";
        process_csv(penguin_data);
    }
}
