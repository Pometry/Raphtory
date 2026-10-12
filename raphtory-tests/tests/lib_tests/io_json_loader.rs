#[cfg(test)]
mod tests {
    use bzip2::{write::BzEncoder, Compression as BzCompression};
    use flate2::{write::GzEncoder, Compression};
    use raphtory::{io::json_loader::*, prelude::*};
    use serde::Deserialize;
    use std::{fs::File, io::Write};
    use tempfile::tempdir;

    #[derive(Debug, Deserialize)]
    struct TestRecord {
        name: String,
        time: i64,
    }

    fn test_json_rec(g: Graph, loader: JsonLinesLoader<TestRecord>) {
        loader
            .load_into_graph(&g, |testrec: TestRecord, g: &Graph| {
                let _ = g.add_node(testrec.time, testrec.name.clone(), NO_PROPS, None, None);
                Ok(())
            })
            .expect("Unable to add node to graph");
        assert_eq!(g.count_nodes(), 3);
        assert_eq!(g.count_edges(), 0);
        let mut names = g.nodes().name().iter_values().collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, vec!["test", "testbz", "testgz"]);
    }

    #[test]
    fn test_load_into_graph() {
        let dir = tempdir().unwrap();
        let plain_file = dir.path().join("test.json");
        let gzip_file = dir.path().join("test.json.gz");
        let bzip_file = dir.path().join("test.json.bz2");

        // Create plain json file
        File::create(&plain_file)
            .unwrap()
            .write_all(b"{\"name\": \"test\", \"time\": 1}\n")
            .expect("unable to make plain file");

        // Create gzip compressed json file
        let f = File::create(&gzip_file).unwrap();
        let mut gz = GzEncoder::new(f, Compression::fast());
        gz.write_all(b"{\"name\": \"testgz\", \"time\": 2}\n")
            .expect("unable to write to gz file");
        gz.finish().expect("Unable to write GZ file");

        // Create bzip2 compressed json file
        let f = File::create(&bzip_file).unwrap();
        let mut bz = BzEncoder::new(f, BzCompression::fast());
        bz.write_all(b"{\"name\": \"testbz\", \"time\": 3}\n")
            .expect("unable to write to bz file");
        bz.finish().expect("Unable to write BZ file");

        let g = Graph::new();
        let loader = JsonLinesLoader::<TestRecord>::new(dir.path().to_path_buf(), None);
        test_json_rec(g, loader);
    }
}
