#[cfg(test)]
mod csv_loader_test {
    use raphtory::{io::csv_loader::CsvLoader, prelude::*};
    use csv::StringRecord;
    use raphtory_api::core::utils::logging::global_info_logger;
    use regex::Regex;
    use serde::Deserialize;
    use std::path::{Path, PathBuf};
    use tracing::error;

    #[test]
    fn regex_match() {
        let r = Regex::new(r".+address").unwrap();
        // todo: move file path to data module
        let text = "bitcoin/address_000000000001.csv.gz";
        assert!(r.is_match(text));
        let text = "bitcoin/received_000000000001.csv.gz";
        assert!(!r.is_match(text));
    }

    #[test]
    fn regex_match_2() {
        let r = Regex::new(r".+(sent|received)").unwrap();
        // todo: move file path to data module
        let text = "bitcoin/sent_000000000001.csv.gz";
        assert!(r.is_match(text));
        let text = "bitcoin/received_000000000001.csv.gz";
        assert!(r.is_match(text));
        let text = "bitcoin/address_000000000001.csv.gz";
        assert!(!r.is_match(text));
    }

    #[derive(Deserialize, std::fmt::Debug)]
    pub struct Lotr {
        src_id: String,
        dst_id: String,
        time: i64,
    }

    fn lotr_test(g: Graph, csv_loader: CsvLoader, has_header: bool, delimiter: &str, r: Regex) {
        csv_loader
            .set_header(has_header)
            .set_delimiter(delimiter)
            .with_filter(r)
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = lotr.src_id.id();
                let dst_id = lotr.dst_id.id();
                let time = lotr.time;

                g.add_node(time, src_id, [("name", Prop::str("Character"))], None, None)
                    .map_err(|err| error!("{:?}", err))
                    .ok();
                g.add_node(time, dst_id, [("name", Prop::str("Character"))], None, None)
                    .map_err(|err| error!("{:?}", err))
                    .ok();
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    [("name", Prop::str("Character Co-occurrence"))],
                    None,
                )
                .unwrap();
            })
            .expect("Csv did not parse.");
    }

    fn lotr_test_rec(g: Graph, csv_loader: CsvLoader, has_header: bool, delimiter: &str, r: Regex) {
        csv_loader
            .set_header(has_header)
            .set_delimiter(delimiter)
            .with_filter(r)
            .load_rec_into_graph(&g, |lotr: StringRecord, g: &Graph| {
                let src_id = lotr.get(0).map(|s| s.id()).unwrap();
                let dst_id = lotr.get(1).map(|s| s.id()).unwrap();
                let time = lotr.get(2).map(|s| s.parse::<i64>().unwrap()).unwrap();

                g.add_node(time, src_id, [("name", Prop::str("Character"))], None, None)
                    .map_err(|err| error!("{:?}", err))
                    .ok();
                g.add_node(time, dst_id, [("name", Prop::str("Character"))], None, None)
                    .map_err(|err| error!("{:?}", err))
                    .ok();
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    [("name", Prop::str("Character Co-occurrence"))],
                    None,
                )
                .unwrap();
            })
            .expect("Csv did not parse.");
    }

    #[test]
    fn test_headers_flag_and_delimiter() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../resource/"]
            .iter()
            .collect();

        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
        let g = Graph::new();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        lotr_test_rec(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_header_flag_file_with_header() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = false;
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_flag_has_header_but_file_has_no_header() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let r = Regex::new(r".+(lotr-without-header.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_header_names() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr-wrong.csv)").unwrap();
        let has_header = true;
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_delimiter() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let has_header = true;
        let delimiter = ".";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }
}
