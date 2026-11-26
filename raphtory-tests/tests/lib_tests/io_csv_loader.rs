#[cfg(test)]
mod csv_loader_test {
    use csv::StringRecord;
    use raphtory::{
        errors::GraphError,
        io::csv_loader::{CsvErr, CsvLoader},
        prelude::*,
    };
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

    fn lotr_test(
        g: Graph,
        csv_loader: CsvLoader,
        has_header: bool,
        delimiter: &str,
        r: Regex,
    ) -> Result<(), CsvErr> {
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
                )?;
                Ok(())
            })
    }

    fn lotr_test_rec(
        g: Graph,
        csv_loader: CsvLoader,
        has_header: bool,
        delimiter: &str,
        r: Regex,
    ) -> Result<(), CsvErr> {
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
                )?;
                Ok(())
            })
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
        lotr_test(g, csv_loader, has_header, delimiter, r).unwrap();
        let g = Graph::new();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        lotr_test_rec(g, csv_loader, has_header, delimiter, r).unwrap();
    }

    #[test]
    fn test_wrong_header_flag_file_with_header() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = false;
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let delimiter = ",";
        let result = lotr_test(g, csv_loader, has_header, delimiter, r);
        assert!(matches!(result, Err(CsvErr::CsvError(_))));
    }

    #[test]
    fn test_flag_has_header_but_file_has_no_header() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let r = Regex::new(r".+(lotr-without-header.csv)").unwrap();
        let delimiter = ",";
        let result = lotr_test(g, csv_loader, has_header, delimiter, r);
        assert!(matches!(result, Err(CsvErr::CsvError(_))));
    }

    #[test]
    fn test_wrong_header_names() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr-wrong.csv)").unwrap();
        let has_header = true;
        let delimiter = ",";
        let result = lotr_test(g, csv_loader, has_header, delimiter, r);
        assert!(matches!(result, Err(CsvErr::CsvError(_))));
    }

    #[test]
    fn test_wrong_delimiter() {
        global_info_logger();
        let g = Graph::new();
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let has_header = true;
        let delimiter = ".";
        let result = lotr_test(g, csv_loader, has_header, delimiter, r);
        assert!(matches!(result, Err(CsvErr::CsvError(_))));
    }

    #[test]
    fn a_loader_error_reaches_the_caller() {
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../resource/lotr.csv"]
            .iter()
            .collect();
        let result = CsvLoader::new(csv_path).set_header(true).load_into_graph(
            &Graph::new(),
            |_: Lotr, _| {
                Err(GraphError::InvalidValue {
                    reason: "refused".to_string(),
                })
            },
        );
        assert!(matches!(
            result,
            Err(CsvErr::GraphError(GraphError::InvalidValue { .. }))
        ));
    }
}
