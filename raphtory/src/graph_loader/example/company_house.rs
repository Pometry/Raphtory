use crate::{graph_loader::source::csv_loader::CsvLoader, prelude::*};
use chrono::NaiveDateTime;
use serde::Deserialize;
use std::{fs, path::PathBuf, time::Instant};

#[derive(Deserialize, std::fmt::Debug)]
pub struct CompanyHouse {
    address: String,
    pincode: String,
    company: String,
    owner: String,
    illegal_hmo: Option<String>,
}

pub fn company_house_graph(path: Option<String>) -> Graph {
    let default_data_dir: PathBuf = PathBuf::from("/tmp/company-house");

    let data_dir = match path {
        Some(path) => PathBuf::from(path),
        None => default_data_dir,
    };

    let dir_str = data_dir.to_str().unwrap();
    fs::create_dir_all(dir_str).expect(&format!("Failed to create directory {}", dir_str));

    let encoded_data_dir = data_dir.join("graphdb.bincode");

    fn restore_from_bincode(encoded_data_dir: &PathBuf) -> Option<Graph> {
        if encoded_data_dir.exists() {
            let now = Instant::now();
            let g = Graph::load_from_file(encoded_data_dir.as_path())
                .map_err(|err| {
                    println!(
                        "Restoring from bincode failed with error: {}! Reloading file!",
                        err
                    )
                })
                .ok()?;

            println!(
                "Loaded graph from encoded data files {} with {} vertices, {} edges which took {} seconds",
                encoded_data_dir.to_str().unwrap(),
                g.num_vertices(),
                g.num_edges(),
                now.elapsed().as_secs()
            );

            Some(g)
        } else {
            None
        }
    }

    let g = restore_from_bincode(&encoded_data_dir).unwrap_or_else(|| {
        let g = Graph::new();
        let now = Instant::now();
        let ts = 1;

        CsvLoader::new(data_dir)
            .set_header(true)
            .set_delimiter(",")
            .load_into_graph(&g, |company_house: CompanyHouse, g: &Graph| {
                let pincode = &company_house.pincode;
                let address = format!("{}, {pincode}", company_house.address);
                let company = company_house.company;
                let owner = company_house.owner;
                // let illegal_flag : Option<String> = match company_house.illegal_hmo {
                //     Some(value) => match value.as_str() {
                //         "true" => Some("true".to_string()),
                //         "false" => Some("false".to_string()),
                //         _ => None
                //     },
                //     None => None
                // };

                g.add_vertex(
                    NaiveDateTime::from_timestamp_opt(ts, 0).unwrap(),
                    owner.clone(),
                    NO_PROPS,
                )
                .expect("Failed to add vertex");

                g.add_vertex_properties(owner.clone(), [("type", "owner")])
                    .expect("Failed to add vertex static property");

                g.add_vertex(
                    NaiveDateTime::from_timestamp_opt(ts, 0).unwrap(),
                    company.clone(),
                    NO_PROPS,
                )
                .expect("Failed to add vertex");

                g.add_vertex_properties(
                    company.clone(),
                    [
                        ("type", "company".as_prop()),
                        (
                            "flag",
                            (company_house.illegal_hmo.clone().unwrap_or("None".into())).as_prop(),
                        ),
                    ],
                )
                .expect("Failed to add vertex static property");

                g.add_vertex(
                    NaiveDateTime::from_timestamp_opt(ts, 0).unwrap(),
                    address.clone(),
                    NO_PROPS,
                )
                .expect("Failed to add vertex");

                g.add_vertex_properties(
                    address.clone(),
                    [
                        ("type", "address".as_prop()),
                        (
                            "flag",
                            (company_house.illegal_hmo.clone().unwrap_or("None".into())).as_prop(),
                        ),
                    ],
                )
                .expect("Failed to add vertex static property");

                g.add_edge(
                    NaiveDateTime::from_timestamp_opt(ts, 0).unwrap(),
                    owner.clone(),
                    company.clone(),
                    NO_PROPS,
                    Some(pincode),
                )
                .expect("Failed to add edge");

                g.add_edge_properties(
                    owner,
                    company.clone(),
                    [("rel", "owns")],
                    Some(pincode),
                )
                .expect("Failed to add edge static property");

                g.add_edge(
                    NaiveDateTime::from_timestamp_opt(ts, 0).unwrap(),
                    company.clone(),
                    address.clone(),
                    NO_PROPS,
                    None,
                )
                .expect("Failed to add edge");

                g.add_edge_properties(
                    company,
                    address,
                    [("rel", "owns")],
                    None,
                )
                .expect("Failed to add edge static property");
            })
            .expect("Failed to load graph from CSV data files");

        println!(
            "Loaded graph from CSV data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir)
            .expect("Failed to save graph");

        g
    });

    g
}

#[cfg(test)]
mod company_house_graph_test {
    use super::*;
    use crate::db::api::view::{TimeOps, VertexViewOps};

    #[test]
    #[ignore]
    fn test_ch_load() {
        let g = company_house_graph(None);
        assert_eq!(g.start().unwrap(), 1000);
        assert_eq!(g.end().unwrap(), 1001);
        g.window(1000, 1001)
            .vertices()
            .into_iter()
            .for_each(|v| println!("vertexid = {}", v.id()));
    }
}
