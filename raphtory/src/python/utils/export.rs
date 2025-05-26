use crate::db::api::properties::{internal::PropertiesOps, Properties};
use itertools::Itertools;
use raphtory_api::core::{
    entities::properties::{meta::Meta, prop::Prop},
    storage::{arc_str::ArcStr, timeindex::AsTime},
};
use rayon::{iter::IntoParallelRefIterator, prelude::*};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub(crate) fn extract_properties<P>(
    include_property_history: bool,
    convert_datetime: bool,
    explode: bool,
    column_names: &Vec<String>,
    is_prop_both_temp_and_const: &HashSet<String>,
    item: &Properties<P>,
    properties_map: &mut HashMap<String, Prop>,
    prop_time_dict: &mut HashMap<i64, HashMap<String, Prop>>,
    start_time: i64,
) where
    P: PropertiesOps + Clone,
{
    let properties = item.constant().as_map();
    let properties_collected: HashMap<String, Prop> = properties
        .par_iter()
        .map(|(name, prop)| {
            let column_name = if is_prop_both_temp_and_const.contains(name.as_ref()) {
                format!("{}_constant", name)
            } else {
                name.to_string()
            };
            (column_name, prop.clone())
        })
        .collect();
    properties_collected.clone_into(properties_map);

    if explode {
        let mut empty_dict = HashMap::new();
        column_names.clone().iter().for_each(|name| {
            let _ = empty_dict.insert(name.clone(), Prop::from(""));
        });

        if item.temporal().iter().count() == 0 {
            if item.constant().iter().count() == 0 {
                // node is empty so add as empty time
                let _ = prop_time_dict.insert(start_time, empty_dict.clone());
            } else {
                item.constant().iter().for_each(|(name, prop_val)| {
                    if !prop_time_dict.contains_key(&start_time) {
                        let _ = prop_time_dict.insert(start_time, empty_dict.clone());
                    }
                    let data_dict = prop_time_dict.get_mut(&0i64).unwrap();
                    let _ = data_dict.insert(name.to_string(), prop_val);
                })
            }
        }
        item.temporal()
            .histories()
            .iter()
            .for_each(|(prop_name, (time, prop_val))| {
                let column_name = if is_prop_both_temp_and_const.contains(prop_name.as_ref()) {
                    format!("{}_temporal", prop_name)
                } else {
                    prop_name.to_string()
                };
                if !prop_time_dict.contains_key(time) {
                    prop_time_dict.insert(*time, empty_dict.clone());
                }
                let data_dict = prop_time_dict.get_mut(&time).unwrap();
                let _ = data_dict.insert(column_name.clone(), prop_val.clone());
            });
    } else if include_property_history {
        item.temporal().iter().for_each(|(name, prop_view)| {
            let column_name = if is_prop_both_temp_and_const.contains(name.as_ref()) {
                format!("{}_temporal", name)
            } else {
                name.to_string()
            };
            if convert_datetime {
                let mut prop_vec = vec![];
                prop_view.iter().for_each(|(time, prop)| {
                    let prop_time = Prop::DTime(time.dt().unwrap());
                    prop_vec.push(Prop::List(Arc::from(vec![prop_time, prop])))
                });
                let wrapped = Prop::from(prop_vec);
                let _ = properties_map.insert(column_name, wrapped);
            } else {
                let vec_props = prop_view
                    .iter()
                    .map(|(k, v)| Prop::from(vec![Prop::from(k), v]))
                    .collect_vec();
                let wrapped = Prop::List(Arc::from(vec_props));
                let _ = properties_map.insert(column_name, wrapped);
            }
        });
    } else {
        item.temporal().iter().for_each(|(name, t_prop)| {
            let column_name = if is_prop_both_temp_and_const.contains(name.as_ref()) {
                format!("{}_temporal", name)
            } else {
                name.to_string()
            };
            let _ = properties_map.insert(column_name, t_prop.latest().unwrap_or(Prop::from("")));
        });
    }
}

pub(crate) fn get_column_names_from_props(
    column_names: &mut Vec<String>,
    edge_meta: &Meta,
) -> HashSet<String> {
    let mut is_prop_both_temp_and_const: HashSet<String> = HashSet::new();
    let temporal_properties: HashSet<ArcStr> = edge_meta
        .temporal_prop_meta()
        .get_keys()
        .iter()
        .cloned()
        .collect();
    let constant_properties: HashSet<ArcStr> = edge_meta
        .const_prop_meta()
        .get_keys()
        .iter()
        .cloned()
        .collect();
    constant_properties
        .intersection(&temporal_properties)
        .into_iter()
        .for_each(|name| {
            column_names.push(format!("{}_constant", name));
            column_names.push(format!("{}_temporal", name));
            is_prop_both_temp_and_const.insert(name.to_string());
        });
    constant_properties
        .symmetric_difference(&temporal_properties)
        .into_iter()
        .for_each(|name| {
            column_names.push(name.to_string());
        });
    column_names.push("update_history".parse().unwrap());
    is_prop_both_temp_and_const
}

pub(crate) fn create_row(
    convert_datetime: bool,
    explode: bool,
    column_names: &Vec<String>,
    properties_map: HashMap<String, Prop>,
    prop_time_dict: HashMap<i64, HashMap<String, Prop>>,
    row_header: Vec<Prop>,
    start_point: usize,
    history: Vec<i64>,
) -> Vec<Vec<Prop>> {
    if explode {
        let new_rows: Vec<Vec<Prop>> = prop_time_dict
            .par_iter()
            .map(|(time, item_dict)| {
                let mut row: Vec<Prop> = row_header.clone();
                for prop_name in &column_names[start_point..(column_names.len() - 1)] {
                    if let Some(prop_val) = properties_map.get(prop_name) {
                        row.push(prop_val.clone());
                    } else if let Some(prop_val) = item_dict.get(prop_name) {
                        row.push(prop_val.clone());
                    } else {
                        row.push(Prop::from(""));
                    }
                }

                if convert_datetime {
                    row.push(Prop::DTime(time.dt().unwrap()));
                } else {
                    row.push(Prop::from(*time));
                }

                row
            })
            .collect();
        new_rows
    } else {
        let mut row: Vec<Prop> = row_header.clone();
        // Flatten properties into the row
        for prop_name in &column_names[start_point..(column_names.len() - 1)] {
            // Skip the first column (name)
            let blank_prop = Prop::from("");
            let prop_value = properties_map.get(prop_name).unwrap_or(&blank_prop);
            let _ = row.push(prop_value.clone()); // Append property value as string
        }

        if convert_datetime {
            let update_list = history
                .iter()
                .map(|val| Prop::DTime(val.dt().unwrap()))
                .collect_vec();
            let _ = row.push(Prop::from(update_list));
        } else {
            let update_list = Prop::from(history.iter().map(|&val| Prop::from(val)).collect_vec());
            let _ = row.push(update_list);
        }
        vec![row]
    }
}
