use crate::{
    db::{
        api::properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{GraphViewOps, VertexViewOps},
};
use itertools::{chain, Itertools};
use std::fmt::Display;

pub trait GraphEntity: Sized {
    fn generate_property_list<F, D>(
        &self,
        time_fmt: &F,
        filter_out: Vec<&str>,
        force_static: Vec<&str>,
    ) -> String
    where
        F: Fn(i64) -> D,
        D: Display;
}

impl<G: GraphViewOps> GraphEntity for VertexView<G> {
    fn generate_property_list<F, D>(
        &self,
        time_fmt: &F,
        filter_out: Vec<&str>,
        force_static: Vec<&str>,
    ) -> String
    where
        F: Fn(i64) -> D,
        D: Display,
    {
        let time_fmt = |time: i64| time_fmt(time).to_string();
        let missing = || "missing".to_owned();
        let min_time_fmt = self.earliest_time().map(time_fmt).unwrap_or_else(missing);
        let min_time = format!("earliest activity: {}", min_time_fmt);
        let max_time_fmt = self.latest_time().map(time_fmt).unwrap_or_else(missing);
        let max_time = format!("latest activity: {}", max_time_fmt);

        let temporal_keys = self
            .temporal_property_keys()
            .filter(|key| !filter_out.contains(&key.as_ref()))
            .filter(|key| !force_static.contains(&key.as_ref()))
            .filter(|key| {
                // the history of the temporal prop has more than one value
                let props = self.temporal_values(key);
                let values = props.iter().map(|prop| prop.to_string());
                values.unique().collect_vec().len() > 1
            })
            .collect_vec();

        let temporal_props = temporal_keys.iter().map(|key| {
            let history = self.temporal_history(&key);
            let props = self.temporal_values(&key);
            let values = props.iter().map(|prop| prop.to_string());
            let time_value_pairs = history.iter().zip(values);
            let events =
                time_value_pairs
                    .unique_by(|(_, value)| value.clone())
                    .map(|(time, value)| {
                        let key = key.to_string();
                        let time = time_fmt(*time);
                        format!("{key} changed to {value} at {time}")
                    });
            itertools::Itertools::intersperse(events, "\n".to_owned()).collect()
        });

        let prop_storage = self.properties();

        let static_props = prop_storage
            .keys()
            .filter(|key| !filter_out.contains(&key.as_ref()))
            .filter(|key| !temporal_keys.contains(key))
            .map(|key| {
                let prop = prop_storage.get(&key).unwrap().to_string();
                let key = key.to_string();
                format!("{key}: {prop}")
            });

        let props = chain!(static_props, temporal_props).sorted_by(|a, b| a.len().cmp(&b.len()));
        // We sort by length so when cutting out the tail of the document we don't remove small properties

        let lines = chain!([min_time, max_time], props);
        itertools::Itertools::intersperse(lines, "\n".to_owned()).collect()
    }
}

impl<G: GraphViewOps> GraphEntity for EdgeView<G> {
    // FIXME: implement this and remove underscore prefix from the parameter names
    fn generate_property_list<F, D>(
        &self,
        _time_fmt: &F,
        _filter_out: Vec<&str>,
        _force_static: Vec<&str>,
    ) -> String
    where
        F: Fn(i64) -> D,
        D: Display,
    {
        // TODO: not needed yet
        "".to_owned()
    }
}
