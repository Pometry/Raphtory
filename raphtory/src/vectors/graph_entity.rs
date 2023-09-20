use crate::{
    db::{
        api::properties::internal::{TemporalPropertiesOps, TemporalPropertyViewOps},
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, GraphViewOps, VertexViewOps},
    vectors::{entity_id::EntityId, EntityDocument},
};
use itertools::{chain, Itertools};
use std::fmt::Display;

pub trait GraphEntity: Sized {
    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String;

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
            .filter(|key| !filter_out.contains(&key.as_str()))
            .filter(|key| !force_static.contains(&key.as_str()))
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
            time_value_pairs
                .unique_by(|(_, value)| value.clone())
                .map(|(time, value)| {
                    let key = key.to_string();
                    let time = time_fmt(*time);
                    format!("{key} changed to {value} at {time}")
                })
                .intersperse("\n".to_owned())
                .collect()
        });

        let prop_storage = self.properties();

        let static_props = prop_storage
            .keys()
            .filter(|key| !filter_out.contains(&key.as_str()))
            .filter(|key| !temporal_keys.contains(key))
            .map(|key| {
                let prop = prop_storage.get(&key).unwrap().to_string();
                let key = key.to_string();
                format!("{key}: {prop}")
            });

        let props = chain!(static_props, temporal_props).sorted_by(|a, b| a.len().cmp(&b.len()));
        // We sort by length so when cutting out the tail of the document we don't remove small properties

        let lines = chain!([min_time, max_time], props);
        lines.intersperse("\n".to_owned()).collect()
    }

    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String,
    {
        let raw_content = template(self);
        let content = match raw_content.char_indices().nth(1000) {
            Some((index, _)) => (&raw_content[..index]).to_owned(),
            None => raw_content,
        };
        // TODO: allow multi document entities !!!!!
        // shortened to 1000 (around 250 tokens) to avoid exceeding the max number of tokens,
        // when embedding but also when inserting documents into prompts

        EntityDocument {
            id: EntityId::Node { id: self.id() },
            content,
        }
    }
}

impl<G: GraphViewOps> GraphEntity for EdgeView<G> {
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
        // TODO: not needed yet
        "".to_owned()
    }
    fn generate_doc<T>(&self, template: &T) -> EntityDocument
    where
        T: Fn(&Self) -> String,
    {
        let content = template(self);
        EntityDocument {
            id: EntityId::Edge {
                src: self.src().id(),
                dst: self.dst().id(),
            },
            content,
        }
    }
}
