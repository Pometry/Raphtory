use crate::{
    core::utils::errors::GraphError, prelude::Prop, python::graph::pandas::dataframe::PretendDF,
};

pub struct PropIter<'a> {
    inner: Box<dyn Iterator<Item = Vec<(&'a str, Prop)>> + 'a>,
}

impl<'a> Iterator for PropIter<'a> {
    type Item = Vec<(&'a str, Prop)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub(crate) fn get_prop_rows<'a>(
    df: &'a PretendDF,
    props: Option<Vec<&'a str>>,
    const_props: Option<Vec<&'a str>>,
) -> Result<(PropIter<'a>, PropIter<'a>), GraphError> {
    let prop_iter = combine_properties(props, df)?;
    let const_prop_iter = combine_properties(const_props, df)?;
    Ok((prop_iter, const_prop_iter))
}

fn combine_properties<'a>(
    props: Option<Vec<&'a str>>,
    df: &'a PretendDF,
) -> Result<PropIter<'a>, GraphError> {
    let iter = props
        .unwrap_or_default()
        .into_iter()
        .map(|name| lift_property(name, df))
        .reduce(|i1, i2| {
            let i1 = i1?;
            let i2 = i2?;
            Ok(Box::new(i1.zip(i2).map(|(mut v1, v2)| {
                v1.extend(v2);
                v1
            })))
        }).unwrap_or_else(|| Ok(Box::new(std::iter::repeat(vec![]))));

    Ok(PropIter { inner: iter? })
}

pub(crate) fn lift_property<'a: 'b, 'b>(
    name: &'a str,
    df: &'b PretendDF,
) -> Result<Box<dyn Iterator<Item = Vec<(&'b str, Prop)>> + 'b>, GraphError> {
    if let Some(col) = df.iter_col::<f64>(name) {
        Ok(iter_as_prop(name, col))
    } else if let Some(col) = df.iter_col::<f32>(name) {
        Ok(iter_as_prop(name, col))
    } else if let Some(col) = df.iter_col::<i64>(name) {
        Ok(iter_as_prop(name, col))
    } else if let Some(col) = df.iter_col::<u64>(name) {
        Ok(iter_as_prop(name, col))
    } else if let Some(col) = df.iter_col::<u32>(name) {
        Ok(iter_as_prop(name, col))
    } else if let Some(col) = df.iter_col::<i32>(name) {
        Ok(iter_as_prop(name, col))
    } else if let Some(col) = df.bool(name) {
        Ok(Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::Bool(v)))
                .collect::<Vec<_>>()
        })))
    } else if let Some(col) = df.utf8::<i32>(name) {
        Ok(Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::str(v)))
                .collect::<Vec<_>>()
        })))
    } else if let Some(col) = df.utf8::<i64>(name) {
        Ok(Box::new(col.map(move |val| {
            val.into_iter()
                .map(|v| (name, Prop::str(v)))
                .collect::<Vec<_>>()
        })))
    } else {
        Err(GraphError::LoadFailure(format!(
            "Column {} could not be parsed -  must be either u64, i64, f64, f32, bool or string. Ensure it contains no NaN, Null or None values.",
            name
        )))
    }
}
pub(crate) fn lift_layer<'a, S: AsRef<str>>(
    layer: Option<S>,
    layer_in_df: bool,
    df: &'a PretendDF,
) -> Box<dyn Iterator<Item = Option<String>> + 'a> {
    if let Some(layer) = layer {
        if layer_in_df {
            if let Some(col) = df.utf8::<i32>(layer.as_ref()) {
                Box::new(col.map(|v| v.map(|v| v.to_string())))
            } else if let Some(col) = df.utf8::<i64>(layer.as_ref()) {
                Box::new(col.map(|v| v.map(|v| v.to_string())))
            } else {
                Box::new(std::iter::repeat(None))
            }
        } else {
            Box::new(std::iter::repeat(Some(layer.as_ref().to_string())))
        }
    } else {
        Box::new(std::iter::repeat(None))
    }
}

fn iter_as_prop<
    'a: 'b,
    'b,
    T: Into<Prop> + Copy + 'static,
    I: Iterator<Item = Option<&'b T>> + 'a,
>(
    name: &'a str,
    is: I,
) -> Box<dyn Iterator<Item = Vec<(&str, Prop)>> + '_> {
    Box::new(is.map(move |val| {
        val.into_iter()
            .map(|v| (name, (*v).into()))
            .collect::<Vec<_>>()
    }))
}
