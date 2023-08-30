use dynamic_graphql::SimpleObject;

/// A single property at a given `time` with a given `value`
#[derive(SimpleObject)]
pub(crate) struct PropertyUpdate {
    pub(crate) time: i64,
    pub(crate) value: String,
}

// A collection of `PropertyUpdate`s under their `propertyName`
#[derive(SimpleObject)]
pub(crate) struct PropertyUpdateGroup {
    pub(crate) property_name: String,
    pub(crate) property_updates: Vec<PropertyUpdate>,
}

impl PropertyUpdate {
    pub fn new(time: i64, value: String) -> Self {
        Self { time, value }
    }
}

impl PropertyUpdateGroup {
    pub fn new(property_name: String, property_updates: Vec<PropertyUpdate>) -> Self {
        Self {
            property_name,
            property_updates,
        }
    }
}
