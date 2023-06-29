use dynamic_graphql::SimpleObject;

#[derive(SimpleObject)]
pub(crate) struct PropertyUpdate {
    pub(crate) time: i64,
    pub(crate) value: String,
}

impl PropertyUpdate {
    pub fn new(time: i64, value: String) -> Self {
        Self { time, value }
    }
}
