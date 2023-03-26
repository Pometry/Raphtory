use crate::{utils, Prop};

pub trait InputVertex {
    fn id(&self) -> u64;
    fn name_prop(&self) -> Option<Prop>;
}

impl InputVertex for u64 {
    fn id(&self) -> u64 {
        *self
    }

    fn name_prop(&self) -> Option<Prop> {
        None
    }
}

impl<'a> InputVertex for &'a str {
    fn id(&self) -> u64 {
        utils::calculate_hash(self)
    }

    fn name_prop(&self) -> Option<Prop> {
        Some(Prop::Str(self.to_string()))
    }
}

impl<'a> InputVertex for String {
    fn id(&self) -> u64 {
        utils::calculate_hash(self)
    }

    fn name_prop(&self) -> Option<Prop> {
        Some(Prop::Str(self.to_string()))
    }
}

