use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

pub trait ToMillis {
    fn to_millis(&self) -> Result<i64, SystemTimeError>;
}
impl ToMillis for SystemTime {
    fn to_millis(&self) -> Result<i64, SystemTimeError> {
        Ok(self.duration_since(UNIX_EPOCH)?.as_millis() as i64)
    }
}
