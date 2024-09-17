use minijinja::{value::Kwargs, Error, State, Value};
use minijinja_contrib::filters::datetimeformat as minijinja_datetimeformat;

/// Formats a timestamp as date and time.
///
/// The value needs to be a datetime managed by Raphtory.
///
/// The filter accepts two keyword arguments (`format` and `tz`) to influence the format
/// and the timezone.  The default format is `"medium"`.  The defaults for these keyword
/// arguments are taken from two global variables in the template context: `DATETIME_FORMAT`
/// and `TIMEZONE`.  If the timezone is set to `"original"` or is not configured, then
/// the timezone of the value is retained.  Otherwise the timezone is the name of a
/// timezone [from the database](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).
///
/// ```jinja
/// {{ value|datetimeformat }}
/// ```
///
/// ```jinja
/// {{ value|datetimeformat(format="short") }}
/// ```
///
/// ```jinja
/// {{ value|datetimeformat(format="short", tz="Europe/Vienna") }}
/// ```
///
/// This filter currently uses the `time` crate to format dates and uses the format
/// string specification of that crate in version 2.  For more information read the
/// [Format description documentation](https://time-rs.github.io/book/api/format-description.html).
/// Additionally some special formats are supported:
///
/// * `short`: a short date and time format (`2023-06-24 16:37`)
/// * `medium`: a medium length date and time format (`Jun 24 2023 16:37`)
/// * `long`: a longer date and time format (`June 24 2023 16:37:22`)
/// * `full`: a full date and time format (`Saturday, June 24 2023 16:37:22`)
/// * `unix`: a unix timestamp in seconds only (`1687624642`)
/// * `iso`: date and time in iso format (`2023-06-24T16:37:22+00:00`)
pub fn datetimeformat(state: &State, value: Value, kwargs: Kwargs) -> Result<String, Error> {
    let millis = i64::try_from(value)?;
    let seconds: f64 = (millis as f64) / 1000f64;
    minijinja_datetimeformat(state, Value::from(Value::from(seconds)), kwargs)
}
