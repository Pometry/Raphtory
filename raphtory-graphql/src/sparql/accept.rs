//! Content negotiation with the `Accept` header (RFC 9110, section 12.5.1).
use poem::web::headers::Mime;

/// The media ranges of `accept`, poem's `Accept` (sorted by quality, the preferred first), with
/// their quality, as `SparqlFormat::from_media_types` takes them. A range whose q is not a
/// number from 0 to 1 is left out. No ranges accept anything.
pub(super) fn ranges(accept: &[Mime]) -> Vec<(&str, f32)> {
    if accept.is_empty() {
        return vec![("*/*", 1.0)];
    }
    accept
        .iter()
        .filter_map(|range| {
            let q = match range.get_param("q") {
                Some(q) => q
                    .as_str()
                    .parse::<f32>()
                    .ok()
                    .filter(|q| (0.0..=1.0).contains(q))?,
                None => 1.0,
            };
            Some((range.essence_str(), q))
        })
        .collect()
}
