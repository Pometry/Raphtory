//! Content negotiation with the `Accept` header (RFC 9110, section 12.5.1).
use poem::web::headers::Mime;

/// The quality of `media_type` (lower case, without parameters): that of the most specific range
/// of `accept` that matches it, 0 if none does. A range whose q is not a number from 0 to 1 is
/// left out.
fn quality(accept: &[Mime], media_type: &str) -> f32 {
    let Some((kind, _)) = media_type.split_once('/') else {
        return 0.0;
    };
    accept
        .iter()
        .filter_map(|range| {
            let q = range.get_param("q").map_or(Some(1.0), |q| {
                q.as_str()
                    .parse::<f32>()
                    .ok()
                    .filter(|q| (0.0..=1.0).contains(q))
            })?;
            let specificity = match (range.type_().as_str(), range.subtype().as_str()) {
                ("*", "*") => 0,
                (k, "*") if k == kind => 1,
                // the essence keeps a `+xml` suffix, which `subtype()` leaves out
                _ if range.essence_str() == media_type => 2,
                _ => return None,
            };
            Some((specificity, q))
        })
        .fold(
            None,
            |best: Option<(u8, f32)>, (specificity, q)| match best {
                Some((best_specificity, best_q))
                    if best_specificity > specificity
                        || (best_specificity == specificity && best_q >= q) =>
                {
                    best
                }
                _ => Some((specificity, q)),
            },
        )
        .map_or(0.0, |(_, q)| q)
}

/// The formats the media ranges `accept` of an `Accept` header accept among `formats`, each
/// listed with the media types it is served for, the preferred first: by quality, ties going to
/// the earlier format. No range accepts them all, in order. Empty if the ranges accept none of
/// them.
pub(super) fn rank<T: Copy>(accept: &[Mime], formats: &[(T, &[&str])]) -> Vec<T> {
    if accept.is_empty() {
        return formats.iter().map(|(format, _)| *format).collect();
    }
    let mut accepted: Vec<(T, f32)> = formats
        .iter()
        .filter_map(|(format, media_types)| {
            let q = media_types
                .iter()
                .map(|media_type| quality(accept, media_type))
                .fold(0.0, f32::max);
            (q > 0.0).then_some((*format, q))
        })
        .collect();
    // stable, so ties keep the order of `formats`
    accepted.sort_by(|(_, a), (_, b)| b.total_cmp(a));
    accepted.into_iter().map(|(format, _)| format).collect()
}

#[cfg(test)]
mod tests {
    use super::rank;
    use poem::web::headers::Mime;

    const FORMATS: &[(&str, &[&str])] = &[
        (
            "json",
            &["application/sparql-results+json", "application/json"],
        ),
        (
            "xml",
            &["application/sparql-results+xml", "application/xml"],
        ),
        ("csv", &["text/csv"]),
        ("tsv", &["text/tab-separated-values"]),
    ];

    #[test]
    fn ranking() {
        let all = ["json", "xml", "csv", "tsv"];
        let rows: &[(&str, &[&str])] = &[
            // no preference: all, in order
            ("", &all),
            ("*/*", &all),
            // by quality, ties in order
            ("text/csv", &["csv"]),
            ("application/xml", &["xml"]),
            ("application/*", &["json", "xml"]),
            ("text/tab-separated-values, text/csv", &["csv", "tsv"]),
            ("text/*;q=0.5, text/tab-separated-values", &["tsv", "csv"]),
            (
                "application/sparql-results+xml, application/sparql-results+json;q=0.5",
                &["xml", "json"],
            ),
            (
                "text/csv;q=0.5, application/sparql-results+xml, */*;q=0.1",
                &["xml", "csv", "json", "tsv"],
            ),
            (
                "*/*;q=0.1, text/tab-separated-values;q=0.2",
                &["tsv", "json", "xml", "csv"],
            ),
            // the most specific range decides, so q=0 refuses
            ("text/csv;q=0, text/*", &["tsv"]),
            (
                "text/*, application/json;q=0, */*;q=0.2",
                &["csv", "tsv", "json", "xml"],
            ),
            (
                "text/*, application/sparql-results+json;q=0, application/json;q=0, */*;q=0.2",
                &["csv", "tsv", "xml"],
            ),
            // nothing acceptable
            ("text/turtle", &[]),
            ("*/*;q=0", &[]),
            ("text/csv;q=0, text/tab-separated-values;q=0", &[]),
        ];
        for (header, expected) in rows {
            let accept: Vec<Mime> = header
                .split(',')
                .filter(|range| !range.trim().is_empty())
                .map(|range| range.trim().parse().unwrap())
                .collect();
            assert_eq!(rank(&accept, FORMATS), *expected, "{header:?}");
        }
    }
}
