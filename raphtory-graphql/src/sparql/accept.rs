//! Content negotiation with the `Accept` header (RFC 9110, section 12.5.1).

/// A media range of an `Accept` header: lower-cased type and subtype (`*` for a wildcard) and
/// its quality.
struct Range {
    kind: String,
    subtype: String,
    q: f32,
}

/// The media ranges of an `Accept` header. Ranges that do not parse are left out.
fn ranges(header: &str) -> Vec<Range> {
    header
        .split(',')
        .filter_map(|item| {
            let mut parts = item.split(';');
            let media = parts.next()?.trim().to_ascii_lowercase();
            let (kind, subtype) = match media.as_str() {
                // sent by some clients for `*/*`
                "*" => ("*", "*"),
                media => media.split_once('/')?,
            };
            if kind.is_empty() || subtype.is_empty() || (kind == "*" && subtype != "*") {
                return None;
            }
            let mut q = 1.0;
            for parameter in parts {
                if let Some((name, value)) = parameter.split_once('=') {
                    if name.trim().eq_ignore_ascii_case("q") {
                        q = value
                            .trim()
                            .parse::<f32>()
                            .ok()
                            .filter(|q| (0.0..=1.0).contains(q))?;
                    }
                }
            }
            Some(Range {
                kind: kind.to_owned(),
                subtype: subtype.to_owned(),
                q,
            })
        })
        .collect()
}

/// The quality of `media_type` (lower case, without parameters): that of the most specific range
/// that matches it, 0 if none does.
fn quality(ranges: &[Range], media_type: &str) -> f32 {
    let Some((kind, subtype)) = media_type.split_once('/') else {
        return 0.0;
    };
    ranges
        .iter()
        .filter_map(|range| {
            let specificity = match (range.kind.as_str(), range.subtype.as_str()) {
                (k, s) if k == kind && s == subtype => 2,
                (k, "*") if k == kind => 1,
                ("*", "*") => 0,
                _ => return None,
            };
            Some((specificity, range.q))
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

/// The formats the `Accept` header `header` accepts among `formats`, each listed with the media
/// types it is served for, the preferred first: by quality, ties going to the earlier format.
/// No header (or one without a valid range) accepts them all, in order. Empty if the header
/// accepts none of them.
pub(super) fn rank<T: Copy>(header: Option<&str>, formats: &[(T, &[&str])]) -> Vec<T> {
    let ranges = header.map(ranges).unwrap_or_default();
    if ranges.is_empty() {
        return formats.iter().map(|(format, _)| *format).collect();
    }
    let mut accepted: Vec<(T, f32)> = formats
        .iter()
        .filter_map(|(format, media_types)| {
            let q = media_types
                .iter()
                .map(|media_type| quality(&ranges, media_type))
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
        let rows: &[(Option<&str>, &[&str])] = &[
            // no preference: all, in order
            (None, &all),
            (Some(""), &all),
            (Some("*"), &all),
            (Some("*/*"), &all),
            (Some("garbage"), &all),
            // case and parameters other than q are ignored
            (
                Some("Text/CSV;charset=utf-8 , application/*; q=0.5,*;q=0.1"),
                &["csv", "json", "xml", "tsv"],
            ),
            (Some("TEXT/*;Q=0, */*"), &["json", "xml"]),
            // invalid ranges are left out
            (
                Some(
                    "text, */json, text/tab-separated-values;q=2, \
                     text/tab-separated-values;q=x, , text/*;q=0.5",
                ),
                &["csv", "tsv"],
            ),
            // by quality, ties in order
            (Some("text/csv"), &["csv"]),
            (Some("application/xml"), &["xml"]),
            (Some("application/*"), &["json", "xml"]),
            (Some("text/tab-separated-values, text/csv"), &["csv", "tsv"]),
            (
                Some("text/*;q=0.5, text/tab-separated-values"),
                &["tsv", "csv"],
            ),
            (
                Some("application/sparql-results+xml, application/sparql-results+json;q=0.5"),
                &["xml", "json"],
            ),
            (
                Some("text/csv;q=0.5, application/sparql-results+xml, */*;q=0.1"),
                &["xml", "csv", "json", "tsv"],
            ),
            (
                Some("*/*;q=0.1, text/tab-separated-values;q=0.2"),
                &["tsv", "json", "xml", "csv"],
            ),
            // the most specific range decides, so q=0 refuses
            (Some("text/csv;q=0, text/*"), &["tsv"]),
            (
                Some("text/*, application/json;q=0, */*;q=0.2"),
                &["csv", "tsv", "json", "xml"],
            ),
            (
                Some(
                    "text/*, application/sparql-results+json;q=0, application/json;q=0, \
                     */*;q=0.2",
                ),
                &["csv", "tsv", "xml"],
            ),
            // nothing acceptable
            (Some("text/turtle"), &[]),
            (Some("*/*;q=0"), &[]),
            (Some("text/csv;q=0, text/tab-separated-values;q=0"), &[]),
        ];
        for (header, expected) in rows {
            assert_eq!(rank(*header, FORMATS), *expected, "{header:?}");
        }
    }
}
