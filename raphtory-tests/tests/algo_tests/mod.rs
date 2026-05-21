use std::collections::HashMap;

#[cfg(test)]
mod centrality;
#[cfg(test)]
mod community_detection;
#[cfg(test)]
mod components;
#[cfg(test)]
mod cores;
#[cfg(test)]
mod embeddings;
#[cfg(test)]
mod metrics;
#[cfg(test)]
mod motifs;
#[cfg(test)]
mod pathing;

fn assert_eq_f64(a: f64, b: f64, precision: f64) {
    assert!((a - b).abs() < precision);
}

fn assert_eq_hashmaps_approx(
    a: &HashMap<String, f64>,
    b: &HashMap<String, f64>,
    precision: f64, // e.g., 1e-5 for 5 decimal places
) {
    assert_eq!(a.len(), b.len(), "HashMaps have different lengths");

    for (key, &val_a) in a {
        let val_b = b.get(key).expect(&format!("Missing key: {}", key));
        let diff = (val_a - val_b).abs();
        assert!(
            diff <= precision,
            "Value mismatch for key '{}': {} != {} (diff: {})",
            key,
            val_a,
            val_b,
            diff
        );
    }
}
