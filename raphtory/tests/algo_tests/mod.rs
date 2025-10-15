use std::collections::HashMap;

mod centrality;
mod community_detection;
mod components;
mod cores;
mod embeddings;
mod metrics;
mod motifs;
mod pathing;

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
