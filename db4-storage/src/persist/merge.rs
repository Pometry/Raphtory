use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct MergeConfig {
    /// Maximum number of components before triggering a merge (size-tiered policy)
    pub component_count_threshold: usize,
    /// Maximum size ratio between smallest and largest components to consider merging
    pub size_ratio_threshold: f64,
    /// Maximum size of a component that can be considered for merging (in MB)
    pub max_component_size_mb: usize,
    /// Prefer merging smaller components first
    pub prefer_small_merges: bool,
}

impl Default for MergeConfig {
    fn default() -> Self {
        MergeConfig {
            component_count_threshold: 4,
            size_ratio_threshold: 10.0,
            max_component_size_mb: 500,
            prefer_small_merges: true,
        }
    }
}

impl MergeConfig {
    pub fn with_max_component_size_mb(mut self, size_mb: usize) -> Self {
        self.max_component_size_mb = size_mb;
        self
    }

    pub fn unlimited_component_size(mut self) -> Self {
        self.max_component_size_mb = usize::MAX / 1024 / 1024;
        self
    }

    pub fn with_component_count_threshold(mut self, threshold: usize) -> Self {
        self.component_count_threshold = threshold;
        self
    }
}

pub trait HasMergeConfig {
    fn merge_config(&self) -> &MergeConfig;

    fn with_merge_config(self, config: MergeConfig) -> Self;
}

pub trait HasSize {
    fn size(&self) -> usize;
}

impl MergeConfig {
    /// Determines if disk components should be merged based on values in the config.
    /// Returns Option<(usize, usize)> with the indexes of pages that should be merged.
    pub fn should_merge_disk_segments<T: HasSize>(
        &self,
        disk_segments: &[T],
    ) -> Option<(usize, usize)> {
        let pages_len = disk_segments.len();

        // Need at least 2 pages to merge
        if pages_len < 2 {
            return None;
        }

        // Size-tiered check: enough components to trigger merge?
        let enough_components = pages_len >= self.component_count_threshold;

        if !enough_components {
            return None;
        }

        // Collect sizes of all pages
        let mut page_sizes: Vec<(usize, usize)> = disk_segments
            .iter()
            .enumerate()
            .map(|(idx, page)| (idx, page.size()))
            .collect();

        // Sort by size for easy comparison and selection
        page_sizes.sort_by_key(|(_, size)| *size);

        // Max component size in bytes
        let max_size_bytes = self.max_component_size_mb * 1024 * 1024;

        // Find candidate pairs for merging
        let mut merge_candidates = Vec::new();

        // Compare adjacent components for possible merges
        for i in 0..page_sizes.len() - 1 {
            let (idx1, size1) = page_sizes[i];

            for j in i + 1..page_sizes.len() {
                let (idx2, size2) = page_sizes[j];

                // Skip if either component is too large
                if size1 > max_size_bytes || size2 > max_size_bytes {
                    continue;
                }

                // Skip if combined size exceeds max size
                if size1 + size2 > max_size_bytes {
                    continue;
                }

                // Calculate size ratio
                let ratio = size2 as f64 / size1 as f64;

                // If size ratio is within threshold, add as candidate
                if ratio <= self.size_ratio_threshold {
                    // Score this candidate pair (lower is better)
                    let score = if self.prefer_small_merges {
                        // Prefer smaller components
                        size1 + size2
                    } else {
                        // Prefer larger ratio differences (more balanced)
                        ((self.size_ratio_threshold - ratio) * 1000.0) as usize
                    };

                    merge_candidates.push((idx1, idx2, score));
                }
            }
        }

        // If we have candidates, choose the best one
        if !merge_candidates.is_empty() {
            // Sort by score (lower is better)
            merge_candidates.sort_by_key(|(_, _, score)| *score);
            let (idx1, idx2, _) = merge_candidates[0];

            // Return the page indexes to merge
            return Some((idx1, idx2));
        }

        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct MockPage {
        size: usize,
    }

    impl HasSize for MockPage {
        fn size(&self) -> usize {
            self.size
        }
    }

    #[test]
    fn test_one_page() {
        let actual = MergeConfig::default().should_merge_disk_segments(&[MockPage { size: 1 }]);
        assert_eq!(actual, None);
    }

    #[test]
    fn test_two_pages() {
        let actual = MergeConfig::default()
            .should_merge_disk_segments(&[MockPage { size: 1 }, MockPage { size: 2 }]);
        assert_eq!(actual, None);
    }

    #[test]
    fn test_three_pages() {
        let actual = MergeConfig::default().should_merge_disk_segments(&[
            MockPage { size: 1 },
            MockPage { size: 2 },
            MockPage { size: 3 },
        ]);
        assert_eq!(actual, None);
    }

    #[test]
    fn dont_merge_small_page_into_big() {
        let actual = MergeConfig::default().should_merge_disk_segments(&[
            MockPage { size: 1 },
            MockPage { size: 2_000_000 },
            MockPage { size: 3_000_000 },
            MockPage { size: 4_000_000 },
        ]);
        assert_eq!(actual, Some((1, 2)));
    }

    #[test]
    fn dont_merge_big_page_into_small() {
        let actual = MergeConfig::default().should_merge_disk_segments(&[
            MockPage { size: 1_000_000 },
            MockPage { size: 2 },
            MockPage { size: 3 },
            MockPage { size: 4 },
        ]);
        assert_eq!(actual, Some((1, 2)));
    }

    #[test]
    fn skip_when_too_large() {
        let actual = MergeConfig::default().should_merge_disk_segments(&[
            MockPage { size: 1 },
            MockPage { size: 600_000_000 },
            MockPage { size: 600_000_000 },
            MockPage { size: 600_000_000 },
            MockPage { size: 600_000_000 },
        ]);
        assert_eq!(actual, None);
    }

    #[test]
    fn merge_the_small_ones() {
        let actual = MergeConfig::default().should_merge_disk_segments(&[
            MockPage { size: 60_000_000 },
            MockPage { size: 1 },
            MockPage { size: 60_000_000 },
            MockPage { size: 60_000_000 },
            MockPage { size: 1 },
            MockPage { size: 60_000_000 },
        ]);
        assert_eq!(actual, Some((1, 4)));
    }

    #[test]
    fn merge_small_pages() {
        let actual = MergeConfig::default().should_merge_disk_segments(&[
            MockPage { size: 1 },
            MockPage { size: 2 },
            MockPage { size: 3 },
            MockPage { size: 4 },
            MockPage { size: 5 },
        ]);
        assert_eq!(actual, Some((0, 1)));
    }
}
