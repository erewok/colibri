use std::hash::{Hash, Hasher};

use gxhash::GxHasher;

pub const MAGIC_CONSTANT: u64 = 2862933555777941757;
/// Jump consistent hashing implementation
/// Based on:
/// https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
///
/// This implementation based on the paper.
pub fn jump_consistent_hash(key: &str, number_of_buckets: u32) -> u32 {
    // Problematically, we have to hash the key string into a u64 first
    // This may be a problem because different strings may collide into the same u64
    // but we mostly need a *stable hash* for consistent hashing to work properly.
    let mut hasher: GxHasher = GxHasher::default();
    key.hash(&mut hasher);
    let key_as_u64: u64 = hasher.finish();
    let bucket_num = number_of_buckets as i64;

    let mut b: i64 = -1;
    let mut j: i64 = 0;
    let mut _key: u64 = key_as_u64;

    while j < bucket_num {
        b = j;
        _key = _key.wrapping_mul(MAGIC_CONSTANT).wrapping_add(1);
        j = ((b + 1) as f64 * ((1i64 << 31) as f64 / ((_key >> 33) + 1) as f64)) as i64;
    }
    b as u32
}

/// Return bucket number to left and right of the selected one
pub fn get_neighbor_bucket(bucket_selected: u32, number_of_buckets: u32) -> (u32, u32) {
    if number_of_buckets == 1 {
        // Special case: only one bucket
        (0, 0)
    } else if number_of_buckets == 2 {
        // Special case: two buckets - neighbor is the other one
        if bucket_selected == 0 {
            (1, 1)
        } else {
            (0, 0)
        }
    } else if bucket_selected == 0 {
        // bucket is first
        (number_of_buckets - 1, 1)
    } else if bucket_selected == number_of_buckets - 1 {
        // bucket is last
        (bucket_selected - 1, 0)
    } else {
        // bucket is somewhere in between
        (bucket_selected - 1, bucket_selected + 1)
    }
}

#[cfg(test)]
mod tests {
    use rand::distr::{Alphanumeric, SampleString};

    use super::*;

    #[test]
    fn check_consistency() {
        // same key; same count
        let key = "test".to_string();
        let expected = jump_consistent_hash(&key, 10);
        assert_eq!(expected, jump_consistent_hash(&key, 10));

        // generate different random strings
        // note: 1 bucket is uninteresting because the answer is always 0
        let mut max_bucket: u32 = 0;
        for bucket_count in 1..50 {
            for word_size in 2..50 {
                let string = Alphanumeric.sample_string(&mut rand::rng(), word_size);

                let jmp_hash = jump_consistent_hash(&string, bucket_count);
                // We should perform some statistical analysis on the distribution of these jmp_hash values
                assert!(jmp_hash < bucket_count);
                // Sanity check: it should never exceed this value
                max_bucket = std::cmp::max(max_bucket, jmp_hash);
            }
            assert!(max_bucket < bucket_count)
        }
    }

    #[test]
    fn check_get_neighbor_bucket() {
        let result_first = get_neighbor_bucket(0, 23);
        assert_eq!(result_first, (22, 1));
        let result_last = get_neighbor_bucket(22, 23);
        assert_eq!(result_last, (21, 0));
        let result_middle = get_neighbor_bucket(17, 23);
        assert_eq!(result_middle, (16, 18));
    }

    #[test]
    fn test_consistent_hashing_deterministic() {
        let key = "test_key";
        let bucket_count = 10;

        // Same input should always produce same output
        let result1 = jump_consistent_hash(key, bucket_count);
        let result2 = jump_consistent_hash(key, bucket_count);
        let result3 = jump_consistent_hash(key, bucket_count);

        assert_eq!(result1, result2);
        assert_eq!(result2, result3);
        assert!(result1 < bucket_count);
    }

    #[test]
    fn test_consistent_hashing_bounds() {
        let test_cases = vec![
            ("key1", 1),
            ("key2", 5),
            ("key3", 10),
            ("key4", 50),
            ("key5", 100),
            ("very_long_key_with_lots_of_characters", 73),
            ("", 25), // Empty string
        ];

        for (key, bucket_count) in test_cases {
            let result = jump_consistent_hash(key, bucket_count);
            assert!(
                result < bucket_count,
                "Hash result {} should be less than bucket count {} for key '{}'",
                result,
                bucket_count,
                key
            );
        }
    }

    #[test]
    fn test_consistent_hashing_distribution() {
        use std::collections::HashMap;

        let bucket_count = 10;
        let mut bucket_counts = HashMap::new();

        // Test distribution across 1000 keys
        for i in 0..1000 {
            let key = format!("key_{}", i);
            let bucket = jump_consistent_hash(&key, bucket_count);
            *bucket_counts.entry(bucket).or_insert(0) += 1;
        }

        // Each bucket should get roughly equal distribution (within 50%)
        let expected_per_bucket = 1000 / bucket_count as usize;
        let tolerance = expected_per_bucket * 50 / 100; // 50% tolerance

        // Verify all buckets are used
        assert_eq!(bucket_counts.len(), bucket_count as usize);

        for (bucket, count) in bucket_counts.iter() {
            assert!(
                *count >= expected_per_bucket - tolerance
                    && *count <= expected_per_bucket + tolerance,
                "Bucket {} has {} items, expected {} ± {} ({}% tolerance)",
                bucket,
                count,
                expected_per_bucket,
                tolerance,
                (tolerance * 100) / expected_per_bucket
            );
        }
    }

    #[test]
    fn test_neighbor_bucket_edge_cases() {
        // Test first bucket (edge case)
        assert_eq!(get_neighbor_bucket(0, 10), (9, 1));

        // Test last bucket (edge case)
        assert_eq!(get_neighbor_bucket(9, 10), (8, 0));

        // Test middle bucket
        assert_eq!(get_neighbor_bucket(5, 10), (4, 6));

        // Test with different bucket counts
        assert_eq!(get_neighbor_bucket(2, 5), (1, 3));
        assert_eq!(get_neighbor_bucket(0, 5), (4, 1));
        assert_eq!(get_neighbor_bucket(4, 5), (3, 0));

        // Edge case: single bucket
        assert_eq!(get_neighbor_bucket(0, 1), (0, 0));

        // Edge case: two buckets
        assert_eq!(get_neighbor_bucket(0, 2), (1, 1));
        assert_eq!(get_neighbor_bucket(1, 2), (0, 0));
    }

    #[test]
    fn test_consistent_hashing_reproducibility() {
        // Test that the same key produces the same hash across multiple calls
        let key = "reproducibility_test";
        let bucket_count = 42;

        let first_result = jump_consistent_hash(key, bucket_count);

        // Call multiple times to ensure consistency
        for _ in 0..100 {
            let result = jump_consistent_hash(key, bucket_count);
            assert_eq!(result, first_result, "Hash result changed across calls");
        }
    }
}
