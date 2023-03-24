use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Jump consistent hashing implementation
/// Based on:
/// https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
///
///

pub const MAGIC_CONSTANT: u64 = 2862933555777941757;

/// This implementation based on the paper.
pub fn jump_consistent_hash(key: &str, number_of_buckets: u32) -> u32 {
    let mut hasher: DefaultHasher = Default::default();
    key.hash(&mut hasher);
    let key_as_u64: u64 = hasher.finish();

    let mut b: u32 = 1;
    let mut j: u32 = 0;
    let mut _key: u64 = key_as_u64;
    while j < number_of_buckets {
        b = j;
        _key = _key.wrapping_mul(MAGIC_CONSTANT) + 1;
        let shiftkey = (_key >> 33) as u32;
        j = (b + 1) * ((1 << 31) / (shiftkey + 1));
    }
    b
}

/// Return bucket number to left and right of the selected one
pub fn get_neighbor_bucket(bucket_selected: u32, number_of_buckets: u32) -> (u32, u32) {
    if bucket_selected == 0 {
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
    use rand::distributions::{Alphanumeric, DistString};

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
                let string = Alphanumeric.sample_string(&mut rand::thread_rng(), word_size);

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
}
