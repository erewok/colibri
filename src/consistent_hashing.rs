use std::collections::hash_map::DefaultHasher;
/// Jump consistent hashing implementation
/// Based on:
/// https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
///
///
use std::hash::{Hash, Hasher};

pub const MAGIC_CONSTANT: u64 = 2862933555777941757;

/// This implementation based on the paper
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
        for word_size in 2..50 {
            let string = Alphanumeric.sample_string(&mut rand::thread_rng(), word_size);
            for bucket_count in 1..50 {
                let jmp_hash = jump_consistent_hash(&string, bucket_count);
                assert!(jmp_hash <= bucket_count);
            }
        }
    }
}
