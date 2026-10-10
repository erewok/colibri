//! Properties of the jump consistent hash used for hashring routing.

use colibri::node::hashring::consistent_hashing::{get_neighbor_bucket, jump_consistent_hash};
use hegel::TestCase;
use hegel::generators as gs;

const MAX_BUCKETS: u32 = 10_000;

fn draw_bucket_count(tc: &TestCase) -> u32 {
    tc.draw(gs::integers::<u32>().min_value(1).max_value(MAX_BUCKETS))
}

#[hegel::test]
fn hash_is_a_valid_bucket(tc: TestCase) {
    let key = tc.draw(gs::text());
    let buckets = draw_bucket_count(&tc);
    assert!(jump_consistent_hash(&key, buckets) < buckets);
}

/// Jump hashing's defining property: growing the ring from n to n + 1
/// buckets either leaves a key where it was or moves it to the new bucket.
#[hegel::test]
fn growing_the_ring_only_moves_keys_to_the_new_bucket(tc: TestCase) {
    let key = tc.draw(gs::text());
    let buckets = tc.draw(
        gs::integers::<u32>()
            .min_value(1)
            .max_value(MAX_BUCKETS - 1),
    );
    let before = jump_consistent_hash(&key, buckets);
    let after = jump_consistent_hash(&key, buckets + 1);
    assert!(
        after == before || after == buckets,
        "{key:?} moved from bucket {before} to {after} when growing to {} buckets",
        buckets + 1
    );
}

/// Neighbours are the adjacent buckets on a ring of `buckets` positions.
#[hegel::test]
fn neighbours_are_ring_adjacent(tc: TestCase) {
    let buckets = draw_bucket_count(&tc);
    let selected = tc.draw(gs::integers::<u32>().max_value(buckets - 1));
    let expected = ((selected + buckets - 1) % buckets, (selected + 1) % buckets);
    assert_eq!(get_neighbor_bucket(selected, buckets), expected);
}
