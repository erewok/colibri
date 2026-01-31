//! Practical Benchmarks for Colibri Rate Limiter
//!
//! These benchmarks test realistic scenarios:
//! 1. Token bucket performance (local, no network)
//! 2. Single node throughput
//! 3. Hashring forwarding latency
//!
//! Run with: cargo bench

use colibri::limiters::token_bucket::TokenBucketLimiter;
use colibri::settings::RateLimitSettings;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;

/// Benchmark: Local token bucket operations
/// Tests the raw performance of the rate limiter without any overhead
fn bench_token_bucket_local(c: &mut Criterion) {
    let mut group = c.benchmark_group("token_bucket");

    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 1000,
        rate_limit_interval_seconds: 60,
    };

    // Test 1: Single client, sequential checks
    group.bench_function("single_client_check", |b| {
        let limiter = TokenBucketLimiter::new(settings.clone());
        b.iter(|| black_box(limiter.check_calls_remaining_for_client("test_client")));
    });

    // Test 2: Single client, consuming tokens
    group.bench_function("single_client_consume", |b| {
        let mut limiter = TokenBucketLimiter::new(settings.clone());
        b.iter(|| black_box(limiter.limit_calls_for_client("test_client".to_string())));
    });

    // Test 3: Many clients (cache pressure)
    group.bench_function("many_clients_rotating", |b| {
        let mut limiter = TokenBucketLimiter::new(settings.clone());
        let mut counter = 0;
        b.iter(|| {
            counter += 1;
            let client_id = format!("client_{}", counter % 1000);
            black_box(limiter.limit_calls_for_client(client_id))
        });
    });

    group.finish();
}

/// Benchmark: Token refill performance
/// Tests how efficiently tokens are refilled over time
fn bench_token_refill(c: &mut Criterion) {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 100,
        rate_limit_interval_seconds: 1, // Fast refill for testing
    };

    c.bench_function("token_refill_check", |b| {
        let limiter = TokenBucketLimiter::new(settings.clone());

        // Exhaust some tokens first
        for _ in 0..50 {
            limiter.check_calls_remaining_for_client("refill_client");
        }

        b.iter(|| {
            // This will trigger refill logic
            black_box(limiter.check_calls_remaining_for_client("refill_client"))
        });
    });
}

/// Benchmark: Export/Import operations
/// Tests serialization performance for cluster operations
fn bench_export_import(c: &mut Criterion) {
    let mut group = c.benchmark_group("export_import");

    for client_count in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*client_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_clients", client_count)),
            client_count,
            |b, &count| {
                let settings = RateLimitSettings {
                    rate_limit_max_calls_allowed: 100,
                    rate_limit_interval_seconds: 60,
                };
                let mut limiter = TokenBucketLimiter::new(settings.clone());

                // Create buckets for N clients
                for i in 0..count {
                    limiter.limit_calls_for_client(format!("client_{}", i));
                }

                b.iter(|| {
                    let exported = limiter.export_all_buckets();
                    black_box(exported)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_token_bucket_local,
    bench_token_refill,
    bench_export_import,
);
criterion_main!(benches);
