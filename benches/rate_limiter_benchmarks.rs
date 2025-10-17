use colibri::cli::RateLimitSettings;
use colibri::consistent_hashing::jump_consistent_hash;
use colibri::rate_limit::RateLimiter;
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;

fn benchmark_rate_limiter_sequential(c: &mut Criterion) {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 1000000, // High limit to avoid blocking
        rate_limit_interval_seconds: 3600,
    };

    c.bench_function("rate_limiter_sequential", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut limiter = RateLimiter::new(settings.clone());
            counter += 1;
            let client_id = format!("benchmark_client_{}", counter % 1000);
            black_box(limiter.limit_calls_for_client(client_id))
        })
    });
}

fn benchmark_rate_limiter_check_remaining(c: &mut Criterion) {
    let settings = RateLimitSettings {
        rate_limit_max_calls_allowed: 1000000,
        rate_limit_interval_seconds: 3600,
    };

    let limiter = RateLimiter::new(settings);

    c.bench_function("rate_limiter_check_remaining", |b| {
        let mut counter = 0;
        b.iter(|| {
            counter += 1;
            let client_id = format!("check_client_{}", counter % 100);
            black_box(limiter.check_calls_remaining_for_client(&client_id))
        })
    });
}

fn benchmark_consistent_hashing(c: &mut Criterion) {
    c.bench_function("consistent_hashing", |b| {
        let mut counter = 0;
        b.iter(|| {
            counter += 1;
            let key = format!("key_{}", counter);
            let bucket_count = 100u32;
            black_box(jump_consistent_hash(&key, bucket_count))
        })
    });
}

criterion_group!(
    benches,
    benchmark_rate_limiter_sequential,
    benchmark_rate_limiter_check_remaining,
    benchmark_consistent_hashing
);
criterion_main!(benches);
