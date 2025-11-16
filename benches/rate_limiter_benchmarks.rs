use colibri::limiters::distributed_bucket::DistributedBucketLimiter;
use colibri::settings::RateLimitSettings;
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
            let mut limiter: DistributedBucketLimiter =
                DistributedBucketLimiter::new(1.into(), settings.clone());
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

    let limiter: DistributedBucketLimiter =
        DistributedBucketLimiter::new(1.into(), settings.clone());

    c.bench_function("rate_limiter_check_remaining", |b| {
        let mut counter = 0;
        b.iter(|| {
            counter += 1;
            let client_id = format!("check_client_{}", counter % 100);
            black_box(limiter.check_calls_remaining_for_client(&client_id))
        })
    });
}

criterion_group!(
    benches,
    benchmark_rate_limiter_sequential,
    benchmark_rate_limiter_check_remaining
);
criterion_main!(benches);
