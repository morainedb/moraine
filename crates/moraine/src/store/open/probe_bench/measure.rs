//! Reproducible policy comparisons; output is CSV, timings are not assertions.

use cpu_time::ProcessTime;
use tokio::sync::Barrier;

use super::*;

#[derive(Clone, Copy)]
struct Policy {
    name: &'static str,
    group_size: usize,
    limit: Option<usize>,
}

const POLICIES: [Policy; 9] = [
    Policy {
        name: "individual",
        group_size: 1,
        limit: None,
    },
    Policy {
        name: "grouped",
        group_size: 16,
        limit: None,
    },
    Policy {
        name: "shared32",
        group_size: 1,
        limit: Some(32),
    },
    Policy {
        name: "shared128",
        group_size: 1,
        limit: Some(128),
    },
    Policy {
        name: "shared512",
        group_size: 1,
        limit: Some(512),
    },
    Policy {
        name: "grouped128",
        group_size: 16,
        limit: Some(128),
    },
    Policy {
        name: "grouped64",
        group_size: 64,
        limit: None,
    },
    Policy {
        name: "grouped192",
        group_size: 192,
        limit: None,
    },
    Policy {
        name: "coalesced",
        group_size: 1,
        limit: None,
    },
];

fn numbers(shape: &str, statement: u64, wave: u64, sample: u64) -> Vec<u64> {
    let offset = 256 + sample * 8192;
    (0..192)
        .map(|key| match shape {
            "clustered" => offset + statement * 4096 + wave * 512 + key,
            "gapped" => offset + statement * 4096 + wave * 2048 + key * 8,
            "scattered" => 256 + key * 257 + statement * 16 + wave * 8 + sample * 2048,
            _ => unreachable!(),
        })
        .collect()
}

struct Observation {
    wall: f64,
    cpu: f64,
    median: f64,
    maximum: f64,
}

async fn measure(db: &Arc<Db>, queries: &[Vec<u64>], policy: Policy) -> Observation {
    let barrier = Arc::new(Barrier::new(queries.len() + 1));
    let limit = policy.limit.map(|limit| Arc::new(Semaphore::new(limit)));
    let mut tasks = Vec::new();
    for numbers in queries {
        let expected: Vec<_> = numbers
            .iter()
            .flat_map(|number| number * FANOUT..(number + 1) * FANOUT)
            .collect();
        let (db, barrier, limit, numbers) =
            (db.clone(), barrier.clone(), limit.clone(), numbers.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let start = Instant::now();
            let rows = lookup(&db, &numbers, policy.group_size, limit).await;
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(rows, expected);
            elapsed
        }));
    }
    let cpu = ProcessTime::now();
    let start = Instant::now();
    barrier.wait().await;
    let mut times = Vec::new();
    for task in tasks {
        times.push(task.await.unwrap());
    }
    let wall = start.elapsed().as_secs_f64() * 1000.0;
    let cpu = cpu.elapsed().as_secs_f64() * 1000.0;
    times.sort_by(f64::total_cmp);
    Observation {
        wall,
        cpu,
        median: times[times.len() / 2],
        maximum: *times.last().unwrap(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual probe-policy benchmark"]
async fn probe_policy_benchmark() {
    let samples: u64 =
        std::env::var("MORAINE_PROBE_BENCH_SAMPLES").map_or(3, |value| value.parse().unwrap());
    assert!((1..=3).contains(&samples));
    let (db, cache, objects) = fixture(65_536).await;
    let db = Arc::new(db);
    println!(
        "transport,statements,shape,policy,sample,phase,wall_ms,cpu_ms,median_statement_ms,max_statement_ms,gets,bytes,peak_gets,settle_ms"
    );
    for (transport, delay, limited) in [
        ("memory", 0, false),
        ("20ms", 20, false),
        ("20ms_cap32", 20, true),
    ] {
        if std::env::var("MORAINE_PROBE_BENCH_TRANSPORT")
            .is_ok_and(|selected| selected != transport)
        {
            continue;
        }
        objects.configure(delay, limited);
        for statements in [1, 8] {
            for shape in ["clustered", "gapped", "scattered"] {
                for sample in 0..samples {
                    for index in 0..POLICIES.len() {
                        let policy =
                            POLICIES[(index + usize::try_from(sample).unwrap()) % POLICIES.len()];
                        if std::env::var("MORAINE_PROBE_BENCH_POLICIES").is_ok_and(|selected| {
                            !selected.split(',').any(|name| name == policy.name)
                        }) {
                            continue;
                        }
                        cache.resize(0);
                        assert_eq!(cache.usage(), 0);
                        cache.resize(CACHE_BYTES);
                        objects.coalescing.store(
                            policy.name == "coalesced",
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        for (phase, wave) in [("cold", 0), ("fresh", 1), ("warm", 1)] {
                            let queries: Vec<_> = (0..statements)
                                .map(|statement| numbers(shape, statement, wave, sample))
                                .collect();
                            objects.reset();
                            let cpu = ProcessTime::now();
                            let mut observation = measure(&db, &queries, policy).await;
                            let settle = Instant::now();
                            objects.settle().await;
                            let settle = settle.elapsed().as_secs_f64() * 1000.0;
                            observation.cpu = cpu.elapsed().as_secs_f64() * 1000.0;
                            let (gets, bytes, peak) = objects.counters();
                            println!(
                                "{transport},{statements},{shape},{},{sample},{phase},{:.3},{:.3},{:.3},{:.3},{gets},{bytes},{peak},{settle:.3}",
                                policy.name,
                                observation.wall,
                                observation.cpu,
                                observation.median,
                                observation.maximum
                            );
                            if phase == "warm" {
                                assert_eq!(gets, 0, "warm data should fit in cache");
                            }
                        }
                    }
                }
            }
        }
    }
    objects.configure(0, false);
    db.close().await.unwrap();
}

#[test]
fn fresh_queries_are_disjoint_and_scattered_queries_cover_every_shard() {
    for shape in ["clustered", "gapped", "scattered"] {
        for sample in 0..3 {
            let mut seen = std::collections::BTreeSet::new();
            for wave in 0..2 {
                for statement in 0..8 {
                    let numbers = numbers(shape, statement, wave, sample);
                    for &number in &numbers {
                        assert!(number < 65_536);
                        assert!(seen.insert(number), "revisited {number}");
                    }
                    if shape == "scattered" {
                        for shard in 0..8 {
                            assert_eq!(
                                numbers
                                    .iter()
                                    .filter(|&&number| number % 8 == shard)
                                    .count(),
                                24
                            );
                        }
                    }
                }
            }
        }
    }
}
