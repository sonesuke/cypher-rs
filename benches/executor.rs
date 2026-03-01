use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cypher_rs::{CypherEngine, GraphConfig};
use serde_json::json;

fn create_test_data(node_count: usize) -> serde_json::Value {
    let mut users = Vec::new();
    for i in 0..node_count {
        users.push(json!({
            "id": i.to_string(),
            "role": if i % 3 == 0 { "admin" } else { "user" },
            "age": 20 + (i % 50),
            "name": format!("User{}", i),
            "friends": (0..(i % 5)).map(|j| ((i + j + 1) % node_count).to_string()).collect::<Vec<_>>()
        }));
    }

    json!({ "users": users })
}

fn bench_execute_simple_match(c: &mut Criterion) {
    let data = create_test_data(100);
    let engine = CypherEngine::from_json_auto(&data).unwrap();

    c.bench_function("execute_simple_match", |b| {
        b.iter(|| engine.execute(black_box("MATCH (n) RETURN n.id")));
    });
}

fn bench_execute_with_label(c: &mut Criterion) {
    let data = create_test_data(100);
    let engine = CypherEngine::from_json_auto(&data).unwrap();

    c.bench_function("execute_with_label", |b| {
        b.iter(|| engine.execute(black_box("MATCH (n:admin) RETURN n.id")));
    });
}

fn bench_execute_with_where(c: &mut Criterion) {
    let data = create_test_data(100);
    let engine = CypherEngine::from_json_auto(&data).unwrap();

    c.bench_function("execute_with_where", |b| {
        b.iter(|| engine.execute(black_box("MATCH (n) WHERE n.age > 25 RETURN n.id")));
    });
}

fn bench_execute_count(c: &mut Criterion) {
    let data = create_test_data(100);
    let engine = CypherEngine::from_json_auto(&data).unwrap();

    c.bench_function("execute_count", |b| {
        b.iter(|| engine.execute(black_box("MATCH (n) RETURN COUNT(n)")));
    });
}

fn bench_execute_sum(c: &mut Criterion) {
    let data = create_test_data(100);
    let engine = CypherEngine::from_json_auto(&data).unwrap();

    c.bench_function("execute_sum", |b| {
        b.iter(|| engine.execute(black_box("MATCH (n) RETURN SUM(n.age)")));
    });
}

fn bench_execute_with_relationship(c: &mut Criterion) {
    let data = create_test_data(100);
    let engine = CypherEngine::from_json_auto(&data).unwrap();

    c.bench_function("execute_with_relationship", |b| {
        b.iter(|| engine.execute(black_box("MATCH (a)-[:friends]->(b) RETURN a.id, b.id")));
    });
}

fn bench_execute_variable_graph_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_size");

    for size in [10, 50, 100, 500].iter() {
        let data = create_test_data(*size);
        let engine = CypherEngine::from_json_auto(&data).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| engine.execute(black_box("MATCH (n) RETURN n.id")));
        });
    }

    group.finish();
}

fn bench_execute_count_variable_graph_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("count_graph_size");

    for size in [10, 50, 100, 500].iter() {
        let data = create_test_data(*size);
        let engine = CypherEngine::from_json_auto(&data).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| engine.execute(black_box("MATCH (n) RETURN COUNT(n)")));
        });
    }

    group.finish();
}

fn bench_execute_sum_variable_graph_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_graph_size");

    for size in [10, 50, 100, 500].iter() {
        let data = create_test_data(*size);
        let engine = CypherEngine::from_json_auto(&data).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| engine.execute(black_box("MATCH (n) RETURN SUM(n.age)")));
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_execute_simple_match,
    bench_execute_with_label,
    bench_execute_with_where,
    bench_execute_count,
    bench_execute_sum,
    bench_execute_with_relationship,
    bench_execute_variable_graph_size,
    bench_execute_count_variable_graph_size,
    bench_execute_sum_variable_graph_size,
);

criterion_main!(benches);
