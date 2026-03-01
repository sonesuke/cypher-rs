use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use cypher_rs::parser;

fn bench_parse_simple(c: &mut Criterion) {
    let query = "MATCH (n) RETURN n.id";
    c.bench_function("parse_simple", |b| {
        b.iter(|| parser::parse_query(black_box(query)));
    });
}

fn bench_parse_with_label(c: &mut Criterion) {
    let query = "MATCH (n:admin) RETURN n.id";
    c.bench_function("parse_with_label", |b| {
        b.iter(|| parser::parse_query(black_box(query)));
    });
}

fn bench_parse_with_where(c: &mut Criterion) {
    let query = "MATCH (n) WHERE n.age > 25 RETURN n.id";
    c.bench_function("parse_with_where", |b| {
        b.iter(|| parser::parse_query(black_box(query)));
    });
}

fn bench_parse_with_relationship(c: &mut Criterion) {
    let query = "MATCH (a)-[:knows]->(b) RETURN a.id, b.id";
    c.bench_function("parse_with_relationship", |b| {
        b.iter(|| parser::parse_query(black_box(query)));
    });
}

fn bench_parse_with_aggregate_count(c: &mut Criterion) {
    let query = "MATCH (n) RETURN COUNT(n)";
    c.bench_function("parse_aggregate_count", |b| {
        b.iter(|| parser::parse_query(black_box(query)));
    });
}

fn bench_parse_with_aggregate_sum(c: &mut Criterion) {
    let query = "MATCH (n) RETURN SUM(n.age)";
    c.bench_function("parse_aggregate_sum", |b| {
        b.iter(|| parser::parse_query(black_box(query)));
    });
}

fn bench_parse_complex(c: &mut Criterion) {
    let query = "MATCH (u:admin)-[:knows]->(f) WHERE f.age > 25 AND f.role = 'user' RETURN COUNT(f)";
    c.bench_function("parse_complex", |b| {
        b.iter(|| parser::parse_query(black_box(query)));
    });
}

fn bench_parse_variable_query_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_query_length");

    let queries = vec![
        ("short", "MATCH (n) RETURN n"),
        ("medium", "MATCH (n:admin) WHERE n.age > 25 RETURN n.id, n.name"),
        (
            "long",
            "MATCH (a:admin)-[:knows]->(b:user) WHERE a.age > 25 AND b.active = true RETURN a.id, b.id, a.role, b.role",
        ),
    ];

    for (name, query) in queries {
        group.bench_with_input(BenchmarkId::from_parameter(name), query, |b, q| {
            b.iter(|| parser::parse_query(black_box(q)));
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_parse_simple,
    bench_parse_with_label,
    bench_parse_with_where,
    bench_parse_with_relationship,
    bench_parse_with_aggregate_count,
    bench_parse_with_aggregate_sum,
    bench_parse_complex,
    bench_parse_variable_query_size,
);

criterion_main!(benches);
