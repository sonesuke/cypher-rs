use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use cypher_rs::schema::SchemaAnalyzer;
use serde_json::json;

fn create_simple_schema_data() -> serde_json::Value {
    json!({
        "users": [
            { "id": "1", "role": "admin", "age": 30, "friends": ["2"] },
            { "id": "2", "role": "user", "age": 25, "friends": ["1"] }
        ]
    })
}

fn create_medium_schema_data(field_count: usize) -> serde_json::Value {
    let mut users = Vec::new();
    for i in 0..10 {
        let mut obj = serde_json::Map::new();
        obj.insert("id".to_string(), json!(i.to_string()));
        obj.insert("role".to_string(), json!(if i % 2 == 0 { "admin" } else { "user" }));
        obj.insert("age".to_string(), json!(20 + i));
        obj.insert("name".to_string(), json!(format!("User{}", i)));
        obj.insert("email".to_string(), json!(format!("user{}@example.com", i)));

        for j in 0..field_count {
            obj.insert(format!("field{}", j), json!(format!("value{}", j)));
        }

        obj.insert("friends".to_string(), json!([((i + 1) % 10).to_string()]));
        users.push(json!(obj));
    }

    json!({ "users": users })
}

fn create_nested_schema_data(depth: usize) -> serde_json::Value {
    let mut current = json!([{"id": "1", "name": "User1"}]);
    for i in (0..depth).rev() {
        let mut obj = serde_json::Map::new();
        obj.insert(format!("level{}", i), current);
        current = json!(obj);
    }
    current
}

fn create_array_schema_data(array_count: usize) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    for i in 0..array_count {
        result.insert(
            format!("array{}", i),
            json!([{"id": format!("{}-0", i), "value": i}]),
        );
    }
    json!(result)
}

fn bench_analyze_simple(c: &mut Criterion) {
    let data = create_simple_schema_data();
    c.bench_function("analyze_simple", |b| {
        b.iter(|| SchemaAnalyzer::analyze(black_box(&data)));
    });
}

fn bench_analyze_medium(c: &mut Criterion) {
    let data = create_medium_schema_data(5);
    c.bench_function("analyze_medium", |b| {
        b.iter(|| SchemaAnalyzer::analyze(black_box(&data)));
    });
}

fn bench_analyze_large(c: &mut Criterion) {
    let data = create_medium_schema_data(20);
    c.bench_function("analyze_large", |b| {
        b.iter(|| SchemaAnalyzer::analyze(black_box(&data)));
    });
}

fn bench_analyze_nested(c: &mut Criterion) {
    let data = create_nested_schema_data(5);
    c.bench_function("analyze_nested", |b| {
        b.iter(|| SchemaAnalyzer::analyze(black_box(&data)));
    });
}

fn bench_analyze_multiple_arrays(c: &mut Criterion) {
    let data = create_array_schema_data(10);
    c.bench_function("analyze_multiple_arrays", |b| {
        b.iter(|| SchemaAnalyzer::analyze(black_box(&data)));
    });
}

fn bench_analyze_variable_field_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_count");

    for field_count in [0, 5, 10, 20, 50].iter() {
        let data = create_medium_schema_data(*field_count);

        group.bench_with_input(
            BenchmarkId::from_parameter(field_count),
            field_count,
            |b, _| {
                b.iter(|| SchemaAnalyzer::analyze(black_box(&data)));
            },
        );
    }

    group.finish();
}

fn bench_analyze_variable_nesting_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("nesting_depth");

    for depth in [1, 3, 5, 10].iter() {
        let data = create_nested_schema_data(*depth);

        group.bench_with_input(BenchmarkId::from_parameter(depth), depth, |b, _| {
            b.iter(|| SchemaAnalyzer::analyze(black_box(&data)));
        });
    }

    group.finish();
}

fn bench_analyze_variable_array_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_count");

    for array_count in [1, 5, 10, 20].iter() {
        let data = create_array_schema_data(*array_count);

        group.bench_with_input(
            BenchmarkId::from_parameter(array_count),
            array_count,
            |b, _| {
                b.iter(|| SchemaAnalyzer::analyze(black_box(&data)));
            },
        );
    }

    group.finish();
}

fn bench_infer_graph_config(c: &mut Criterion) {
    let data = create_simple_schema_data();
    c.bench_function("infer_graph_config", |b| {
        b.iter(|| SchemaAnalyzer::infer_graph_config(black_box(&data)));
    });
}

fn bench_to_neo4j_schema(c: &mut Criterion) {
    let data = create_simple_schema_data();
    let schema = SchemaAnalyzer::analyze(&data).unwrap();

    c.bench_function("to_neo4j_schema", |b| {
        b.iter(|| black_box(&schema).to_neo4j_schema());
    });
}

criterion_group!(
    benches,
    bench_analyze_simple,
    bench_analyze_medium,
    bench_analyze_large,
    bench_analyze_nested,
    bench_analyze_multiple_arrays,
    bench_analyze_variable_field_count,
    bench_analyze_variable_nesting_depth,
    bench_analyze_variable_array_count,
    bench_infer_graph_config,
    bench_to_neo4j_schema,
);

criterion_main!(benches);
