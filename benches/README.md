# Benchmarks

Performance benchmarks for the cypher-rs library using [Criterion.rs](https://github.com/bheisler/criterion.rs).

## Running Benchmarks

Run all benchmarks:
```bash
cargo bench
```

Run a specific benchmark:
```bash
cargo bench --bench parser
cargo bench --bench executor
cargo bench --bench schema
```

## Benchmark Files

### `parser.rs`
Benchmarks for the Cypher query parser.

- `parse_simple` - Parse simple MATCH query
- `parse_with_label` - Parse query with label filter
- `parse_with_where` - Parse query with WHERE clause
- `parse_with_relationship` - Parse query with relationship pattern
- `parse_aggregate_count` - Parse COUNT aggregate
- `parse_aggregate_sum` - Parse SUM aggregate
- `parse_complex` - Parse complex multi-clause query
- `parse_query_length/*` - Parse queries of varying length

### `executor.rs`
Benchmarks for query execution.

- `execute_simple_match` - Execute simple MATCH
- `execute_with_label` - Execute with label filter
- `execute_with_where` - Execute with WHERE clause
- `execute_count` - Execute COUNT aggregate
- `execute_sum` - Execute SUM aggregate
- `execute_with_relationship` - Execute relationship traversal
- `graph_size/*` - Execute on varying graph sizes (10, 50, 100, 500 nodes)
- `count_graph_size/*` - COUNT on varying graph sizes
- `sum_graph_size/*` - SUM on varying graph sizes

### `schema.rs`
Benchmarks for schema detection and analysis.

- `analyze_simple` - Analyze simple JSON schema
- `analyze_medium` - Analyze medium complexity schema
- `analyze_large` - Analyze large schema
- `analyze_nested` - Analyze nested JSON
- `analyze_multiple_arrays` - Analyze JSON with multiple arrays
- `field_count/*` - Analyze schemas with varying field counts
- `nesting_depth/*` - Analyze schemas with varying nesting depth
- `array_count/*` - Analyze JSON with varying array counts
- `infer_graph_config` - Auto-infer GraphConfig
- `to_neo4j_schema` - Generate Neo4j-style schema string

## Results

Benchmark results are saved to `target/criterion/` directory.
You can view HTML reports by opening `target/criterion/report/index.html` in a browser.

## Sample Results (Reference)

Parser performance (microseconds):
- Simple query: ~2-4 µs
- Medium query: ~9 µs
- Long query: ~20 µs

Executor performance (microseconds, 100 nodes):
- Simple MATCH: ~100 µs
- COUNT: ~30 µs
- SUM: ~35 µs

Schema performance (microseconds):
- Simple analysis: ~2-3 µs
- Neo4j schema generation: ~1 µs

Note: Actual results vary by hardware and configuration.
