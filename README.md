# cypher-rs

A general-purpose Cypher query execution engine for Rust.

Execute Cypher queries against in-memory JSON data with support for MATCH, WHERE, RETURN clauses and aggregate functions (COUNT, SUM).

## Features

- Cypher Query Support: MATCH, WHERE, RETURN clauses
- Aggregate Functions: COUNT, SUM
- Relationship Traversal: Query graph relationships
- Automatic Schema Detection: Auto-detects graph structure from JSON
- Universal JSON Support: Any JSON object is converted to a graph automatically
- Schema Visualization: Neo4j-style schema output
- Pluggable Storage: JSON, in-memory, or custom storage backends

## Quick Start

```rust
use cypher_rs::CypherEngine;
use serde_json::json;

let data = json!({
    "users": [
        { "id": "1", "role": "admin", "name": "Alice", "age": 30, "friends": ["2"] },
        { "id": "2", "role": "user", "name": "Bob", "age": 25, "friends": ["1"] }
    ]
});

// 1. Auto-detect schema and create engine
let engine = CypherEngine::from_json_auto(&data)?;

// 2. View the detected schema
println!("{}", engine.get_schema());

// 3. Execute Cypher queries
let result = engine.execute("MATCH (u:users) RETURN u.name")?;
```

## API Overview

| Method | Description |
|---|---|
| `from_json_auto(json)` | Auto-detect schema from any JSON object |
| `from_json_with_label(json, label)` | Specify a root node label |

## Usage

### 1. Load JSON Data

`from_json_auto` handles any JSON object — arrays become child nodes, objects become child nodes, scalars become properties:

```rust
// Simple array of objects
let data = json!({
    "users": [
        { "id": "1", "name": "Alice", "age": 30 },
        { "id": "2", "name": "Bob", "age": 25 }
    ]
});

let engine = CypherEngine::from_json_auto(&data)?;

// Root-object with nested arrays (e.g., patent data)
let patent = json!({
    "id": "US123",
    "title": "Method for Processing Data",
    "claims": [
        { "id": "c1", "number": "1", "text": "A method comprising..." },
        { "id": "c2", "number": "2", "text": "The method of claim 1..." }
    ]
});

let engine = CypherEngine::from_json_auto(&patent)?;
```

### 2. Specify Root Label

Use `from_json_with_label` to give the root node a meaningful label:

```rust
let engine = CypherEngine::from_json_with_label(&patent, "Patent")?;

// Query by label
let result = engine.execute("MATCH (p:Patent) RETURN p.title")?;

// Traverse to child nodes
let result = engine.execute("MATCH (p:Patent)-[:claims]->(c) RETURN c.number")?;
```

### 3. View the Schema

```rust
let schema = engine.get_schema();
println!("{}", schema);
```

### 4. Execute Cypher Queries

#### Count Nodes

```rust
let result = engine.execute("MATCH (u) RETURN COUNT(u)")?;
let count = result.get_single_value()?.as_i64()?;
```

#### Filter by Label

```rust
let result = engine.execute("MATCH (u:users) RETURN u.name")?;
```

#### Aggregate Properties

```rust
let result = engine.execute("MATCH (u) RETURN SUM(u.age)")?;
```

#### Query Relationships

```rust
let result = engine.execute("MATCH (u)-[:friends]->(v) RETURN u.name, v.name")?;
for row in &result.rows {
    println!("{} -> {}", row["u.name"], row["v.name"]);
}
```

#### WHERE Clauses

```rust
// Comparison operators: =, <>, <, >, <=, >=
let result = engine.execute("MATCH (u) WHERE u.age > \"25\" RETURN u.name")?;

// Contains operator
let result = engine.execute("MATCH (u) WHERE u.name CONTAINS \"Alice\" RETURN u.name")?;

// Logical operators: AND, OR
let result = engine.execute("MATCH (u) WHERE u.role = \"admin\" AND u.age > \"25\" RETURN u.name")?;
```

## Advanced Usage

### Schema Analysis

Get detailed schema information before creating the engine:

```rust
let detection = CypherEngine::analyze_schema(&data)?;

if detection.is_root_object() {
    let root = detection.root_object.unwrap();
    println!("Root label: {}", root.label);
    for arr in &root.nested_arrays {
        println!("  Nested array: {} ({} elements)", arr.path, arr.element_count);
    }
}
```

### Query Results

```rust
// Single value (aggregates)
let result = engine.execute("MATCH (u) RETURN COUNT(u)")?;
let value = result.get_single_value()?;

// Multiple rows
let result = engine.execute("MATCH (u) RETURN u.id, u.name")?;
for row in &result.rows {
    println!("ID: {}, Name: {}", row["u.id"], row["u.name"]);
}

// As JSON
let json_array = result.as_json_array();
```

## Cypher Support

### Clauses

- **MATCH**: Pattern matching on nodes and relationships
- **WHERE**: Filtering with comparison operators
- **RETURN**: Projection and aliasing

### Comparison Operators

`=`, `<>`, `<`, `>`, `<=`, `>=`, `CONTAINS`

### Logical Operators

`AND`, `OR`

### Aggregate Functions

- `COUNT(variable)` - Count matched entities
- `SUM(variable.property)` - Sum numeric property values

### Relationship Patterns

```cypher
// Outgoing
MATCH (u)-[:rel_type]->(v)

// Incoming
MATCH (u)<-[:rel_type]-(v)

// Undirected
MATCH (u)-[:rel_type]-(v)
```

## Project Structure

```
src/
├── lib.rs              # Public API
├── graph.rs            # Graph, Node, Edge
├── parser/             # Cypher parser
├── engine/             # Query execution engine
│   ├── executor.rs
│   ├── functions/
│   └── storage/
└── schema.rs           # Schema detection
```

## License

[MIT](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
