# cypher-rs

A general-purpose Cypher query execution engine for Rust.

Execute Cypher queries against in-memory JSON data with support for MATCH, WHERE, RETURN clauses and aggregate functions (COUNT, SUM).

## Features

- 🔍 **Cypher Query Support**: MATCH, WHERE, RETURN clauses
- 📊 **Aggregate Functions**: COUNT, SUM
- 🔗 **Relationship Traversal**: Query graph relationships
- 🎯 **Automatic Schema Detection**: Auto-detects graph structure from JSON
- 📋 **Schema Visualization**: Neo4j-style schema output
- 💾 **Pluggable Storage**: JSON, in-memory, or custom storage backends

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
let result = engine.execute("MATCH (u:admin) RETURN u.name")?;
```

## Usage

### 1. Load JSON Data

The simplest way to get started is to use `from_json_auto`, which automatically analyzes your JSON structure:

```rust
use cypher_rs::CypherEngine;
use serde_json::json;

let data = json!({
    "users": [
        { "id": "1", "role": "admin", "name": "Alice", "age": 30 },
        { "id": "2", "role": "user", "name": "Bob", "age": 25 }
    ]
});

let engine = CypherEngine::from_json_auto(&data)?;
```

### 2. View the Schema

Check what the engine detected from your data:

```rust
let schema = engine.get_schema();
println!("{}", schema);
```

Output:
```
Graph Schema
============

Node Types:
  (:admin 1 nodes)
  (:user 1 nodes)

Properties:
  :admin {id: STRING, role: STRING, name: STRING, age: NUMBER}
  :user {id: STRING, role: STRING, name: STRING, age: NUMBER}
```

### 3. Execute Cypher Queries

#### Count Nodes

```rust
let result = engine.execute("MATCH (u) RETURN COUNT(u)")?;
let count = result.get_single_value()?.as_i64()?;
println!("Total users: {}", count); // Output: Total users: 2
```

#### Filter by Label

```rust
let result = engine.execute("MATCH (u:admin) RETURN u.name")?;
```

#### Aggregate Properties

```rust
// Sum ages
let result = engine.execute("MATCH (u) RETURN SUM(u.age)")?;
let total = result.get_single_value()?.as_i64()?;
println!("Total age: {}", total);
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

### Manual Schema Configuration

If auto-detection doesn't work for your data, you can manually specify the schema:

```rust
use cypher_rs::{CypherEngine, GraphConfig};

let config = GraphConfig {
    node_path: "data.users".to_string(),      // JSON path to node array
    id_field: "id".to_string(),                 // Field containing unique ID
    label_field: Some("role".to_string()),    // Field containing node label
    relation_fields: vec!["friends".to_string()], // Fields containing relationship arrays
};

let engine = CypherEngine::from_json(&data, config)?;
```

### Schema Analysis

Get detailed schema information before creating the engine:

```rust
use cypher_rs::CypherEngine;

let detection = CypherEngine::analyze_schema(&data)?;

// Neo4j-style schema
println!("{}", detection.to_neo4j_schema());

// Compact pattern representation
println!("{}", detection.to_pattern());

// Or convert to GraphConfig
let config = detection.to_graph_config().unwrap();
```

### Query Results

#### Access Single Values (Aggregates)

```rust
let result = engine.execute("MATCH (u) RETURN COUNT(u)")?;
let value = result.get_single_value()?;
println!("Count: {}", value.as_i64().unwrap());
```

#### Access Multiple Rows

```rust
let result = engine.execute("MATCH (u) RETURN u.id, u.name")?;
for row in &result.rows {
    println!("ID: {}, Name: {}", row["u.id"], row["u.name"]);
}
```

#### Get Results as JSON

```rust
let result = engine.execute("MATCH (u) RETURN u.id, u.name")?;
let json_array = result.as_json_array();
```

## Cypher Support

### Supported Clauses

- **MATCH**: Pattern matching on nodes and relationships
- **WHERE**: Filtering with comparison operators
- **RETURN**: Projection and aliasing

### Comparison Operators

- `=` - Equal
- `<>` - Not equal
- `<` - Less than
- `>` - Greater than
- `<=` - Less than or equal
- `>=` - Greater than or equal
- `CONTAINS` - String contains

### Logical Operators

- `AND` - Logical AND
- `OR` - Logical OR

### Aggregate Functions

- `COUNT(variable)` - Count matched entities
- `SUM(variable.property)` - Sum numeric property values

### Relationship Patterns

```cypher
// Outgoing relationship
MATCH (u)-[:rel_type]->(v)

// Incoming relationship
MATCH (u)<-[:rel_type]-(v)

// Undirected relationship
MATCH (u)-[:rel_type]-(v)
```

## Examples

### Social Network

```rust
let data = json!({
    "users": [
        { "id": "1", "name": "Alice", "type": "Person", "friends": ["2", "3"] },
        { "id": "2", "name": "Bob", "type": "Person", "friends": ["1"] },
        { "id": "3", "name": "Charlie", "type": "Person", "friends": ["1", "2"] }
    ]
});

let engine = CypherEngine::from_json_auto(&data)?;

// Find friends of friends
let result = engine.execute("MATCH (u)-[:friends]->(v)-[:friends]->(w) RETURN u.name, w.name")?;
```

### E-commerce Orders

```rust
let data = json!({
    "orders": [
        { "id": "o1", "customer": "c1", "amount": 100 },
        { "id": "o2", "customer": "c1", "amount": 200 },
        { "id": "o3", "customer": "c2", "amount": 150 }
    ]
});

let config = GraphConfig {
    node_path: "orders".to_string(),
    id_field: "id".to_string(),
    label_field: None,
    relation_fields: vec![],
};

let engine = CypherEngine::from_json(&data, config)?;

// Calculate total per customer
let result = engine.execute("MATCH (o) RETURN o.customer, SUM(o.amount)")?;
```

## Project Structure

```
src/
├── lib.rs              # Public API
├── config.rs           # GraphConfig
├── graph.rs            # Graph, Node, Edge
├── parser/             # Cypher parser
│   ├── mod.rs
│   ├── ast.rs
│   └── cypher.pest
├── engine/             # Query execution engine
│   ├── executor.rs    # Core query executor
│   ├── functions/      # Function evaluations
│   │   └── aggregate.rs # COUNT, SUM
│   └── storage/        # Storage backends
│       ├── storage_trait.rs
│       ├── json.rs
│       └── memory.rs
└── schema.rs           # Schema detection
```

## License

[MIT](LICENSE)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
