//! # cypher-rs
//!
//! A general-purpose Cypher query execution engine for Rust.
//!
//! This crate allows you to execute Cypher queries against in-memory JSON data,
//! with support for MATCH, WHERE, RETURN clauses and aggregate functions (COUNT, SUM).
//!
//! ## Example
//!
//! ```rust
//! use cypher_rs::CypherEngine;
//! use serde_json::json;
//!
//! let data = json!({
//!     "users": [
//!         { "id": "1", "role": "admin", "age": 30, "friends": ["2"] },
//!         { "id": "2", "role": "user", "age": 25, "friends": ["1"] }
//!     ]
//! });
//!
//! let engine = CypherEngine::from_json_auto(&data).unwrap();
//!
//! // Count all users
//! let result = engine.execute("MATCH (u:users) RETURN COUNT(u)").unwrap();
//! assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));
//!
//! // Sum ages
//! let result = engine.execute("MATCH (u:users) RETURN SUM(u.age)").unwrap();
//! assert_eq!(result.get_single_value().unwrap().as_i64(), Some(55));
//! ```

pub mod engine;
pub mod graph;
pub mod parser;
pub mod schema;

use serde_json::Value;
use std::fmt;

pub use engine::storage::SyncStorage;
pub use engine::{EngineError, QueryResult, Result};
pub use engine::{JsonStorage, MemoryStorage, MemoryStorageBuilder};
pub use graph::{Edge, Graph, Node};
pub use schema::{RootObjectSchema, SchemaAnalyzer, SchemaDetection, SchemaError};

/// Error type for CypherEngine operations.
#[derive(Debug)]
pub enum CypherError {
    /// Error during graph building
    GraphBuild(String),
    /// Error during query execution
    QueryExecution(EngineError),
}

impl fmt::Display for CypherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CypherError::GraphBuild(msg) => write!(f, "Graph build error: {}", msg),
            CypherError::QueryExecution(e) => write!(f, "Query execution error: {}", e),
        }
    }
}

impl std::error::Error for CypherError {}

impl From<EngineError> for CypherError {
    fn from(err: EngineError) -> Self {
        CypherError::QueryExecution(err)
    }
}

/// The main Cypher query execution engine.
///
/// # Example
///
/// ```rust
/// use cypher_rs::CypherEngine;
/// use serde_json::json;
///
/// let data = json!({
///     "users": [
///         { "id": "1", "role": "admin", "age": 30 },
///         { "id": "2", "role": "user", "age": 25 }
///     ]
/// });
///
/// let engine = CypherEngine::from_json_auto(&data).unwrap();
/// let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
/// ```
pub struct CypherEngine {
    graph: graph::Graph,
}

impl CypherEngine {
    /// Create a new CypherEngine from JSON data with automatic schema detection.
    ///
    /// This method automatically analyzes the JSON structure and infers the
    /// appropriate graph. Works with any JSON object structure.
    ///
    /// # Example
    ///
    /// ```rust
    /// use cypher_rs::CypherEngine;
    /// use serde_json::json;
    ///
    /// let data = json!({
    ///     "users": [
    ///         { "id": "1", "role": "admin", "age": 30, "friends": ["2"] },
    ///         { "id": "2", "role": "user", "age": 25, "friends": ["1"] }
    ///     ]
    /// });
    ///
    /// let engine = CypherEngine::from_json_auto(&data).unwrap();
    /// let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
    /// ```
    pub fn from_json_auto(json: &Value) -> std::result::Result<Self, CypherError> {
        use engine::storage::json::build_graph_from_root_object;
        let detection = schema::SchemaAnalyzer::analyze(json)
            .map_err(|e: schema::SchemaError| CypherError::GraphBuild(e.to_string()))?;

        let label = detection
            .root_object
            .as_ref()
            .map(|r| r.label.as_str())
            .unwrap_or("Root");
        let graph = build_graph_from_root_object(json, label)
            .map_err(|e| CypherError::GraphBuild(e.to_string()))?;
        Ok(Self { graph })
    }

    /// Create a new CypherEngine from JSON data with a custom root label.
    ///
    /// # Example
    ///
    /// ```rust
    /// use cypher_rs::CypherEngine;
    /// use serde_json::json;
    ///
    /// let data = json!({
    ///     "id": "US1234567",
    ///     "title": "Method for Processing Data",
    ///     "claims": [
    ///         { "id": "claim-1", "number": "1", "text": "A method comprising..." }
    ///     ]
    /// });
    ///
    /// let engine = CypherEngine::from_json_with_label(&data, "Patent").unwrap();
    /// let result = engine.execute("MATCH (p:Patent) RETURN p.title").unwrap();
    /// ```
    pub fn from_json_with_label(
        json: &Value,
        label: &str,
    ) -> std::result::Result<Self, CypherError> {
        use engine::storage::json::build_graph_from_root_object;
        let graph = build_graph_from_root_object(json, label)
            .map_err(|e| CypherError::GraphBuild(e.to_string()))?;
        Ok(Self { graph })
    }

    /// Analyze JSON data and return schema detection information.
    ///
    /// # Example
    ///
    /// ```rust
    /// use cypher_rs::CypherEngine;
    /// use serde_json::json;
    ///
    /// let data = json!({
    ///     "users": [
    ///         { "id": "1", "role": "admin", "friends": ["2"] }
    ///     ]
    /// });
    ///
    /// let schema = CypherEngine::analyze_schema(&data).unwrap();
    /// assert!(schema.is_root_object());
    /// let root = schema.root_object.unwrap();
    /// println!("Nested array: {}", root.nested_arrays[0].path);
    /// ```
    pub fn analyze_schema(json: &Value) -> std::result::Result<SchemaDetection, CypherError> {
        schema::SchemaAnalyzer::analyze(json)
            .map_err(|e: schema::SchemaError| CypherError::GraphBuild(e.to_string()))
    }

    /// Execute a Cypher query against the graph.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use cypher_rs::CypherEngine;
    /// # use serde_json::json;
    /// # let data = json!({"users": [{"id": "1", "role": "admin", "age": 30}]});
    /// # let engine = CypherEngine::from_json_auto(&data).unwrap();
    /// let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
    /// let result = engine.execute("MATCH (u) RETURN SUM(u.age)").unwrap();
    /// let result = engine.execute("MATCH (u) RETURN u.id, u.role").unwrap();
    /// ```
    pub fn execute(&self, query: &str) -> Result<QueryResult> {
        engine::execute(query, &self.graph)
    }

    /// Get a reference to the underlying graph.
    pub fn graph(&self) -> &graph::Graph {
        &self.graph
    }

    /// Get the Neo4j-style schema representation of this engine's graph.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use cypher_rs::CypherEngine;
    /// # use serde_json::json;
    /// # let data = json!({"users": [{"id": "1", "role": "admin"}]});
    /// # let engine = CypherEngine::from_json_auto(&data).unwrap();
    /// let schema = engine.get_schema();
    /// println!("{}", schema);
    /// ```
    pub fn get_schema(&self) -> String {
        let mut output = String::new();

        output.push_str("Graph Schema\n");
        output.push_str("============\n\n");

        if self.graph.nodes.is_empty() {
            output.push_str("No nodes in graph\n");
            return output;
        }

        // Group nodes by label
        let mut labels_by_label: std::collections::HashMap<String, Vec<&graph::Node>> =
            std::collections::HashMap::new();
        for node in &self.graph.nodes {
            let label = node.label.as_ref().unwrap().clone();
            labels_by_label.entry(label).or_default().push(node);
        }

        output.push_str("Node Types:\n");
        let mut label_names: Vec<String> = labels_by_label.keys().cloned().collect();
        label_names.sort();
        for label in &label_names {
            let count = labels_by_label.get(label).map(|v| v.len()).unwrap_or(0);
            output.push_str(&format!("  (:{} {} nodes)\n", label, count));
        }
        output.push('\n');

        output.push_str("Properties:\n");
        for label in &label_names {
            if let Some(nodes) = labels_by_label.get(label)
                && let Some(first_node) = nodes.first()
            {
                let mut properties: Vec<String> = Vec::new();
                if let Value::Object(obj) = &first_node.data {
                    for (key, value) in obj {
                        let type_str = match value {
                            Value::String(_) => "STRING",
                            Value::Number(_) => "NUMBER",
                            Value::Bool(_) => "BOOLEAN",
                            Value::Array(_) => "ARRAY",
                            Value::Object(_) => "OBJECT",
                            Value::Null => "NULL",
                        };
                        properties.push(format!("{}: {}", key, type_str));
                    }
                }
                if !properties.is_empty() {
                    output.push_str(&format!("  :{} {{{}}}\n", label, properties.join(", ")));
                }
            }
        }
        output.push('\n');

        if !self.graph.edges.is_empty() {
            output.push_str("Relationship Types:\n");

            let mut rel_types: std::collections::HashMap<
                String,
                (
                    std::collections::HashSet<String>,
                    std::collections::HashSet<String>,
                ),
            > = std::collections::HashMap::new();

            for edge in &self.graph.edges {
                let from_label = self.graph.nodes[edge.from].label.as_ref().unwrap().clone();
                let to_label = self.graph.nodes[edge.to].label.as_ref().unwrap().clone();

                rel_types
                    .entry(edge.rel_type.clone())
                    .or_insert_with(|| {
                        (
                            std::collections::HashSet::new(),
                            std::collections::HashSet::new(),
                        )
                    })
                    .0
                    .insert(from_label);
                rel_types
                    .entry(edge.rel_type.clone())
                    .or_insert_with(|| {
                        (
                            std::collections::HashSet::new(),
                            std::collections::HashSet::new(),
                        )
                    })
                    .1
                    .insert(to_label);
            }

            let mut sorted_rels: Vec<_> = rel_types.into_iter().collect();
            sorted_rels.sort_by(|a, b| a.0.cmp(&b.0));

            for (rel_type, (from_labels, to_labels)) in sorted_rels {
                let from: Vec<_> = from_labels.into_iter().collect();
                let to: Vec<_> = to_labels.into_iter().collect();
                for f in &from {
                    for t in &to {
                        output.push_str(&format!("  (:{})-[:{}]->(:{})\n", f, rel_type, t));
                    }
                }
            }
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_count() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "age": 30 },
                { "id": "2", "role": "user", "age": 25 },
                { "id": "3", "role": "admin", "age": 35 }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        let result = engine.execute("MATCH (u:users) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(3));
    }

    #[test]
    fn test_basic_sum() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "age": 30 },
                { "id": "2", "role": "user", "age": 25 },
                { "id": "3", "role": "admin", "age": 35 }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        let result = engine.execute("MATCH (u:users) RETURN SUM(u.age)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(90));
    }

    #[test]
    fn test_simple_return() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice", "role": "admin" },
                { "id": "2", "name": "Bob", "role": "user" }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        let result = engine.execute("MATCH (u:users) RETURN u.id").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0]["u.id"], 1);
        assert_eq!(result.rows[1]["u.id"], 2);

        let result = engine
            .execute("MATCH (u:users) WHERE u.name = \"Alice\" RETURN u.id")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["u.id"], 1);
    }

    #[test]
    fn test_relationships() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice", "friends": ["2", "3"] },
                { "id": "2", "name": "Bob", "friends": ["1"] },
                { "id": "3", "name": "Charlie", "friends": [] }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        let result = engine.execute("MATCH (u)-[:friends]->(v) WHERE u.name = \"Alice\" RETURN v.name").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_nested_json_path() {
        let data = json!({
            "data": {
                "users": [
                    { "id": "1", "role": "admin" },
                    { "id": "2", "role": "user" }
                ]
            }
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();
        let result = engine.execute("MATCH (u:users) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));
    }

    #[test]
    fn test_result_as_json() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice" },
                { "id": "2", "name": "Bob" }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        let result = engine.execute("MATCH (u:users) RETURN u.id, u.name").unwrap();
        let json_array = result.as_json_array();

        assert!(json_array.is_array());
        assert_eq!(json_array.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_where_operators() {
        let data = json!({
            "items": [
                { "id": "1", "value": 10 },
                { "id": "2", "value": 20 },
                { "id": "3", "value": 30 }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        let result = engine
            .execute("MATCH (i:items) WHERE i.value > \"15\" RETURN COUNT(i)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));

        let result = engine
            .execute("MATCH (i:items) WHERE i.value <= \"20\" RETURN COUNT(i)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));

        let result = engine
            .execute("MATCH (i:items) WHERE i.value <> \"20\" RETURN COUNT(i)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));
    }

    #[test]
    fn test_logical_operators() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "active": true },
                { "id": "2", "role": "user", "active": true },
                { "id": "3", "role": "user", "active": false }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        let result = engine
            .execute("MATCH (u:users) WHERE u.role = \"admin\" AND u.active = \"true\" RETURN COUNT(u)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(1));

        let result = engine
            .execute("MATCH (u:users) WHERE u.role = \"admin\" OR u.role = \"user\" RETURN COUNT(u)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(3));
    }

    #[test]
    fn test_contains_operator() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice Smith" },
                { "id": "2", "name": "Bob Jones" },
                { "id": "3", "name": "Charlie Smith" }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        let result = engine
            .execute("MATCH (u:users) WHERE u.name CONTAINS \"Smith\" RETURN COUNT(u)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));
    }

    #[test]
    fn test_from_json_auto() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "age": 30, "friends": ["2"] },
                { "id": "2", "role": "user", "age": 25, "friends": ["1"] }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        // Root + 2 user nodes = 3 total
        let result = engine.execute("MATCH (n) RETURN COUNT(n)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(3));

        // Label derived from array key
        let result = engine.execute("MATCH (u:users) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));
    }

    #[test]
    fn test_from_json_auto_with_relations() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice", "friends": ["2", "3"] },
                { "id": "2", "name": "Bob", "friends": ["1"] },
                { "id": "3", "name": "Charlie", "friends": ["2"] }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();

        // friends: 1->2, 1->3, 2->1, 3->2 = 4 + 3 root->user = 7
        let result = engine.execute("MATCH (u)-[]->(v) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(7));
    }

    #[test]
    fn test_analyze_schema() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "age": 30, "friends": ["2"] },
                { "id": "2", "role": "user", "age": 25, "friends": ["1"] }
            ]
        });

        let schema = CypherEngine::analyze_schema(&data).unwrap();
        assert!(schema.is_root_object());

        let root = schema.root_object.unwrap();
        assert_eq!(root.nested_arrays.len(), 1);
        assert_eq!(root.nested_arrays[0].path, "users");
    }

    #[test]
    fn test_analyze_schema_nested() {
        let data = json!({
            "data": {
                "network": {
                    "users": [
                        { "id": "1", "type": "Person", "connections": ["2"] }
                    ]
                }
            }
        });

        let schema = CypherEngine::analyze_schema(&data).unwrap();
        assert!(schema.is_root_object());
        let root = schema.root_object.unwrap();
        assert_eq!(root.nested_arrays[0].path, "data");
        assert_eq!(root.nested_arrays[0].element_count, 1);
    }

    #[test]
    fn test_get_schema() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "name": "Alice", "age": 30, "friends": ["2"] },
                { "id": "2", "role": "user", "name": "Bob", "age": 25, "friends": ["1", "3"] },
                { "id": "3", "role": "user", "name": "Charlie", "age": 28, "friends": ["2"] }
            ]
        });

        let engine = CypherEngine::from_json_auto(&data).unwrap();
        let schema = engine.get_schema();

        assert!(schema.contains("Graph Schema"));
        assert!(schema.contains("Node Types:"));
        assert!(schema.contains("(:users"));
        assert!(schema.contains("Relationship Types:"));
        assert!(schema.contains("friends"));
    }

    #[test]
    fn test_schema_to_neo4j() {
        let data = json!({
            "users": [
                { "id": "1", "role": "admin", "name": "Alice", "friends": ["2"] },
                { "id": "2", "role": "user", "name": "Bob", "friends": ["1"] }
            ]
        });

        let schema = CypherEngine::analyze_schema(&data).unwrap();
        let neo4j_schema = schema.to_neo4j_schema();

        assert!(neo4j_schema.contains("Graph Schema"));
        assert!(neo4j_schema.contains("Node Types:"));
        assert!(neo4j_schema.contains("Relationship Types:"));
        assert!(neo4j_schema.contains("friends"));
    }

    #[test]
    fn test_schema_to_pattern() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice", "friends": ["2"] }
            ]
        });

        let schema = CypherEngine::analyze_schema(&data).unwrap();
        let pattern = schema.to_pattern();

        assert!(pattern.contains("friends"));
        assert!(pattern.contains(":users"));
    }

    #[test]
    fn test_from_json_with_label() {
        let data = json!({
            "id": "doc-1",
            "title": "My Document",
            "sections": [
                { "id": "s1", "heading": "Introduction" },
                { "id": "s2", "heading": "Conclusion" }
            ]
        });

        let engine = CypherEngine::from_json_with_label(&data, "Root").unwrap();

        assert_eq!(engine.graph().nodes.len(), 3);

        let result = engine.execute("MATCH (r:Root) RETURN r.title").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("r.title"),
            Some(&serde_json::json!("My Document"))
        );
    }

    #[test]
    fn test_from_json_with_label_patent() {
        let data = json!({
            "id": "US1234567",
            "title": "Method for Processing Data",
            "claims": [
                { "id": "claim-1", "number": "1", "text": "A method comprising..." },
                { "id": "claim-2", "number": "2", "text": "The method of claim 1..." }
            ]
        });

        let engine = CypherEngine::from_json_with_label(&data, "Patent").unwrap();

        assert_eq!(engine.graph().nodes.len(), 3);

        let result = engine.execute("MATCH (p:Patent) RETURN p.title").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("p.title"),
            Some(&serde_json::json!("Method for Processing Data"))
        );
    }

    #[test]
    fn test_from_json_with_label_multiple_arrays() {
        let data = json!({
            "id": "patent-123",
            "title": "Test Patent",
            "claims": [
                { "id": "c1", "number": "1", "text": "Claim 1" },
                { "id": "c2", "number": "2", "text": "Claim 2" }
            ],
            "description_paragraphs": [
                { "id": "d1", "number": "1", "text": "Paragraph 1" }
            ]
        });

        let engine = CypherEngine::from_json_with_label(&data, "Patent").unwrap();

        assert_eq!(engine.graph().nodes.len(), 4);
        assert_eq!(engine.graph().edges.len(), 3);

        let result = engine
            .execute("MATCH (p:Patent)-[:claims]->(c) RETURN COUNT(c)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));

        let result = engine
            .execute("MATCH (p:Patent)-[:description_paragraphs]->(c) RETURN COUNT(c)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(1));
    }
}
