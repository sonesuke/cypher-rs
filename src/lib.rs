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
//! use cypher_rs::{CypherEngine, GraphConfig};
//! use serde_json::json;
//!
//! let data = json!({
//!     "users": [
//!         { "id": "1", "role": "admin", "age": 30, "friends": ["2"] },
//!         { "id": "2", "role": "user", "age": 25, "friends": ["1"] }
//!     ]
//! });
//!
//! let config = GraphConfig {
//!     node_path: "users".to_string(),
//!     id_field: "id".to_string(),
//!     label_field: Some("role".to_string()),
//!     relation_fields: vec!["friends".to_string()],
//! };
//!
//! let engine = CypherEngine::from_json(&data, config).unwrap();
//!
//! // Count all users
//! let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
//! assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));
//!
//! // Count admins
//! let result = engine.execute("MATCH (u:admin) RETURN COUNT(u)").unwrap();
//! assert_eq!(result.get_single_value().unwrap().as_i64(), Some(1));
//!
//! // Sum ages
//! let result = engine.execute("MATCH (u) RETURN SUM(u.age)").unwrap();
//! assert_eq!(result.get_single_value().unwrap().as_i64(), Some(55));
//! ```

pub mod config;
pub mod engine;
pub mod graph;
pub mod parser;
pub mod schema;

use serde_json::Value;
use std::fmt;

pub use config::GraphConfig;
pub use engine::storage::SyncStorage;
pub use engine::{EngineError, QueryResult, Result};
pub use engine::{JsonStorage, MemoryStorage, MemoryStorageBuilder};
pub use graph::{Edge, Graph, Node};
pub use schema::{SchemaAnalyzer, SchemaDetection, SchemaError};

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
/// use cypher_rs::{CypherEngine, GraphConfig};
/// use serde_json::json;
///
/// let data = json!({
///     "users": [
///         { "id": "1", "role": "admin", "age": 30 },
///         { "id": "2", "role": "user", "age": 25 }
///     ]
/// });
///
/// let config = GraphConfig::minimal("users", "id");
/// let engine = CypherEngine::from_json(&data, config).unwrap();
/// let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
/// ```
pub struct CypherEngine {
    graph: graph::Graph,
}

impl CypherEngine {
    /// Create a new CypherEngine from JSON data using the given configuration.
    ///
    /// # Arguments
    ///
    /// * `json` - The JSON data to query
    /// * `config` - Configuration for mapping JSON to graph structure
    ///
    /// # Example
    ///
    /// ```rust
    /// use cypher_rs::{CypherEngine, GraphConfig};
    /// use serde_json::json;
    ///
    /// let data = json!({
    ///     "users": [
    ///         { "id": "1", "role": "admin", "age": 30 },
    ///         { "id": "2", "role": "user", "age": 25 }
    ///     ]
    /// });
    ///
    /// let config = GraphConfig {
    ///     node_path: "users".to_string(),
    ///     id_field: "id".to_string(),
    ///     label_field: Some("role".to_string()),
    ///     relation_fields: vec![],
    /// };
    ///
    /// let engine = CypherEngine::from_json(&data, config).unwrap();
    /// ```
    pub fn from_json(json: &Value, config: GraphConfig) -> std::result::Result<Self, CypherError> {
        let storage = engine::JsonStorage::from_value(json.clone());
        let graph = storage
            .load_graph_sync(&config)
            .map_err(|e: engine::StorageError| CypherError::GraphBuild(e.to_string()))?;
        Ok(Self { graph })
    }

    /// Create a new CypherEngine from JSON data with automatic schema detection.
    ///
    /// This method automatically analyzes the JSON structure and infers the
    /// appropriate GraphConfig, eliminating the need to manually specify
    /// the node path, ID field, label field, and relation fields.
    ///
    /// # Arguments
    ///
    /// * `json` - The JSON data to query
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
        let config = schema::SchemaAnalyzer::infer_graph_config(json)
            .map_err(|e: schema::SchemaError| CypherError::GraphBuild(e.to_string()))?;
        Self::from_json(json, config)
    }

    /// Analyze JSON data and return schema detection information.
    ///
    /// This method provides detailed information about the detected schema,
    /// including field types and recommendations for graph configuration.
    ///
    /// # Arguments
    ///
    /// * `json` - The JSON data to analyze
    ///
    /// # Returns
    ///
    /// A `SchemaDetection` containing all detected arrays and recommendations.
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
    /// println!("Primary array: {}", schema.primary_recommendation.as_ref().unwrap().path);
    /// ```
    pub fn analyze_schema(json: &Value) -> std::result::Result<SchemaDetection, CypherError> {
        schema::SchemaAnalyzer::analyze(json)
            .map_err(|e: schema::SchemaError| CypherError::GraphBuild(e.to_string()))
    }

    /// Execute a Cypher query against the graph.
    ///
    /// # Arguments
    ///
    /// * `query` - The Cypher query string to execute
    ///
    /// # Returns
    ///
    /// A `QueryResult` containing the query results.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use cypher_rs::{CypherEngine, GraphConfig};
    /// # use serde_json::json;
    /// # let data = json!({"users": [{"id": "1", "role": "admin", "age": 30}]});
    /// # let config = GraphConfig::minimal("users", "id");
    /// # let engine = CypherEngine::from_json(&data, config).unwrap();
    /// // Execute a COUNT query
    /// let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
    ///
    /// // Execute a SUM query
    /// let result = engine.execute("MATCH (u) RETURN SUM(u.age)").unwrap();
    ///
    /// // Execute a simple SELECT query
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
    /// This analyzes the current graph structure and returns a formatted
    /// string representation similar to Neo4j's schema visualization.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use cypher_rs::{CypherEngine, GraphConfig};
    /// # use serde_json::json;
    /// # let data = json!({"users": [{"id": "1", "role": "admin", "friends": ["2"]}]});
    /// # let config = GraphConfig::minimal("users", "id");
    /// # let engine = CypherEngine::from_json(&data, config).unwrap();
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
            let label = node.label.as_ref().unwrap_or(&"Node".to_string()).clone();
            labels_by_label.entry(label).or_default().push(node);
        }

        // Output Node Types
        output.push_str("Node Types:\n");
        let mut label_names: Vec<String> = labels_by_label.keys().cloned().collect();
        label_names.sort();
        for label in &label_names {
            let count = labels_by_label.get(label).map(|v| v.len()).unwrap_or(0);
            output.push_str(&format!("  (:{} {} nodes)\n", label, count));
        }
        output.push('\n');

        // Output Properties per node type
        output.push_str("Properties:\n");
        for label in &label_names {
            if let Some(nodes) = labels_by_label.get(label) {
                if let Some(first_node) = nodes.first() {
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
        }
        output.push('\n');

        // Output Relationship Types
        if !self.graph.edges.is_empty() {
            output.push_str("Relationship Types:\n");

            // Group relationships by type
            let mut rel_types: std::collections::HashMap<
                String,
                (
                    std::collections::HashSet<String>,
                    std::collections::HashSet<String>,
                ),
            > = std::collections::HashMap::new();

            for edge in &self.graph.edges {
                let from_label = self.graph.nodes[edge.from]
                    .label
                    .as_ref()
                    .unwrap_or(&"Node".to_string())
                    .clone();
                let to_label = self.graph.nodes[edge.to]
                    .label
                    .as_ref()
                    .unwrap_or(&"Node".to_string())
                    .clone();

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

        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: Some("role".to_string()),
            relation_fields: vec![],
        };

        let engine = CypherEngine::from_json(&data, config).unwrap();

        // Count all users
        let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(3));

        // Count admins
        let result = engine.execute("MATCH (u:admin) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));

        // Count regular users
        let result = engine.execute("MATCH (u:user) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(1));
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

        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: Some("role".to_string()),
            relation_fields: vec![],
        };
        let engine = CypherEngine::from_json(&data, config).unwrap();

        // Sum all ages
        let result = engine.execute("MATCH (u) RETURN SUM(u.age)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(90));

        // Sum admin ages
        let result = engine.execute("MATCH (u:admin) RETURN SUM(u.age)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(65));
    }

    #[test]
    fn test_simple_return() {
        let data = json!({
            "users": [
                { "id": "1", "name": "Alice", "role": "admin" },
                { "id": "2", "name": "Bob", "role": "user" }
            ]
        });

        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: Some("role".to_string()),
            relation_fields: vec![],
        };

        let engine = CypherEngine::from_json(&data, config).unwrap();

        // Return all ids
        let result = engine.execute("MATCH (u) RETURN u.id").unwrap();
        assert_eq!(result.rows.len(), 2);
        // IDs are returned as numbers when they're numeric
        assert_eq!(result.rows[0]["u.id"], 1);
        assert_eq!(result.rows[1]["u.id"], 2);

        // Return with WHERE
        let result = engine
            .execute("MATCH (u) WHERE u.name = \"Alice\" RETURN u.id")
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

        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: None,
            relation_fields: vec!["friends".to_string()],
        };

        let engine = CypherEngine::from_json(&data, config).unwrap();

        // Count relationships
        let result = engine.execute("MATCH (u)-[]->(v) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(3));

        // Find Alice's friends
        let result = engine
            .execute("MATCH (u)-[]->(v) WHERE u.name = \"Alice\" RETURN v.name")
            .unwrap();
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

        let config = GraphConfig {
            node_path: "data.users".to_string(),
            id_field: "id".to_string(),
            label_field: Some("role".to_string()),
            relation_fields: vec![],
        };

        let engine = CypherEngine::from_json(&data, config).unwrap();
        let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
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

        let config = GraphConfig::minimal("users", "id");
        let engine = CypherEngine::from_json(&data, config).unwrap();

        let result = engine.execute("MATCH (u) RETURN u.id, u.name").unwrap();
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

        let config = GraphConfig::minimal("items", "id");
        let engine = CypherEngine::from_json(&data, config).unwrap();

        // Greater than
        let result = engine
            .execute("MATCH (i) WHERE i.value > \"15\" RETURN COUNT(i)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));

        // Less than or equal
        let result = engine
            .execute("MATCH (i) WHERE i.value <= \"20\" RETURN COUNT(i)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));

        // Not equal
        let result = engine
            .execute("MATCH (i) WHERE i.value <> \"20\" RETURN COUNT(i)")
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

        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: Some("role".to_string()),
            relation_fields: vec![],
        };

        let engine = CypherEngine::from_json(&data, config).unwrap();

        // AND - admin and active
        let result = engine
            .execute("MATCH (u:admin) WHERE u.active = \"true\" RETURN COUNT(u)")
            .unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(1));

        // OR - admin or user
        let result = engine
            .execute("MATCH (u) WHERE u.role = \"admin\" OR u.role = \"user\" RETURN COUNT(u)")
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

        let config = GraphConfig::minimal("users", "id");
        let engine = CypherEngine::from_json(&data, config).unwrap();

        let result = engine
            .execute("MATCH (u) WHERE u.name CONTAINS \"Smith\" RETURN COUNT(u)")
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

        // Auto-detect schema
        let engine = CypherEngine::from_json_auto(&data).unwrap();

        // Should correctly infer the schema
        let result = engine.execute("MATCH (u) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(2));

        // Label should be detected
        let result = engine.execute("MATCH (u:admin) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(1));
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

        assert_eq!(schema.array_schemas.len(), 1);
        assert!(schema.primary_recommendation.is_some());

        let primary = schema.primary_recommendation.as_ref().unwrap();
        assert_eq!(primary.path, "users");
        assert_eq!(primary.recommended_id_field, Some("id".to_string()));
        assert_eq!(primary.recommended_label_field, Some("role".to_string()));
        assert!(primary
            .recommended_relation_fields
            .contains(&"friends".to_string()));
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
        let primary = schema.primary_recommendation.as_ref().unwrap();
        assert_eq!(primary.path, "data.network.users");
        assert_eq!(primary.recommended_id_field, Some("id".to_string()));
        assert_eq!(primary.recommended_label_field, Some("type".to_string()));
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

        // Test relationship queries
        // 1->2, 1->3, 2->1, 3->2 = 4 edges total
        let result = engine.execute("MATCH (u)-[]->(v) RETURN COUNT(u)").unwrap();
        assert_eq!(result.get_single_value().unwrap().as_i64(), Some(4));
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

        let config = GraphConfig {
            node_path: "users".to_string(),
            id_field: "id".to_string(),
            label_field: Some("role".to_string()),
            relation_fields: vec!["friends".to_string()],
        };

        let engine = CypherEngine::from_json(&data, config).unwrap();
        let schema = engine.get_schema();

        // Verify schema contains expected elements
        assert!(schema.contains("Graph Schema"));
        assert!(schema.contains("Node Types:"));
        assert!(schema.contains("(:admin"));
        assert!(schema.contains("(:user"));
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

        // Verify Neo4j-style schema contains expected elements
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

        // Pattern should contain relationship
        assert!(pattern.contains("friends"));
        assert!(pattern.contains(":users"));
    }
}
